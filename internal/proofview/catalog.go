// Package proofview owns retained finalized read views and the immutable
// certification attachments (UC bytes plus optional partition view) that make
// them servable. A Catalog is created per local backend/shard. It holds no
// backend-specific logic: views are backend.ReadView handles, so the in-memory
// and RocksDB backends plug in the same way.
//
// The catalog never exposes staging or proposal state: Publish is the only way
// in, and callers invoke it only after the block is finalized and the backend
// state is committed. Requests select a view only by Latest or by exact root.
package proofview

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
)

// Errors map onto the JSON-RPC data.detail values of the batch API (plan §2.4).
var (
	// ErrNoFinalizedView: nothing has been published (no_finalized_view).
	ErrNoFinalizedView = errors.New("proofview: no finalized view")
	// ErrViewNotRetained: the requested root is not retained, or latest cannot
	// be served because the newest certification could not be retained
	// (view_not_retained). Evicted and never-published are not distinguished.
	ErrViewNotRetained = errors.New("proofview: view not retained")
	// ErrBusyPins: too many concurrent leases (busy / pinned_views).
	ErrBusyPins = errors.New("proofview: pin limit reached")
	// ErrBusyBudget: retention is over budget, new pins are refused
	// (busy / retention_budget).
	ErrBusyBudget = errors.New("proofview: retention budget exceeded")
	// ErrBudget: the new view alone does not fit the retention budget.
	ErrBudget = errors.New("proofview: view does not fit retention budget")
	// ErrInvalidPublication: inputs are inconsistent (root, block, UC).
	ErrInvalidPublication = errors.New("proofview: invalid publication")
	// ErrClosed: the catalog is closed.
	ErrClosed = errors.New("proofview: catalog closed")
)

// Certification is the certification material accepted for a finalized state:
// the complete UC bytes, and optionally the partition-view bytes that arrived
// with exactly that UC.
type Certification struct {
	UC            []byte
	PartitionView []byte
}

// PublishInput is one finalized, committed local state. Publish takes ownership
// of Reader: it is closed on any failure and when the view is retired.
type PublishInput struct {
	Block  BlockIdentity
	Root   []byte
	Reader backend.ReadView
	Cert   Certification
}

// attIndex is the digest index of the attachments of one view. Every lookup is counted in the catalog
// (probes): a lookup path which examined many views would show it, wherever its loop lives.
type attIndex struct {
	m      map[[sha256.Size]byte]*attachment
	probes *uint64
}

func (x attIndex) get(d [sha256.Size]byte) *attachment {
	*x.probes++
	return x.m[d]
}
func (x attIndex) put(a *attachment)       { x.m[a.digest] = a }
func (x attIndex) del(d [sha256.Size]byte) { delete(x.m, d) }
func (x attIndex) size() int               { return len(x.m) }
func (x attIndex) each(f func(d [sha256.Size]byte)) {
	for d := range x.m {
		f(d)
	}
}

// attachment is one certification (UC plus optional partition view) of a view. The
// attachments of a view form a list, oldest first, whose tail is the latest
// certification; it is indexed by digest.
type attachment struct {
	uc         []byte
	digest     [sha256.Size]byte
	view       []byte
	round      uint64 // UC root-chain round: the order in which certifications supersede each other
	bytes      uint64
	refs       int
	prev, next *attachment
}

// view is one retained finalized state. All catalog bookkeeping about it is O(1):
// it is a member of a handful of intrusive lists/indexes (publication order, same-root
// chain, identity map, eviction/pin heaps) instead of being found by scanning.
type view struct {
	block   BlockIdentity
	root    []byte
	reader  backend.ReadView
	refs    int
	retired bool
	seq     uint64 // publication order

	attHead, attTail *attachment // oldest ... latest
	attN             int
	attIdx           attIndex
	attBytes         uint64 // sum of the bytes of the attachments in the list

	leases map[*Lease]struct{}
	// pending are the leases pressure has not cancelled yet (a subset of leases)
	pending map[*Lease]struct{}

	// live views in publication order
	prev, next *view
	// live views with equal root, older/newer
	rootOlder, rootNewer *view
	idKey                string
	live                 bool
	// positions in the evictable / pinned heaps, -1 when not a member
	evictIdx, pinIdx int
}

// Catalog retains finalized views. All methods are safe for concurrent use.
//
// Cost model (V live views, U attachments per view, P pins): publishing, attaching,
// acquiring, releasing and the retained-byte total are O(1) or O(log V); evicting k
// views is O(k log V). The retained total is maintained, not recomputed: attachment
// bytes by running sums, backend bytes by the backend's O(1) lineage total
// (WithAccountant; without it the sum over the open readers is the fallback).
type Catalog struct {
	limits Limits
	acc    backend.RetentionAccountant

	mu sync.Mutex
	// live views, oldest first; tail is the newest published
	head, tail *view
	nViews     int
	seq        uint64
	byID       map[string]*view // block identity and root -> the live view
	byRoot     map[string]*view // root -> newest live view with it
	// byDigest maps the digest of a certification to the live views holding it (normally one),
	// so a late partition view finds its view without scanning the retained ones.
	byDigest     map[[sha256.Size]byte]map[*view]struct{}
	digestVisits uint64   // candidate views examined by digest lookups (hardware independent metric)
	attProbes    uint64   // every lookup of a digest in a view\'s attachment index, wherever it is done
	evictable    viewHeap // live, unpinned views except the newest, oldest first
	pinned       viewHeap // live, pinned views, oldest first
	// retiredLive are the retired views with leases pressure has not cancelled yet: a pressure
	// pass visits only pending leases, so a lease is looked at once, not on every later pass.
	retiredLive map[*view]struct{}
	leaseVisits uint64 // leases examined by pressure passes (hardware independent metric)

	retired     map[*view]struct{}
	retiredAtts map[*attachment]struct{}
	leases      map[*Lease]struct{}

	// maintained attachment totals (bytes)
	liveAttBytes        uint64 // attachments of live views
	retiredViewAttBytes uint64 // attachments of retired views still pinned
	retiredAttBytes     uint64 // attachments removed from a view but still leased

	// latestStale is set when the newest certification could not be retained.
	// Latest then reports ErrViewNotRetained until a later publication
	// succeeds; it never falls back to an older view.
	latestStale bool
	// curView/curRound identify the current published certification
	// independently of what is retained: curView is the retained view it
	// belongs to (nil if that publication was rejected) and curRound its UC
	// root round. Only a genuinely newer certification on curView, or a newer
	// published view, clears latestStale; filling in historical material never
	// does.
	curView  *view
	curRound uint64
	closed   bool

	evictions     uint64
	pressureKills uint64
}

// Option configures a Catalog.
type Option func(*Catalog)

// WithAccountant supplies the O(1) total of the backend's open readers' retained
// bytes (see backend.RetentionAccountant). The catalog must own every open reader
// of that backend. Without it the total is the sum over the catalog's readers.
func WithAccountant(a backend.RetentionAccountant) Option { return func(c *Catalog) { c.acc = a } }

// NewCatalog creates an empty catalog.
func NewCatalog(limits Limits, opts ...Option) (*Catalog, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	c := &Catalog{
		limits:      limits,
		byID:        make(map[string]*view),
		byRoot:      make(map[string]*view),
		byDigest:    make(map[[sha256.Size]byte]map[*view]struct{}),
		retired:     make(map[*view]struct{}),
		retiredAtts: make(map[*attachment]struct{}),
		leases:      make(map[*Lease]struct{}),
	}
	c.evictable.idx = func(v *view) *int { return &v.evictIdx }
	c.pinned.idx = func(v *view) *int { return &v.pinIdx }
	c.retiredLive = make(map[*view]struct{})
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// UsesAccountant reports whether the retained-byte total comes from the backend's O(1) accountant
// (otherwise it is summed over the held readers, linear in the number of views).
func (c *Catalog) UsesAccountant() bool { return c.acc != nil }

// Limits returns the catalog's limits.
func (c *Catalog) Limits() Limits { return c.limits }

// Stats is a point-in-time snapshot for metrics.
type Stats struct {
	Views         int
	RetiredPinned int
	ActivePins    int
	RetainedBytes uint64
	Evictions     uint64
	PressureKills uint64
	LatestStale   bool
}

func (c *Catalog) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Views:         c.nViews,
		RetiredPinned: len(c.retired),
		ActivePins:    len(c.leases),
		RetainedBytes: c.retainedBytesLocked(),
		Evictions:     c.evictions,
		PressureKills: c.pressureKills,
		LatestStale:   c.latestStale,
	}
}

// decodeUC returns the UC's input-record state hash and root-chain round.
func decodeUC(uc []byte) ([]byte, uint64, error) {
	var cert types.UnicityCertificate
	if err := types.Cbor.Unmarshal(uc, &cert); err != nil {
		return nil, 0, fmt.Errorf("decode UC: %w", err)
	}
	root := cert.GetStateHash()
	if len(root) == 0 {
		return nil, 0, errors.New("UC has no input record state hash")
	}
	return root, cert.GetRootRoundNumber(), nil
}

func newAttachment(cert Certification, root []byte) (*attachment, error) {
	if len(cert.UC) == 0 {
		return nil, fmt.Errorf("%w: missing UC", ErrInvalidPublication)
	}
	got, round, err := decodeUC(cert.UC)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPublication, err)
	}
	if !bytes.Equal(got, root) {
		return nil, fmt.Errorf("%w: UC root %x does not match view root %x", ErrInvalidPublication, got, root)
	}
	a := &attachment{
		uc:     append([]byte(nil), cert.UC...),
		digest: sha256.Sum256(cert.UC),
		round:  round,
	}
	if len(cert.PartitionView) > 0 {
		a.view = append([]byte(nil), cert.PartitionView...)
	}
	a.bytes = uint64(len(a.uc)+len(a.view)) + attachmentOverhead
	return a, nil
}

const attachmentOverhead = 128

// Publish retains a finalized, committed state together with its certification.
//
// The reader's root, the declared block root and the UC's input-record hash
// must all agree. Publishing a block identity and root already retained adds
// the certification as an attachment (see AttachCertification) and closes the
// redundant reader. If the new view cannot fit the retention budget even after
// evicting all older unpinned history, it is not published, latest becomes
// unavailable (never an older view), and ErrBudget is returned; ordinary
// finalization is unaffected and the caller should only log/count it.
func (c *Catalog) Publish(ctx context.Context, in PublishInput) error {
	if in.Reader == nil {
		return fmt.Errorf("%w: nil reader", ErrInvalidPublication)
	}
	var toClose []backend.ReadView
	owned := false // reader ownership moved into the catalog
	defer func() {
		if !owned {
			toClose = append(toClose, in.Reader)
		}
		c.settle(toClose)
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if in.Block.Number == nil {
		return fmt.Errorf("%w: missing block number", ErrInvalidPublication)
	}
	if len(in.Root) == 0 || !bytes.Equal(in.Root, in.Reader.RootHash()) {
		return fmt.Errorf("%w: reader root does not match block root", ErrInvalidPublication)
	}
	att, err := newAttachment(in.Cert, in.Root)
	if err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if existing := c.findLocked(in.Block, in.Root); existing != nil {
		// Same finalized state again (retry, repeat, duplicate delivery):
		// keep the retained tree, add the certification; the redundant reader
		// is closed by the deferred cleanup. It leaves the accounting now so
		// that it does not weigh on the budget while it waits to be closed.
		retireAccounting(in.Reader)
		var aerr error
		toClose, aerr = c.attachLocked(existing, att, toClose)
		return aerr
	}
	v := &view{
		block:    in.Block.clone(),
		root:     append([]byte(nil), in.Root...),
		reader:   in.Reader,
		attIdx:   attIndex{m: make(map[[sha256.Size]byte]*attachment, 1), probes: &c.attProbes},
		evictIdx: -1,
		pinIdx:   -1,
	}
	v.idKey = identityKey(v.block, v.root)
	if viewCost(v, att.bytes) > c.limits.RetentionBudgetBytes {
		// Cannot retain this view even alone: refuse without disturbing older
		// history, and make latest unavailable rather than serving an older view.
		retireAccounting(in.Reader)
		c.latestStale = true
		c.curView, c.curRound = nil, att.round
		return ErrBudget
	}
	c.pushAttLocked(v, att)
	c.linkViewLocked(v)
	toClose = c.enforceLocked(toClose)
	c.latestStale = false
	c.curView, c.curRound = v, att.round
	owned = true
	return nil
}

// viewCost is the cost of a view holding "extraAtt" more attachment bytes.
func viewCost(v *view, extraAtt uint64) uint64 {
	return v.reader.RetainedBytes() + v.attBytes + extraAtt
}

func retireAccounting(r backend.ReadView) {
	if ar, ok := r.(backend.AccountingRetirer); ok {
		ar.RetireAccounting()
	}
}

// AttachCertification adds certification material to an already retained
// finalized state, identified by block identity and root. It covers repeat/
// timeout UCs on unchanged state and late arrival of a valid partition view for
// a UC already attached. It neither commits nor advances state. A matching UC
// without a partition view is replaced (immutably) by the same UC with one; an
// attachment that already has a view is never overwritten.
func (c *Catalog) AttachCertification(ctx context.Context, block BlockIdentity, root []byte, cert Certification) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	att, err := newAttachment(cert, root)
	if err != nil {
		return err
	}
	var toClose []backend.ReadView
	defer func() { c.settle(toClose) }()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	v := c.findLocked(block, root)
	if v == nil {
		return ErrViewNotRetained
	}
	toClose, err = c.attachLocked(v, att, toClose)
	return err
}

// attachLocked installs att on v. Caller holds c.mu.
//
// A UC already attached only gains a missing partition view. Any other UC is
// accepted only if it is newer (higher root round) than the view's latest;
// an older or equal UC that is no longer (or never was) attached is historical
// and is dropped, so redelivery can never promote it over a newer certification.
func (c *Catalog) attachLocked(v *view, att *attachment, toClose []backend.ReadView) ([]backend.ReadView, error) {
	if old := v.attIdx.get(att.digest); old != nil {
		if old.view != nil || att.view == nil {
			return toClose, nil // nothing new
		}
		c.replaceAttLocked(v, old, att) // fill the missing partition view; position (and latest-ness) unchanged
		return c.enforceAttachLocked(v, att, toClose)
	}
	if v.attTail != nil && att.round <= v.attTail.round {
		return toClose, nil
	}
	c.pushAttLocked(v, att)
	for v.attN > c.limits.MaxAttachmentsPerView {
		c.dropAttLocked(v, v.attHead)
	}
	return c.enforceAttachLocked(v, att, toClose)
}

func (c *Catalog) enforceAttachLocked(v *view, att *attachment, toClose []backend.ReadView) ([]backend.ReadView, error) {
	toClose = c.enforceLocked(toClose)
	isCur := v == c.curView
	if viewCost(v, 0) > c.limits.RetentionBudgetBytes {
		// The certification does not fit; drop it. If it is (or supersedes)
		// the current one, latest would otherwise be older material.
		if v.attIdx.get(att.digest) == att {
			c.dropAttLocked(v, att)
		}
		if isCur && att.round >= c.curRound {
			c.curRound = att.round
			c.latestStale = true
		}
		return toClose, ErrBudget
	}
	if isCur && att.round > c.curRound {
		c.curRound = att.round
		c.latestStale = false
	}
	return toClose, nil
}

// pushAttLocked appends att as the latest attachment of v.
func (c *Catalog) pushAttLocked(v *view, att *attachment) {
	att.prev, att.next = v.attTail, nil
	if v.attTail != nil {
		v.attTail.next = att
	} else {
		v.attHead = att
	}
	v.attTail = att
	v.attN++
	v.attIdx.put(att)
	v.attBytes += att.bytes
	if v.live {
		c.liveAttBytes += att.bytes
		c.indexDigestLocked(v, att.digest)
	}
}

// replaceAttLocked puts att in the position of old (same digest).
func (c *Catalog) replaceAttLocked(v *view, old, att *attachment) {
	att.prev, att.next = old.prev, old.next
	if old.prev != nil {
		old.prev.next = att
	} else {
		v.attHead = att
	}
	if old.next != nil {
		old.next.prev = att
	} else {
		v.attTail = att
	}
	v.attIdx.put(att)
	v.attBytes = v.attBytes - old.bytes + att.bytes
	if v.live {
		c.liveAttBytes = c.liveAttBytes - old.bytes + att.bytes
	}
	c.retireAttLocked(old)
}

// dropAttLocked removes a from v.
func (c *Catalog) dropAttLocked(v *view, a *attachment) {
	if a.prev != nil {
		a.prev.next = a.next
	} else {
		v.attHead = a.next
	}
	if a.next != nil {
		a.next.prev = a.prev
	} else {
		v.attTail = a.prev
	}
	a.prev, a.next = nil, nil
	v.attN--
	v.attIdx.del(a.digest)
	v.attBytes -= a.bytes
	if v.live {
		c.liveAttBytes -= a.bytes
		c.unindexDigestLocked(v, a.digest)
	}
	c.retireAttLocked(a)
}

// retireAttLocked tracks a removed attachment while leases still hold it, so
// it keeps counting against the budget.
func (c *Catalog) retireAttLocked(a *attachment) {
	if a.refs > 0 {
		if _, ok := c.retiredAtts[a]; !ok {
			c.retiredAtts[a] = struct{}{}
			c.retiredAttBytes += a.bytes
		}
	}
}

func (c *Catalog) indexDigestLocked(v *view, d [sha256.Size]byte) {
	m := c.byDigest[d]
	if m == nil {
		m = make(map[*view]struct{}, 1)
		c.byDigest[d] = m
	}
	m[v] = struct{}{}
}

func (c *Catalog) unindexDigestLocked(v *view, d [sha256.Size]byte) {
	if m := c.byDigest[d]; m != nil {
		delete(m, v)
		if len(m) == 0 {
			delete(c.byDigest, d)
		}
	}
}

// newestWithDigestLocked is the newest live view holding the certification.
func (c *Catalog) newestWithDigestLocked(d [sha256.Size]byte) (*view, *attachment) {
	var best *view
	var bestAtt *attachment
	for v := range c.byDigest[d] {
		c.digestVisits++
		a := v.attIdx.get(d) // the index is exact: a holder always has it
		if a == nil {
			continue
		}
		if best == nil || v.seq > best.seq {
			best, bestAtt = v, a
		}
	}
	return best, bestAtt
}

func identityKey(b BlockIdentity, root []byte) string {
	n := "-"
	if b.Number != nil {
		n = b.Number.String()
	}
	return strconv.Itoa(len(root)) + ":" + string(root) + strconv.Itoa(len(n)) + ":" + n + string(b.ConfigLineage)
}

// findLocked returns the live view of the exact block identity and root.
func (c *Catalog) findLocked(block BlockIdentity, root []byte) *view {
	return c.byID[identityKey(block, root)]
}

// linkViewLocked makes v the newest live view.
func (c *Catalog) linkViewLocked(v *view) {
	c.seq++
	v.seq = c.seq
	v.live = true
	c.liveAttBytes += v.attBytes
	v.attIdx.each(func(d [sha256.Size]byte) { c.indexDigestLocked(v, d) })
	prevTail := c.tail
	v.prev = c.tail
	if c.tail != nil {
		c.tail.next = v
	} else {
		c.head = v
	}
	c.tail = v
	c.nViews++
	c.byID[v.idKey] = v
	if old := c.byRoot[string(v.root)]; old != nil {
		old.rootNewer = v
		v.rootOlder = old
	}
	c.byRoot[string(v.root)] = v
	// the previous newest becomes evictable history unless pinned
	if prevTail != nil {
		c.reclassLocked(prevTail)
	}
	c.reclassLocked(v)
}

// unlinkViewLocked removes v from the live structures.
func (c *Catalog) unlinkViewLocked(v *view) {
	if v.prev != nil {
		v.prev.next = v.next
	} else {
		c.head = v.next
	}
	if v.next != nil {
		v.next.prev = v.prev
	} else {
		c.tail = v.prev
	}
	v.prev, v.next = nil, nil
	c.nViews--
	delete(c.byID, v.idKey)
	if v.rootNewer == nil {
		if v.rootOlder != nil {
			c.byRoot[string(v.root)] = v.rootOlder
		} else {
			delete(c.byRoot, string(v.root))
		}
	} else {
		v.rootNewer.rootOlder = v.rootOlder
	}
	if v.rootOlder != nil {
		v.rootOlder.rootNewer = v.rootNewer
	}
	v.rootOlder, v.rootNewer = nil, nil
	v.live = false
	c.liveAttBytes -= v.attBytes
	v.attIdx.each(func(d [sha256.Size]byte) { c.unindexDigestLocked(v, d) })
	c.evictable.remove(v)
	c.pinned.remove(v)
	if c.tail != nil {
		c.reclassLocked(c.tail) // a new newest view is never evicted as history
	}
}

// reclassLocked restores the heap membership of a live view: pinned views are in the
// pinned heap; unpinned views other than the newest are evictable.
func (c *Catalog) reclassLocked(v *view) {
	if !v.live {
		return
	}
	if v.refs > 0 {
		c.evictable.remove(v)
		c.pinned.push(v)
		return
	}
	c.pinned.remove(v)
	if v == c.tail {
		c.evictable.remove(v)
	} else {
		c.evictable.push(v)
	}
}

// readersBytesLocked is the retained bytes of the backend readers the catalog holds.
func (c *Catalog) readersBytesLocked() uint64 {
	if c.acc != nil {
		return c.acc.RetainedBytesTotal()
	}
	// fallback without a backend total: sum over the held readers (linear in V)
	var total uint64
	for v := c.head; v != nil; v = v.next {
		total += v.reader.RetainedBytes()
	}
	for v := range c.retired {
		total += v.reader.RetainedBytes()
	}
	return total
}

// retainedBytesLocked is the budgeted total: readers, attachments of live views,
// of retired pinned views, and attachments still leased after their removal.
func (c *Catalog) retainedBytesLocked() uint64 {
	return c.readersBytesLocked() + c.liveAttBytes + c.retiredViewAttBytes + c.retiredAttBytes
}

// enforceLocked evicts to satisfy the view count and byte budget. Oldest
// history goes first; an evicted view that is still pinned is retired (kept,
// counted, closed on its last release). The newest view is never evicted here.
// If still over budget, the oldest active leases are cancelled so retired
// views can drain; their readers are not closed until released.
func (c *Catalog) enforceLocked(toClose []backend.ReadView) []backend.ReadView {
	for c.nViews > 1 {
		overCount := c.nViews > c.limits.MaxViews
		if !overCount && c.retainedBytesLocked() <= c.limits.RetentionBudgetBytes {
			break
		}
		// Unpinned history first: evicting it actually frees memory. A pinned
		// view is retired (hidden from lookup, still counted) only to satisfy
		// the view count, since retiring it frees no bytes.
		v := c.evictable.peek()
		if v == nil {
			if !overCount {
				break
			}
			v = c.head
		}
		c.unlinkViewLocked(v)
		c.evictions++
		toClose = c.retireViewLocked(v, toClose)
	}
	if c.retainedBytesLocked() > c.limits.RetentionBudgetBytes {
		c.cancelOldestLeasesLocked()
	}
	return toClose
}

func (c *Catalog) retireViewLocked(v *view, toClose []backend.ReadView) []backend.ReadView {
	v.retired = true
	if v.refs > 0 {
		c.retired[v] = struct{}{}
		c.retiredViewAttBytes += v.attBytes
		if len(v.pending) > 0 {
			c.retiredLive[v] = struct{}{}
		}
		return toClose
	}
	// leaves the accounting now; closed outside the lock
	retireAccounting(v.reader)
	return append(toClose, v.reader)
}

func (c *Catalog) cancelOldestLeasesLocked() {
	// Leases are cancelled oldest-view-first; a lease on a retired view goes
	// before one on a retained view. Only leases pressure has not dealt with yet are visited
	// (each once): a view whose leases are all cancelled is skipped until a new lease pins it.
	for v := range c.retiredLive {
		c.cancelViewLeasesLocked(v)
		delete(c.retiredLive, v)
	}
	if c.retainedBytesLocked() <= c.limits.RetentionBudgetBytes {
		return
	}
	// the oldest live pinned view: its leases are cancelled; if it has none left to cancel nothing
	// else is touched (the pass is about draining the oldest pinned history)
	if v := c.pinned.peek(); v != nil {
		c.cancelViewLeasesLocked(v)
	}
}

func (c *Catalog) cancelViewLeasesLocked(v *view) {
	for l := range v.pending {
		c.leaseVisits++
		delete(v.pending, l)
		if l.ctx.Err() == nil {
			l.cancel()
			c.pressureKills++
		}
	}
}

// AcquireLatest pins the newest published view with its latest certification.
// The latest certification is served even without a partition view; if the
// newest certification could not be retained it fails with ErrViewNotRetained
// rather than returning older material.
func (c *Catalog) AcquireLatest(ctx context.Context) (*Lease, error) {
	return c.acquire(ctx, nil)
}

// AcquireRoot pins the newest retained view whose root equals root, with that
// view's latest certification. Callers cannot select a particular seal.
func (c *Catalog) AcquireRoot(ctx context.Context, root []byte) (*Lease, error) {
	if len(root) == 0 {
		return nil, ErrViewNotRetained
	}
	return c.acquire(ctx, root)
}

func (c *Catalog) acquire(ctx context.Context, root []byte) (*Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	var v *view
	if root == nil {
		if c.nViews == 0 && !c.latestStale {
			return nil, ErrNoFinalizedView
		}
		if c.latestStale || c.nViews == 0 {
			return nil, ErrViewNotRetained
		}
		v = c.tail
	} else {
		v = c.byRoot[string(root)]
		if v == nil {
			return nil, ErrViewNotRetained
		}
	}
	if v.attN == 0 {
		return nil, ErrViewNotRetained
	}
	if len(c.leases) >= c.limits.MaxConcurrentPins {
		return nil, ErrBusyPins
	}
	if c.retainedBytesLocked() > c.limits.RetentionBudgetBytes {
		return nil, ErrBusyBudget
	}
	att := v.attTail
	lctx, cancel := context.WithTimeout(ctx, c.limits.MaxLeaseDuration)
	l := &Lease{c: c, v: v, att: att, ctx: lctx, cancel: cancel}
	v.refs++
	att.refs++
	if v.leases == nil {
		v.leases = make(map[*Lease]struct{}, 1)
		v.pending = make(map[*Lease]struct{}, 1)
	}
	v.leases[l] = struct{}{}
	v.pending[l] = struct{}{}

	c.leases[l] = struct{}{}
	c.reclassLocked(v)
	return l, nil
}

func (c *Catalog) release(l *Lease) {
	var toClose []backend.ReadView
	c.mu.Lock()
	delete(c.leases, l)
	delete(l.v.leases, l)
	delete(l.v.pending, l)
	if len(l.v.pending) == 0 {
		delete(c.retiredLive, l.v)
	}
	l.att.refs--
	if l.att.refs == 0 {
		if _, ok := c.retiredAtts[l.att]; ok {
			delete(c.retiredAtts, l.att)
			c.retiredAttBytes -= l.att.bytes
		}
	}
	l.v.refs--
	if l.v.refs == 0 && l.v.retired {
		delete(c.retired, l.v)
		c.retiredViewAttBytes -= l.v.attBytes
		retireAccounting(l.v.reader)
		toClose = append(toClose, l.v.reader)
	}
	c.reclassLocked(l.v)
	// A release can make an over-budget view evictable.
	if !c.closed {
		toClose = c.enforceLocked(toClose)
	}
	c.mu.Unlock()
	c.settle(toClose)
}

// settle closes readers that left the catalog, outside the lock, then re-runs
// retention enforcement. Closing a reader can move its accounted charge onto an
// older surviving (possibly pinned) reader; readers leave the accounting when they
// leave the catalog (retireAccounting) so that the transfer is already visible, and
// enforcement is re-run once more after the closes for readers without that ability.
// Enforcement never closes a reader a lease still holds.
func (c *Catalog) settle(toClose []backend.ReadView) {
	for len(toClose) > 0 {
		closeAll(toClose)
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		toClose = c.enforceLocked(nil)
		c.mu.Unlock()
	}
}

// Close cancels all leases, retires every view, and closes the readers that no
// lease holds; pinned ones close on their last release. Further calls fail.
func (c *Catalog) Close() error {
	var toClose []backend.ReadView
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	for l := range c.leases {
		l.cancel()
	}
	for v := c.head; v != nil; {
		next := v.next
		c.unlinkViewLocked(v)
		toClose = c.retireViewLocked(v, toClose)
		v = next
	}
	c.mu.Unlock()
	closeAll(toClose)
	return nil
}

func closeAll(rs []backend.ReadView) {
	for _, r := range rs {
		if r != nil {
			_ = r.Close()
		}
	}
}

// viewHeap is a min-heap of views by publication order with O(log n) removal of any member.
type viewHeap struct {
	items []*view
	idx   func(*view) *int
}

func (h *viewHeap) less(i, j int) bool { return h.items[i].seq < h.items[j].seq }

func (h *viewHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	*h.idx(h.items[i]) = i
	*h.idx(h.items[j]) = j
}

func (h *viewHeap) up(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !h.less(i, p) {
			return
		}
		h.swap(i, p)
		i = p
	}
}

func (h *viewHeap) down(i int) {
	n := len(h.items)
	for {
		l, r, m := 2*i+1, 2*i+2, i
		if l < n && h.less(l, m) {
			m = l
		}
		if r < n && h.less(r, m) {
			m = r
		}
		if m == i {
			return
		}
		h.swap(i, m)
		i = m
	}
}

func (h *viewHeap) push(v *view) {
	if *h.idx(v) >= 0 {
		return
	}
	h.items = append(h.items, v)
	*h.idx(v) = len(h.items) - 1
	h.up(len(h.items) - 1)
}

func (h *viewHeap) remove(v *view) {
	i := *h.idx(v)
	if i < 0 {
		return
	}
	last := len(h.items) - 1
	if i != last {
		h.swap(i, last)
	}
	h.items[last] = nil
	h.items = h.items[:last]
	*h.idx(v) = -1
	if i != last {
		h.down(i)
		h.up(i)
	}
}

func (h *viewHeap) peek() *view {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

// AccountantFor returns WithAccountant(b) when the backend can total the retained bytes of its open
// readers in O(1) (both the memory and the RocksDB backend do), and no option otherwise, so that
// the catalog constructor can be given the backend whatever its type:
//
//	proofview.NewCatalog(limits, proofview.AccountantFor(backend))
//
// The catalog must own every reader the backend retains for the O(1) total to be the sum of the
// catalog's readers (one catalog per backend).
func AccountantFor(b any) Option {
	if acc, ok := b.(backend.RetentionAccountant); ok {
		return WithAccountant(acc)
	}
	return func(*Catalog) {}
}

// RetainedBytesBySummation recomputes the budgeted total the slow way: the sum of the held readers'
// charges and of every attachment (live, of retired pinned views, and still leased after removal). It is the
// O(V) reference the maintained total must equal, for tests and diagnostics; it takes the catalog lock.
func (c *Catalog) RetainedBytesBySummation() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var total uint64
	for v := c.head; v != nil; v = v.next {
		total += v.reader.RetainedBytes()
		for a := v.attHead; a != nil; a = a.next {
			total += a.bytes
		}
	}
	for v := range c.retired {
		total += v.reader.RetainedBytes()
		for a := v.attHead; a != nil; a = a.next {
			total += a.bytes
		}
	}
	for a := range c.retiredAtts {
		total += a.bytes
	}
	return total
}
