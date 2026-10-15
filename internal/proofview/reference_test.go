package proofview

// Reference model: the straightforward implementation of the retention catalog
// (linear scans over its views) kept only to define the semantics which the indexed
// catalog must reproduce exactly. It is the catalog as merged before the indexes.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"sync"

	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
)

type refAttachment struct {
	uc     []byte
	digest [sha256.Size]byte
	view   []byte
	round  uint64 // UC root-chain round: the order in which certifications supersede each other
	bytes  uint64
	refs   int
}

type refView struct {
	block   BlockIdentity
	root    []byte
	reader  backend.ReadView
	atts    []*refAttachment // oldest first; last is the latest certification
	refs    int
	retired bool
}

// Catalog retains finalized views. All methods are safe for concurrent use.
type refCatalog struct {
	limits Limits

	mu          sync.Mutex
	views       []*refView // oldest first; last is the newest published
	retired     map[*refView]struct{}
	retiredAtts map[*refAttachment]struct{}
	leases      map[*refLease]struct{}
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
	curView  *refView
	curRound uint64
	closed   bool

	evictions     uint64
	pressureKills uint64
}

// NewCatalog creates an empty catalog.
func newRefCatalog(limits Limits) (*refCatalog, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	return &refCatalog{
		limits:      limits,
		retired:     make(map[*refView]struct{}),
		retiredAtts: make(map[*refAttachment]struct{}),
		leases:      make(map[*refLease]struct{}),
	}, nil
}

// Limits returns the catalog's limits.
func (c *refCatalog) Limits() Limits { return c.limits }

func (c *refCatalog) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Views:         len(c.views),
		RetiredPinned: len(c.retired),
		ActivePins:    len(c.leases),
		RetainedBytes: c.retainedBytesLocked(),
		Evictions:     c.evictions,
		PressureKills: c.pressureKills,
		LatestStale:   c.latestStale,
	}
}

func newRefAttachment(cert Certification, root []byte) (*refAttachment, error) {
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
	a := &refAttachment{
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

// Publish retains a finalized, committed state together with its certification.
//
// The reader's root, the declared block root and the UC's input-record hash
// must all agree. Publishing a block identity and root already retained adds
// the certification as an refAttachment (see AttachCertification) and closes the
// redundant reader. If the new view cannot fit the retention budget even after
// evicting all older unpinned history, it is not published, latest becomes
// unavailable (never an older view), and ErrBudget is returned; ordinary
// finalization is unaffected and the caller should only log/count it.
func (c *refCatalog) Publish(ctx context.Context, in PublishInput) error {
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
	att, err := newRefAttachment(in.Cert, in.Root)
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
		// is closed by the deferred cleanup.
		var aerr error
		toClose, aerr = c.attachLocked(existing, att, toClose)
		return aerr
	}
	v := &refView{
		block:  in.Block.clone(),
		root:   append([]byte(nil), in.Root...),
		reader: in.Reader,
		atts:   []*refAttachment{att},
	}
	if c.viewCostLocked(v) > c.limits.RetentionBudgetBytes {
		// Cannot retain this view even alone: refuse without disturbing older
		// history, and make latest unavailable rather than serving an older view.
		c.latestStale = true
		c.curView, c.curRound = nil, att.round
		return ErrBudget
	}
	c.views = append(c.views, v)
	toClose = c.enforceLocked(toClose)
	c.latestStale = false
	c.curView, c.curRound = v, att.round
	owned = true
	return nil
}

// AttachCertification adds certification material to an already retained
// finalized state, identified by block identity and root. It covers repeat/
// timeout UCs on unchanged state and late arrival of a valid partition view for
// a UC already attached. It neither commits nor advances state. A matching UC
// without a partition view is replaced (immutably) by the same UC with one; an
// refAttachment that already has a view is never overwritten.
func (c *refCatalog) AttachCertification(ctx context.Context, block BlockIdentity, root []byte, cert Certification) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	att, err := newRefAttachment(cert, root)
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
func (c *refCatalog) attachLocked(v *refView, att *refAttachment, toClose []backend.ReadView) ([]backend.ReadView, error) {
	for i, old := range v.atts {
		if old.digest != att.digest {
			continue
		}
		if old.view != nil || att.view == nil {
			return toClose, nil // nothing new
		}
		v.atts[i] = att // fill the missing partition view; position (and latest-ness) unchanged
		c.retireAttLocked(old)
		return c.enforceAttachLocked(v, att, toClose)
	}
	if n := len(v.atts); n > 0 && att.round <= v.atts[n-1].round {
		return toClose, nil
	}
	v.atts = append(v.atts, att)
	for len(v.atts) > c.limits.MaxAttachmentsPerView {
		c.retireAttLocked(v.atts[0])
		v.atts = v.atts[1:]
	}
	return c.enforceAttachLocked(v, att, toClose)
}

func (c *refCatalog) enforceAttachLocked(v *refView, att *refAttachment, toClose []backend.ReadView) ([]backend.ReadView, error) {
	toClose = c.enforceLocked(toClose)
	isCur := v == c.curView
	if c.viewCostLocked(v) > c.limits.RetentionBudgetBytes {
		// The certification does not fit; drop it. If it is (or supersedes)
		// the current one, latest would otherwise be older material.
		for i, a := range v.atts {
			if a == att {
				v.atts = append(v.atts[:i:i], v.atts[i+1:]...)
				c.retireAttLocked(a)
				break
			}
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

// retireAttLocked tracks a removed refAttachment while leases still hold it, so
// it keeps counting against the budget.
func (c *refCatalog) retireAttLocked(a *refAttachment) {
	if a.refs > 0 {
		c.retiredAtts[a] = struct{}{}
	}
}

func (c *refCatalog) findLocked(block BlockIdentity, root []byte) *refView {
	for i := len(c.views) - 1; i >= 0; i-- {
		v := c.views[i]
		if bytes.Equal(v.root, root) && v.block.equal(block) {
			return v
		}
	}
	return nil
}

func (c *refCatalog) viewCostLocked(v *refView) uint64 {
	cost := v.reader.RetainedBytes()
	for _, a := range v.atts {
		cost += a.bytes
	}
	return cost
}

func (c *refCatalog) retainedBytesLocked() uint64 {
	var total uint64
	for _, v := range c.views {
		total += c.viewCostLocked(v)
	}
	for v := range c.retired {
		total += c.viewCostLocked(v)
	}
	for a := range c.retiredAtts {
		total += a.bytes
	}
	return total
}

// enforceLocked evicts to satisfy the view count and byte budget. Oldest
// history goes first; an evicted view that is still pinned is retired (kept,
// counted, closed on its last release). The newest view is never evicted here.
// If still over budget, the oldest active leases are cancelled so retired
// views can drain; their readers are not closed until released.
func (c *refCatalog) enforceLocked(toClose []backend.ReadView) []backend.ReadView {
	for len(c.views) > 1 {
		overCount := len(c.views) > c.limits.MaxViews
		if !overCount && c.retainedBytesLocked() <= c.limits.RetentionBudgetBytes {
			break
		}
		// Unpinned history first: evicting it actually frees memory. A pinned
		// view is retired (hidden from lookup, still counted) only to satisfy
		// the view count, since retiring it frees no bytes.
		idx := -1
		for i, v := range c.views[:len(c.views)-1] {
			if v.refs == 0 {
				idx = i
				break
			}
		}
		if idx < 0 {
			if !overCount {
				break
			}
			idx = 0
		}
		v := c.views[idx]
		c.views = append(c.views[:idx:idx], c.views[idx+1:]...)
		c.evictions++
		toClose = c.retireViewLocked(v, toClose)
	}
	if c.retainedBytesLocked() > c.limits.RetentionBudgetBytes {
		c.cancelOldestLeasesLocked()
	}
	return toClose
}

func (c *refCatalog) retireViewLocked(v *refView, toClose []backend.ReadView) []backend.ReadView {
	v.retired = true
	if v.refs > 0 {
		c.retired[v] = struct{}{}
		return toClose
	}
	return append(toClose, v.reader)
}

func (c *refCatalog) cancelOldestLeasesLocked() {
	// Leases are cancelled oldest-view-first; a lease on a retired view goes
	// before one on a retained view.
	for l := range c.leases {
		if l.v.retired && l.ctx.Err() == nil {
			l.cancel()
			c.pressureKills++
		}
	}
	if c.retainedBytesLocked() <= c.limits.RetentionBudgetBytes {
		return
	}
	for _, v := range c.views {
		for l := range c.leases {
			if l.v == v && l.ctx.Err() == nil {
				l.cancel()
				c.pressureKills++
			}
		}
		if v.refs > 0 {
			return
		}
	}
}

// AcquireLatest pins the newest published view with its latest certification.
// The latest certification is served even without a partition view; if the
// newest certification could not be retained it fails with ErrViewNotRetained
// rather than returning older material.
func (c *refCatalog) AcquireLatest(ctx context.Context) (*refLease, error) {
	return c.acquire(ctx, nil)
}

// AcquireRoot pins the newest retained view whose root equals root, with that
// view's latest certification. Callers cannot select a particular seal.
func (c *refCatalog) AcquireRoot(ctx context.Context, root []byte) (*refLease, error) {
	if len(root) == 0 {
		return nil, ErrViewNotRetained
	}
	return c.acquire(ctx, root)
}

func (c *refCatalog) acquire(ctx context.Context, root []byte) (*refLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	var v *refView
	if root == nil {
		if len(c.views) == 0 && !c.latestStale {
			return nil, ErrNoFinalizedView
		}
		if c.latestStale || len(c.views) == 0 {
			return nil, ErrViewNotRetained
		}
		v = c.views[len(c.views)-1]
	} else {
		for i := len(c.views) - 1; i >= 0; i-- {
			if bytes.Equal(c.views[i].root, root) {
				v = c.views[i]
				break
			}
		}
		if v == nil {
			return nil, ErrViewNotRetained
		}
	}
	if len(v.atts) == 0 {
		return nil, ErrViewNotRetained
	}
	if len(c.leases) >= c.limits.MaxConcurrentPins {
		return nil, ErrBusyPins
	}
	if c.retainedBytesLocked() > c.limits.RetentionBudgetBytes {
		return nil, ErrBusyBudget
	}
	att := v.atts[len(v.atts)-1]
	lctx, cancel := context.WithTimeout(ctx, c.limits.MaxLeaseDuration)
	l := &refLease{c: c, v: v, att: att, ctx: lctx, cancel: cancel}
	v.refs++
	att.refs++
	c.leases[l] = struct{}{}
	return l, nil
}

func (c *refCatalog) release(l *refLease) {
	var toClose []backend.ReadView
	c.mu.Lock()
	delete(c.leases, l)
	l.att.refs--
	if l.att.refs == 0 {
		delete(c.retiredAtts, l.att)
	}
	l.v.refs--
	if l.v.refs == 0 && l.v.retired {
		delete(c.retired, l.v)
		toClose = append(toClose, l.v.reader)
	}
	// A release can make an over-budget view evictable.
	if !c.closed {
		toClose = c.enforceLocked(toClose)
	}
	c.mu.Unlock()
	c.settle(toClose)
}

// settle closes readers that left the catalog, outside the lock, then re-runs
// retention enforcement. Closing a reader can move its accounted charge onto an
// older surviving (possibly pinned) reader; until the close the charge was not
// visible, so pressure that appears only afterwards would otherwise go
// unnoticed. Enforcement never closes a reader a lease still holds.
func (c *refCatalog) settle(toClose []backend.ReadView) {
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
func (c *refCatalog) Close() error {
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
	for _, v := range c.views {
		toClose = c.retireViewLocked(v, toClose)
	}
	c.views = nil
	c.mu.Unlock()
	closeAll(toClose)
	return nil
}

// refLease pins one view in the reference model.
type refLease struct {
	c        *refCatalog
	v        *refView
	att      *refAttachment
	ctx      context.Context
	cancel   context.CancelFunc
	released bool
}

func (l *refLease) Release() {
	if l.released {
		return
	}
	l.released = true
	l.cancel()
	l.c.release(l)
}
