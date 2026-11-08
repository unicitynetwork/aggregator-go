// Package utb serves the Unicity Trust Base history (signed record version 2).
//
// The history starts from a bundle file an operator ships with the deployment: the trust anchor, verified completely from an operator-supplied genesis
// pin. It then grows: Append adds the next entry, verified from the held tip, never replacing anything, and every entry is persisted (the bundle's own
// included) before it is served, so a restart never falls back to a shorter history. Append is the one seam a later source (the BFT root connection,
// once bft-core serves V2) feeds; nothing feeds it yet. Refresh only copies entries another process sharing the database has already persisted.
//
// Respond answers GetTrustBase queries from the held history with the same page semantics as the root peers (statuses, frozen snapshot, paging caps),
// so a client's fetch loop works against either. A history that is shorter than another replica's is still valid: a client asks the rest elsewhere
// or later.
package utb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"
)

const (
	// DefaultMaxFileBytes and DefaultMaxEntries are the file budget of a shipped bundle; a release never has a fixed history length.
	DefaultMaxFileBytes = 64 << 20
	DefaultMaxEntries   = 1 << 16
)

var (
	// ErrLoad is returned for every reason the configured bundle cannot be used; the aggregator refuses to start.
	ErrLoad = errors.New("trust-base bundle refused")
	// ErrStore: the durable store is inconsistent with the bundle or with itself (another history, a hole, a missing or corrupt tail); the aggregator
	// refuses to start, or a refresh reports it.
	ErrStore = errors.New("trust-base store inconsistent")
	// Append errors.
	ErrConflict = errors.New("a different trust-base entry is held for the epoch")
	ErrGap      = errors.New("trust-base entry is ahead of the next epoch")
	ErrChain    = errors.New("trust-base entry does not extend the held history")
)

// Outcome of Append.
type Outcome int

const (
	// Appended: the entry is new to this process and now served.
	Appended Outcome = iota + 1
	// NoOp: the same entry is already held.
	NoOp
)

// LoadConfig is what the operator configures.
type LoadConfig struct {
	Path string
	// Pin is the genesis EntryID, from authenticated operator configuration; it is never computed from the file.
	Pin [types.TrustBaseV2IDLen]byte
	// Network is the network the aggregator serves, from independent configuration (never taken from the bundle).
	Network types.NetworkID
	// MaxFileBytes and MaxEntries bound the read; zero takes the defaults.
	MaxFileBytes int
	MaxEntries   int
}

// History is a verified, append-only trust-base history.
type History struct {
	network types.NetworkID
	genesis uint64
	pin     [types.TrustBaseV2IDLen]byte
	store   Store

	wmu sync.Mutex // serialises Append and Refresh

	mu      sync.RWMutex // guards the slices (append-only) and the refresh status
	raw     [][]byte
	entries []*types.RootTrustBaseV2
	ids     [][types.TrustBaseV2IDLen]byte

	refreshErr error
	refreshAt  time.Time
}

// view is an immutable snapshot of the history: the slices are append-only, so a copy of their headers stays valid while the history grows.
type view struct {
	network types.NetworkID
	genesis uint64
	raw     [][]byte
	ids     [][types.TrustBaseV2IDLen]byte
}

func (h *History) view() view {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return view{network: h.network, genesis: h.genesis, raw: h.raw, ids: h.ids}
}

// loadBundle reads, strictly decodes and fully verifies the bundle: the genesis is the pinned identity, every later entry carries a strict quorum of its
// predecessor's members, the history is complete from the genesis, the network is the configured one and a page can carry one maximum-size entry.
func loadBundle(cfg LoadConfig) (*types.TrustBaseBundle, error) {
	if cfg.MaxFileBytes <= 0 {
		cfg.MaxFileBytes = DefaultMaxFileBytes
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = DefaultMaxEntries
	}
	if cfg.Network == 0 {
		return nil, fmt.Errorf("%w: no expected network configured", ErrLoad)
	}
	data, err := readBounded(cfg.Path, cfg.MaxFileBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLoad, err)
	}
	b, err := types.DecodeTrustBaseBundle(data, types.TrustBaseLimits{MaxBytes: cfg.MaxFileBytes, MaxEntries: cfg.MaxEntries})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLoad, err)
	}
	if err := types.VerifyTrustBaseBundle(b, cfg.Pin); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLoad, err)
	}
	if b.Network != cfg.Network {
		return nil, fmt.Errorf("%w: the bundle is of network %d, this aggregator serves network %d", ErrLoad, b.Network, cfg.Network)
	}
	if types.MaxTrustBaseEntryBytes+types.TrustBaseResponseEnvelopeBound > types.DefaultTrustBaseMaxResponse {
		return nil, fmt.Errorf("%w: a page cannot carry one maximum-size entry", ErrLoad)
	}
	return b, nil
}

// Open loads the bundle (the trust anchor) and the store, and returns the held history: the bundle plus every verified entry the store holds beyond it.
//
// The bundle must be a prefix of the held history: a stored entry at an epoch the bundle covers must have the bundle's EntryID, and each stored entry
// beyond it must be exactly the next epoch and verify from the held tip (the same strict check as Append). The complete history, bundle entries
// included, is then written to the store, so an older matching bundle on a later restart changes nothing. Open refuses (ErrStore) when the store is
// bound to another network or genesis, holds a different history, has a hole, or holds less than its own durable tip says (a missing or corrupt tail).
// It cannot detect a rollback of the whole database together with its metadata; that needs a checkpoint from outside.
func Open(ctx context.Context, cfg LoadConfig, store Store) (*History, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: no durable store", ErrStore)
	}
	b, err := loadBundle(cfg)
	if err != nil {
		return nil, err
	}
	h := &History{network: b.Network, genesis: b.Entries[0].Epoch, pin: cfg.Pin, store: store}
	for i, e := range b.Entries {
		id, err := e.EntryID()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrLoad, err)
		}
		h.raw, h.entries, h.ids = append(h.raw, b.Raw[i]), append(h.entries, e), append(h.ids, id)
	}
	bundleTip := h.entries[len(h.entries)-1].Epoch

	if err := store.Bind(ctx, h.network, cfg.Pin); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStore, err)
	}
	meta, stored, err := store.Load(ctx, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: reading the store: %w", ErrStore, err)
	}
	present := map[uint64]bool{}
	for _, raw := range stored {
		if len(raw) > types.MaxTrustBaseEntryBytes {
			return nil, fmt.Errorf("%w: a stored entry of %d bytes exceeds the cap of %d", ErrStore, len(raw), types.MaxTrustBaseEntryBytes)
		}
		e, err := types.DecodeRootTrustBaseV2(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: a stored entry does not decode: %v", ErrStore, err)
		}
		id, err := e.EntryID()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrStore, err)
		}
		if present[e.Epoch] {
			return nil, fmt.Errorf("%w: epoch %d is stored twice", ErrStore, e.Epoch)
		}
		present[e.Epoch] = true
		switch {
		case e.NetworkID != h.network || e.Epoch < h.genesis:
			return nil, fmt.Errorf("%w: stored epoch %d is not of this history", ErrStore, e.Epoch)
		case e.Epoch <= bundleTip:
			if id != h.ids[e.Epoch-h.genesis] {
				return nil, fmt.Errorf("%w: stored epoch %d differs from the bundle's", ErrStore, e.Epoch)
			}
		default:
			if err := h.verifyNext(e, id); err != nil {
				return nil, fmt.Errorf("%w: stored epoch %d: %v", ErrStore, e.Epoch, err)
			}
			h.raw, h.entries, h.ids = append(h.raw, raw), append(h.entries, e), append(h.ids, id)
		}
	}
	tipEpoch := h.entries[len(h.entries)-1].Epoch
	if meta.TipEpoch != 0 {
		if meta.TipEpoch < h.genesis || meta.TipEpoch > tipEpoch || h.ids[meta.TipEpoch-h.genesis] != meta.TipID {
			return nil, fmt.Errorf("%w: the store acknowledged epoch %d but holds a shorter or different history (missing or corrupt tail)", ErrStore, meta.TipEpoch)
		}
	}
	// Write what the store lacks (the bundle's entries first), then advance the tip.
	for i, raw := range h.raw {
		epoch := h.genesis + uint64(i)
		if present[epoch] {
			continue
		}
		if err := store.Put(ctx, epoch, raw); err != nil && !errors.Is(err, ErrExists) {
			return nil, fmt.Errorf("%w: writing epoch %d: %w", ErrStore, epoch, err)
		}
	}
	if err := store.Advance(ctx, tipEpoch, h.ids[len(h.ids)-1]); err != nil {
		return nil, fmt.Errorf("%w: advancing the tip: %w", ErrStore, err)
	}
	return h, nil
}

// verifyNext checks e is exactly the epoch after the held tip and verifies from it. The caller holds the write lock (or owns h).
func (h *History) verifyNext(e *types.RootTrustBaseV2, _ [types.TrustBaseV2IDLen]byte) error {
	tip := h.entries[len(h.entries)-1]
	if e.NetworkID != h.network {
		return errors.New("another network")
	}
	if e.Epoch != tip.Epoch+1 {
		return fmt.Errorf("epoch %d does not follow the held tip %d", e.Epoch, tip.Epoch)
	}
	return e.VerifySuccessor(tip)
}

// Append adds the next entry: strict decode, exactly the epoch after the held tip, verified from the held tip (previous-quorum signatures, hash link,
// network, start round), persisted, then served. Held entries are never replaced. The same entry again is a NoOp (its witness must still verify); a
// different entry at a held epoch is ErrConflict, one ahead of the next epoch ErrGap, one that does not extend the tip ErrChain. If the store already
// holds the epoch (another process wrote it) the stored bytes are validated and adopted: the entry is new to this process, so it is Appended without
// being written again. A failed write publishes nothing; retrying is safe.
//
// This is the seam for a later source of entries.
func (h *History) Append(ctx context.Context, raw []byte) (Outcome, error) {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	// A page refuses an entry over this size, so a stored one would make every page that reaches it undecodable for the clients, for good.
	if len(raw) > types.MaxTrustBaseEntryBytes {
		return 0, fmt.Errorf("%w: entry of %d bytes, the cap is %d", ErrChain, len(raw), types.MaxTrustBaseEntryBytes)
	}
	e, err := types.DecodeRootTrustBaseV2(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrChain, err)
	}
	id, err := e.EntryID()
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrChain, err)
	}
	v := h.view()
	tipEpoch := v.genesis + uint64(len(v.raw)) - 1
	switch {
	case e.NetworkID != h.network || e.Epoch < h.genesis:
		return 0, fmt.Errorf("%w: epoch %d of network %d", ErrChain, e.Epoch, e.NetworkID)
	case e.Epoch <= tipEpoch:
		if id != v.ids[e.Epoch-v.genesis] {
			return 0, fmt.Errorf("%w: epoch %d", ErrConflict, e.Epoch)
		}
		if e.Epoch == h.genesis {
			if err := e.VerifyGenesis(); err != nil {
				return 0, fmt.Errorf("%w: %v", ErrChain, err)
			}
			return NoOp, nil
		}
		prev, err := types.DecodeRootTrustBaseV2(v.raw[e.Epoch-v.genesis-1])
		if err != nil {
			return 0, fmt.Errorf("%w: %v", ErrStore, err)
		}
		if err := e.VerifySuccessor(prev); err != nil {
			return 0, fmt.Errorf("%w: the witness of epoch %d: %v", ErrChain, e.Epoch, err)
		}
		return NoOp, nil
	case e.Epoch > tipEpoch+1:
		return 0, fmt.Errorf("%w: epoch %d, held tip %d", ErrGap, e.Epoch, tipEpoch)
	}
	if err := h.verifyNext(e, id); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrChain, err)
	}
	switch err := h.store.Put(ctx, e.Epoch, raw); {
	case err == nil:
	case errors.Is(err, ErrExists):
		stored, gerr := h.store.Get(ctx, e.Epoch)
		if gerr != nil {
			return 0, fmt.Errorf("%w: reading the stored epoch %d: %w", ErrStore, e.Epoch, gerr)
		}
		se, derr := types.DecodeRootTrustBaseV2(stored)
		if derr != nil {
			return 0, fmt.Errorf("%w: the stored epoch %d does not decode: %v", ErrStore, e.Epoch, derr)
		}
		sid, derr := se.EntryID()
		if derr != nil {
			return 0, fmt.Errorf("%w: %v", ErrStore, derr)
		}
		if sid != id {
			return 0, fmt.Errorf("%w: the store holds another entry for epoch %d", ErrConflict, e.Epoch)
		}
		if verr := h.verifyNext(se, sid); verr != nil {
			return 0, fmt.Errorf("%w: the stored epoch %d: %v", ErrStore, e.Epoch, verr)
		}
		raw, e = stored, se
	default:
		return 0, fmt.Errorf("persisting epoch %d: %w", e.Epoch, err)
	}
	if err := h.store.Advance(ctx, e.Epoch, id); err != nil {
		return 0, fmt.Errorf("advancing the stored tip to epoch %d: %w", e.Epoch, err)
	}
	h.publish(raw, e, id)
	return Appended, nil
}

func (h *History) publish(raw []byte, e *types.RootTrustBaseV2, id [types.TrustBaseV2IDLen]byte) {
	h.mu.Lock()
	h.raw, h.entries, h.ids = append(h.raw, raw), append(h.entries, e), append(h.ids, id)
	h.mu.Unlock()
}

// Refresh adopts entries another process sharing the store has persisted: those beyond the held tip and within the durable tip read first, each
// verified from the held tip by the same strict check, without being written again. It returns how many were adopted. It is not a source of new
// entries. The outcome is remembered for Status.
func (h *History) Refresh(ctx context.Context) (adopted int, err error) {
	h.wmu.Lock()
	defer h.wmu.Unlock()
	defer func() {
		h.mu.Lock()
		h.refreshErr, h.refreshAt = err, time.Now()
		h.mu.Unlock()
	}()
	v := h.view()
	tipEpoch := v.genesis + uint64(len(v.raw)) - 1
	meta, stored, err := h.store.Load(ctx, tipEpoch+1)
	if err != nil {
		return 0, fmt.Errorf("%w: reading the store: %w", ErrStore, err)
	}
	for _, raw := range stored {
		if len(raw) > types.MaxTrustBaseEntryBytes {
			return adopted, fmt.Errorf("%w: a stored entry of %d bytes exceeds the cap of %d", ErrStore, len(raw), types.MaxTrustBaseEntryBytes)
		}
		e, derr := types.DecodeRootTrustBaseV2(raw)
		if derr != nil {
			return adopted, fmt.Errorf("%w: a stored entry does not decode: %v", ErrStore, derr)
		}
		if e.Epoch > meta.TipEpoch {
			break // not acknowledged yet (a write in flight, or interrupted before the tip advanced)
		}
		id, derr := e.EntryID()
		if derr != nil {
			return adopted, fmt.Errorf("%w: %v", ErrStore, derr)
		}
		if verr := h.verifyNext(e, id); verr != nil {
			return adopted, fmt.Errorf("%w: stored epoch %d: %v", ErrStore, e.Epoch, verr)
		}
		h.publish(raw, e, id)
		adopted++
	}
	if held, _ := h.Tip(); meta.TipEpoch > held {
		return adopted, fmt.Errorf("%w: the store acknowledged epoch %d, this process can read up to %d (missing tail)", ErrStore, meta.TipEpoch, held)
	}
	return adopted, nil
}

// Status is the outcome of the last Refresh (nil error and zero time before the first).
func (h *History) Status() (lastErr error, at time.Time) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.refreshErr, h.refreshAt
}

// readBounded reads a whole file but never more than max bytes.
func readBounded(path string, max int) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > max {
		return nil, fmt.Errorf("%s is larger than the %d byte budget", path, max)
	}
	return data, nil
}

// Network is the network of the history.
func (h *History) Network() types.NetworkID { return h.network }

// GenesisEpoch is the epoch of the first entry.
func (h *History) GenesisEpoch() uint64 { return h.genesis }

// Tip is the latest entry served.
func (h *History) Tip() (epoch uint64, id [types.TrustBaseV2IDLen]byte) {
	v := h.view()
	return v.genesis + uint64(len(v.raw)) - 1, v.ids[len(v.ids)-1]
}

// Entry returns the entry of an epoch, or nil outside the held history.
func (h *History) Entry(epoch uint64) *types.RootTrustBaseV2 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if epoch < h.genesis || epoch-h.genesis >= uint64(len(h.entries)) {
		return nil
	}
	return h.entries[epoch-h.genesis]
}

// Query is a parsed GET: the HTTP request carries no network and no byte budget (the page is capped at 1 MiB), so a client compares the response's
// network with its own.
type Query struct {
	From            uint64
	To              *uint64
	SnapshotEnd     *uint64
	SnapshotEntryID []byte
	MaxEntries      int // 1..DefaultTrustBaseMaxEntries
}

func (v view) reply(st types.TrustBaseStatus) *types.TrustBaseResponse {
	return &types.TrustBaseResponse{Version: types.TrustBaseResponseVersion, Network: v.network, Status: st}
}

// Unavailable is the answer while the history cannot be trusted to agree with the aggregator's own authority.
func (h *History) Unavailable() *types.TrustBaseResponse {
	return h.view().reply(types.TrustBaseStatusUnavailable)
}

// Respond answers one query from the history.
//
// The first page takes the tip as its snapshot (or the requested end, clamped to the tip) and echoes (effectiveEnd, endEntryID); later pages send
// them back as snapshotEnd/snapshotEntryId, so the end does not move. A replica whose history stops short of the snapshot answers NotYetAvailable,
// one with another entry at that epoch HistoryMismatch. `from` beyond the end is NotYetAvailable with the actual tip. A page is a contiguous
// ascending prefix within the entry and byte budgets, never empty.
func (h *History) Respond(q Query) *types.TrustBaseResponse {
	v := h.view()
	tipEpoch := v.genesis + uint64(len(v.raw)) - 1
	tipID := v.ids[len(v.ids)-1]
	if q.MaxEntries < 1 || q.MaxEntries > types.DefaultTrustBaseMaxEntries ||
		q.From < v.genesis || (q.To != nil && *q.To < q.From) ||
		(q.SnapshotEnd == nil) != (q.SnapshotEntryID == nil) ||
		(q.SnapshotEnd != nil && (len(q.SnapshotEntryID) != types.TrustBaseV2IDLen || q.From > *q.SnapshotEnd ||
			(q.To != nil && *q.To < *q.SnapshotEnd))) {
		return v.reply(types.TrustBaseStatusInvalidRequest)
	}
	end, endID := tipEpoch, tipID
	switch {
	case q.SnapshotEnd != nil:
		if tipEpoch < *q.SnapshotEnd {
			r := v.reply(types.TrustBaseStatusNotYetAvailable)
			r.FromEpoch, r.EffectiveEnd, r.EndEntryID = q.From, tipEpoch, tipID[:]
			return r
		}
		id := v.ids[*q.SnapshotEnd-v.genesis]
		if !bytes.Equal(id[:], q.SnapshotEntryID) {
			return v.reply(types.TrustBaseStatusHistoryMismatch)
		}
		end, endID = *q.SnapshotEnd, id
	case q.To != nil && *q.To < end:
		end, endID = *q.To, v.ids[*q.To-v.genesis]
	}
	if q.From > end {
		r := v.reply(types.TrustBaseStatusNotYetAvailable)
		r.FromEpoch, r.EffectiveEnd, r.EndEntryID = q.From, tipEpoch, tipID[:]
		return r
	}

	budget := types.DefaultTrustBaseMaxResponse - types.TrustBaseResponseEnvelopeBound
	r := v.reply(types.TrustBaseStatusOK)
	r.FromEpoch, r.EffectiveEnd, r.EndEntryID = q.From, end, endID[:]
	used := 0
	epoch := q.From
	for ; epoch <= end && len(r.Entries) < q.MaxEntries; epoch++ {
		raw := v.raw[epoch-v.genesis]
		if used+len(raw) > budget {
			break
		}
		used += len(raw)
		r.Entries = append(r.Entries, raw)
	}
	if len(r.Entries) == 0 {
		return v.reply(types.TrustBaseStatusLimitTooSmall)
	}
	if epoch <= end {
		next := epoch
		r.NextEpoch = &next
	}
	return r
}
