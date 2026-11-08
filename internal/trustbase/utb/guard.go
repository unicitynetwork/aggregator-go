package utb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

var (
	// ErrAuthority: the held history contradicts the aggregator's own trust base (another network, or the same epoch with a different authority). It is
	// a real conflict: the aggregator refuses to start, or stops serving the history.
	ErrAuthority = errors.New("trust-base history disagrees with the aggregator's trust base")
	// ErrBehind: the aggregator's active epoch is not in the held history yet (ahead of its tip, or below its genesis). That is not a fork: the verified
	// prefix is served, NotYetAvailable beyond it, and readiness is degraded until an append covers the epoch.
	ErrBehind = errors.New("trust-base history does not cover the aggregator's active epoch yet")
)

// CheckAuthority compares the held history with a V1 trust base of the aggregator on the fields both records carry: network, epoch, start round,
// members (node id, key, stake) and quorum threshold. V1 hashes are not compared with V2 entry ids (they are different identities). An epoch the held
// history does not contain is ErrBehind, a differing one ErrAuthority.
func (h *History) CheckAuthority(v1 *types.RootTrustBaseV1) error {
	if v1.GetNetworkID() != h.network {
		return fmt.Errorf("%w: V1 trust base is of network %d, the history of %d", ErrAuthority, v1.GetNetworkID(), h.network)
	}
	e := h.Entry(v1.GetEpoch())
	if e == nil {
		tip, _ := h.Tip()
		return fmt.Errorf("%w: V1 epoch %d, held epochs %d..%d", ErrBehind, v1.GetEpoch(), h.genesis, tip)
	}
	if e.EpochStart != v1.GetEpochStart() {
		return fmt.Errorf("%w: epoch %d starts at round %d in V1 and %d in the history", ErrAuthority, e.Epoch, v1.GetEpochStart(), e.EpochStart)
	}
	if e.QuorumThreshold != v1.GetQuorumThreshold() {
		return fmt.Errorf("%w: epoch %d quorum threshold %d in V1 and %d in the history", ErrAuthority, e.Epoch, v1.GetQuorumThreshold(), e.QuorumThreshold)
	}
	have := v1.GetRootNodes()
	if len(have) != len(e.RootNodes) {
		return fmt.Errorf("%w: epoch %d has %d members in V1 and %d in the history", ErrAuthority, e.Epoch, len(have), len(e.RootNodes))
	}
	byID := make(map[string]*types.NodeInfo, len(e.RootNodes))
	for _, n := range e.RootNodes {
		byID[n.NodeID] = n
	}
	for _, n := range have {
		m := byID[n.NodeID]
		if m == nil || !bytes.Equal(m.SigKey, n.SigKey) || m.Stake != n.Stake {
			return fmt.Errorf("%w: epoch %d member %q differs (key or stake)", ErrAuthority, e.Epoch, n.NodeID)
		}
	}
	return nil
}

// fingerprint is a canonical identity of the authority a V1 trust base carries, so a same-epoch change is noticed.
func fingerprint(v1 *types.RootTrustBaseV1) [sha256.Size]byte {
	h := sha256.New()
	var b [8]byte
	put := func(v uint64) { binary.BigEndian.PutUint64(b[:], v); h.Write(b[:]) }
	put(uint64(v1.GetNetworkID()))
	put(v1.GetEpoch())
	put(v1.GetEpochStart())
	put(v1.GetQuorumThreshold())
	nodes := append([]*types.NodeInfo(nil), v1.GetRootNodes()...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	for _, n := range nodes {
		put(uint64(len(n.NodeID)))
		h.Write([]byte(n.NodeID))
		put(uint64(len(n.SigKey)))
		h.Write(n.SigKey)
		put(n.Stake)
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

// LatestV1 returns the aggregator's latest V1 trust base; interfaces.ErrTrustBaseNotFound means none is installed yet.
type LatestV1 func(ctx context.Context) (*types.RootTrustBaseV1, error)

type guardKey struct {
	epoch uint64
	print [sha256.Size]byte
	tip   uint64
}

// Guard keeps the served history consistent with the aggregator's mutable V1 trust base (which PUT and the BFT admission can extend while the V2
// history only grows by Append). "Active" is the aggregator's highest admitted V1 trust base; whether that is the authenticated certified epoch is not
// established here. The verdict is remembered per (V1 epoch, authority fingerprint, held tip), so neither a same-epoch change nor a later append is
// missed. It never repairs anything. An empty V1 store is consistent (nothing to contradict yet).
type Guard struct {
	history *History
	latest  LatestV1

	mu   sync.Mutex
	key  guardKey
	done bool
	err  error
}

// NewGuard returns a guard for h over the V1 source.
func NewGuard(h *History, latest LatestV1) *Guard { return &Guard{history: h, latest: latest} }

// History is the guarded history.
func (g *Guard) History() *History { return g.history }

// Check returns nil when the history covers the active authority and agrees with it, an error wrapping ErrBehind while it does not cover it yet (the
// prefix may be served), one wrapping ErrAuthority for a conflict (it must not be served), and any other error for a transient failure to read V1
// (not remembered).
func (g *Guard) Check(ctx context.Context) error {
	v1, err := g.latest(ctx)
	if errors.Is(err, interfaces.ErrTrustBaseNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading the aggregator's latest trust base: %w", err)
	}
	tip, _ := g.history.Tip()
	key := guardKey{epoch: v1.GetEpoch(), print: fingerprint(v1), tip: tip}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.done && g.key == key {
		return g.err
	}
	g.done, g.key, g.err = true, key, g.history.CheckAuthority(v1)
	return g.err
}

// CheckInstalled is the startup check: every installed V1 trust base of an epoch the held history covers must agree with it (a conflict is an error);
// an active epoch the history does not cover yet is not (the aggregator starts, unready for that authority, until an append covers it).
func (g *Guard) CheckInstalled(ctx context.Context, list func(ctx context.Context, from, to uint64) ([]*types.RootTrustBaseV1, error)) error {
	if err := g.Check(ctx); err != nil && !errors.Is(err, ErrBehind) {
		return err
	}
	head, err := g.latest(ctx)
	if errors.Is(err, interfaces.ErrTrustBaseNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading the aggregator's latest trust base: %w", err)
	}
	if head.GetEpoch() < g.history.GenesisEpoch() {
		return nil // below the anchor: nothing to compare yet
	}
	all, err := list(ctx, g.history.GenesisEpoch(), head.GetEpoch())
	if err != nil {
		return fmt.Errorf("reading the aggregator's trust bases: %w", err)
	}
	for _, v1 := range all {
		if err := g.history.CheckAuthority(v1); err != nil && !errors.Is(err, ErrBehind) {
			return err
		}
	}
	return nil
}
