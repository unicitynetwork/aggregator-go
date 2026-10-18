package bft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

const (
	// peerRate and peerBurst bound the certification responses accepted from one admitted root
	// peer (token bucket). A root sends about one UC per root round, so this is far above honest
	// traffic and keeps one faulty root from filling the shared receive queue.
	peerRate  = 20.0 // frames per second
	peerBurst = 40.0
)

// IntakeTrustBases is the view of the trust base history the certification intake needs. Nothing
// here talks to the network except AdmitNext, which only the catch-up worker calls.
type IntakeTrustBases interface {
	// GetByEpochLocal returns an admitted trust base from the local store, never from the BFT node.
	GetByEpochLocal(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error)
	// Head is the highest admitted epoch (the history below it is contiguous).
	Head(ctx context.Context) (uint64, error)
	// AdmitNext fetches, verifies and stores the epoch after the head.
	AdmitNext(ctx context.Context) (uint64, error)
}

// localHistory adapts a plain trust base store, which has no head and no way to fetch more.
type localHistory struct{ store TrustBaseStore }

func (l localHistory) GetByEpochLocal(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	return l.store.GetByEpoch(ctx, epoch)
}

func (localHistory) Head(context.Context) (uint64, error) {
	return 0, errors.New("the trust base store does not report its head")
}

func (localHistory) AdmitNext(context.Context) (uint64, error) {
	return 0, errors.New("the trust base store can not admit new epochs")
}

// peerSet is an immutable set of peer IDs.
type peerSet map[peer.ID]struct{}

// Membership is the set of peer IDs allowed to send certification responses. It is derived only from
// admitted (verified) root trust bases: let E be the epoch of the last authenticated live
// certificate and H the admitted head. The set is the union of the root nodes of E-1, E and E+1
// (transition overlap) and, while E is behind H, also of H-1 and H. It is published atomically on
// admission and on live-epoch advancement; peers outside it are rejected before a frame is read.
// Nothing received from the network (a UC, a bootstrap peer, an unverified answer) adds an ID.
type Membership struct {
	history IntakeTrustBases

	set  atomic.Pointer[peerSet]
	live atomic.Uint64 // E
	head atomic.Uint64 // H as of the last refresh

	mu     sync.Mutex
	quotas map[peer.ID]*bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newMembership(history IntakeTrustBases, liveEpoch uint64) *Membership {
	m := &Membership{history: history, quotas: map[peer.ID]*bucket{}}
	m.live.Store(liveEpoch)
	empty := peerSet{}
	m.set.Store(&empty)
	return m
}

// Refresh recomputes and publishes the set. It fails closed: on error the previous set stays.
func (m *Membership) Refresh(ctx context.Context) error {
	live := m.live.Load()
	head, err := m.history.Head(ctx)
	if err != nil || head < live {
		head = live
	}
	if live == 0 {
		// no authenticated certificate yet (genesis): the live epoch is the head (epochs start at 1)
		live = max(head, 1)
		head = live
		m.live.CompareAndSwap(0, live)
	}
	epochs := map[uint64]struct{}{live: {}, live + 1: {}}
	if live > 0 {
		epochs[live-1] = struct{}{}
	}
	if live < head {
		epochs[head] = struct{}{}
		epochs[head-1] = struct{}{}
	}
	next := peerSet{}
	haveLive := false
	for e := range epochs {
		if e == 0 || e > head {
			continue
		}
		tb, err := m.history.GetByEpochLocal(ctx, e)
		if errors.Is(err, interfaces.ErrTrustBaseNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to load trust base for epoch %d: %w", e, err)
		}
		ids, err := rootNodesIDSlice(tb)
		if err != nil {
			return fmt.Errorf("epoch %d: %w", e, err)
		}
		for _, id := range ids {
			next[id] = struct{}{}
		}
		if e == live {
			haveLive = true
		}
	}
	if !haveLive {
		return fmt.Errorf("no admitted trust base for the live epoch %d", live)
	}
	m.head.Store(head)
	m.set.Store(&next)
	m.mu.Lock()
	for id := range m.quotas {
		if _, ok := next[id]; !ok {
			delete(m.quotas, id)
		}
	}
	m.mu.Unlock()
	return nil
}

// Advance records a newly authenticated live epoch and republishes the set when it grew.
func (m *Membership) Advance(ctx context.Context, epoch uint64) error {
	for {
		cur := m.live.Load()
		if epoch <= cur {
			return nil
		}
		if m.live.CompareAndSwap(cur, epoch) {
			break
		}
	}
	return m.Refresh(ctx)
}

// Contains reports whether the peer is currently a member.
func (m *Membership) Contains(id peer.ID) bool {
	_, ok := (*m.set.Load())[id]
	return ok
}

// Admit is the bft-core receive-protocol admission callback: membership first, then the per-peer
// frame quota. It runs before every frame of a stream, so a peer that leaves the set or exceeds its
// quota has its stream reset.
func (m *Membership) Admit(id peer.ID) bool {
	return m.Contains(id) && m.allow(id, time.Now())
}

func (m *Membership) allow(id peer.ID, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.quotas[id]
	if !ok {
		b = &bucket{tokens: peerBurst, last: now}
		m.quotas[id] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * peerRate
	if b.tokens > peerBurst {
		b.tokens = peerBurst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
