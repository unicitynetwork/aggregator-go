package bft

import (
	"context"
	"crypto/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cryptolibp2p "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-core/network"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	"github.com/unicitynetwork/bft-core/network/protocol/handshake"
	"github.com/unicitynetwork/bft-go-base/types"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	tnoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
)

// flakyHistory injects failures and a pause into the local history reads.
type flakyHistory struct {
	IntakeTrustBases
	failHead  atomic.Bool
	failGets  atomic.Int32 // number of upcoming GetByEpochLocal calls that fail
	headHook  func()       // called at the start of Head (may block)
	headCalls atomic.Int32
}

func (f *flakyHistory) Head(ctx context.Context) (uint64, error) {
	f.headCalls.Add(1)
	if f.headHook != nil {
		f.headHook()
	}
	if f.failHead.Load() {
		return 0, context.DeadlineExceeded
	}
	return f.IntakeTrustBases.Head(ctx)
}

func (f *flakyHistory) GetByEpochLocal(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	if f.failGets.Load() > 0 && f.failGets.Add(-1) >= 0 {
		return nil, context.DeadlineExceeded
	}
	return f.IntakeTrustBases.GetByEpochLocal(ctx, epoch)
}

// wrapHistory routes every history read of the harness client through a flakyHistory.
func (h *harness) wrapHistory() *flakyHistory {
	f := &flakyHistory{IntakeTrustBases: h.client.history}
	h.client.history = f
	h.client.members.history = f
	h.client.catchup.history = f
	return f
}

// A failed head read must not shrink the published set to the live epoch.
func TestMembership_FailedHeadReadKeepsThePreviousSet(t *testing.T) {
	h := newHarness(t, 5, 5, 5)
	f := h.wrapHistory()
	m := h.client.members
	m.live.Store(2)
	require.NoError(t, m.Refresh(t.Context()))
	for e := uint64(1); e <= 5; e++ {
		require.True(t, m.Contains(h.chain.ids[e][0]), "epoch %d", e)
	}
	f.failHead.Store(true)
	require.Error(t, m.Refresh(t.Context()))
	require.True(t, m.Dirty())
	for e := uint64(1); e <= 5; e++ {
		require.True(t, m.Contains(h.chain.ids[e][0]), "epoch %d roots stay admitted after a failed head read", e)
	}
	f.failHead.Store(false)
	require.NoError(t, m.Refresh(t.Context()))
	require.False(t, m.Dirty())
}

// A transient local error while publishing the membership of a newly admitted epoch is retried at the
// same head, for an admission by this node and for one by another HA node.
func TestIntake_FailedPublicationIsRetriedAtTheSameHead(t *testing.T) {
	for _, mode := range []string{"own admission", "HA admission"} {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t, 2, 1, 2)
			f := h.wrapHistory()
			c := h.client
			newRoot := h.chain.ids[2][0]
			require.False(t, c.members.Contains(newRoot))
			if mode == "HA admission" {
				require.NoError(t, h.store.Store(t.Context(), h.chain.tbs[2]))
			}
			f.failGets.Store(1) // the publication after the admission fails once
			h.clock.advance(time.Second)
			c.catchup.runOnce(t.Context())
			require.EqualValues(t, 2, c.admittedHead.Load(), "the admission is visible")
			require.False(t, c.members.Contains(newRoot), "publication failed")
			require.True(t, c.members.Dirty())

			// nothing else changes: same head, same live epoch; the next run must still repair it
			h.clock.advance(time.Second)
			c.catchup.runOnce(t.Context())
			require.True(t, c.members.Contains(newRoot), "retried at the same head")
			require.False(t, c.members.Dirty())
		})
	}
}

// An older recomputation must not republish retired members after a newer one completed.
func TestMembership_ConcurrentRefreshCannotReAdmitRetiredRoots(t *testing.T) {
	h := newHarness(t, 5, 5, 5)
	f := h.wrapHistory()
	m := h.client.members
	m.live.Store(2)
	require.NoError(t, m.Refresh(t.Context()))
	require.True(t, m.Contains(h.chain.ids[2][0]))

	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.headHook = func() { once.Do(func() { close(entered); <-release }) }
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = m.Refresh(t.Context()) }() // computes with live=2 and pauses
	<-entered
	go func() { defer wg.Done(); _ = m.Advance(t.Context(), 5) }() // newer epoch, runs while the first is paused
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	require.EqualValues(t, 5, m.live.Load())
	require.False(t, m.Contains(h.chain.ids[2][0]), "epoch 2 roots stay retired")
	require.False(t, m.Contains(h.chain.ids[3][0]), "epoch 3 roots stay retired")
	require.True(t, m.Contains(h.chain.ids[4][0]))
	require.True(t, m.Contains(h.chain.ids[5][0]))
}

// The recovery handshake after a catch-up is actually delivered to the newly admitted roots: the send
// is asynchronous and must not inherit the cancelled context of the catch-up run.
func TestCatchUp_RecoveryHandshakeIsDelivered(t *testing.T) {
	h := newHarness(t, 2, 1, 2)
	h.useRealHandshake = true
	c := h.client
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)

	newPeer := func(signer interface{ MarshalPrivateKey() ([]byte, error) }) *network.Peer {
		raw, err := signer.MarshalPrivateKey()
		require.NoError(t, err)
		key, err := cryptolibp2p.UnmarshalSecp256k1PrivateKey(raw)
		require.NoError(t, err)
		priv, err := key.Raw()
		require.NoError(t, err)
		pub, err := key.GetPublic().Raw()
		require.NoError(t, err)
		conf, err := network.NewPeerConfiguration("/ip4/127.0.0.1/tcp/0", nil, &network.PeerKeyPair{PublicKey: pub, PrivateKey: priv}, nil, nil)
		require.NoError(t, err)
		p, err := network.NewPeer(t.Context(), conf, log.Logger, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = p.Close() })
		return p
	}
	obs := &Observability{mp: mnoop.NewMeterProvider(), tp: tnoop.NewTracerProvider(), logger: log.Logger}

	// the new roots are real libp2p peers (their IDs are those of the epoch 2 trust base) with handshake receivers
	var inboxes []<-chan any
	self := newPeer(&keyOnly{k: cryptolibp2pKey(t)})
	for _, id := range h.chain.ids[2] {
		var signer interface{ MarshalPrivateKey() ([]byte, error) }
		for nodeID, s := range h.chain.signers[2] {
			if nodeID == id.String() {
				signer = s
			}
		}
		require.NotNil(t, signer)
		rp := newPeer(signer)
		require.Equal(t, id, rp.ID(), "the receiver has the identity of the trust base node")
		nw, err := network.NewLibP2PNetwork(rp, 8, obs)
		require.NoError(t, err)
		require.NoError(t, nw.RegisterReceiveProtocols([]network.ReceiveProtocolDescription{{
			ProtocolID: network.ProtocolHandshake, TypeFn: func() any { return &handshake.Handshake{} },
		}}))
		inboxes = append(inboxes, nw.ReceivedChannel())
		self.Network().Peerstore().AddAddrs(rp.ID(), rp.MultiAddresses(), peerstore.PermanentAddrTTL)
	}
	bn, err := NewLibP2PNetwork(t.Context(), self, log, DefaultNetworkOptions)
	require.NoError(t, err)
	c.mu.Lock()
	c.peer, c.network = self, bn
	c.mu.Unlock()

	h.clock.advance(time.Second)
	c.catchup.runOnce(t.Context()) // admits epoch 2, then runs the real post-catch-up handshake
	require.EqualValues(t, 2, c.admittedHead.Load())

	delivered := 0
	for _, in := range inboxes {
		select {
		case m := <-in:
			require.IsType(t, &handshake.Handshake{}, m)
			delivered++
		case <-time.After(3 * time.Second):
		}
	}
	require.Equal(t, 3, delivered, "the recovery handshake reaches the newly admitted roots")
}

type keyOnly struct{ k []byte }

func (k *keyOnly) MarshalPrivateKey() ([]byte, error) { return k.k, nil }

func cryptolibp2pKey(t *testing.T) []byte {
	priv, _, err := cryptolibp2p.GenerateSecp256k1Key(rand.Reader)
	require.NoError(t, err)
	raw, err := priv.Raw()
	require.NoError(t, err)
	return raw
}

// While the root stays silent, recovery (handshake and catch-up wake) is paced by the inactivity
// timeout, not by every heartbeat.
func TestRecoveryIsPacedByTheInactivityTimeout(t *testing.T) {
	c := &BFTClientImpl{conf: &config.BFTConfig{InactivityTimeout: 5 * time.Second}}
	t0 := time.Now()
	c.lastCertResponseTime.Store(t0.UnixMilli())
	require.False(t, c.recoveryDue(t0.Add(4*time.Second)), "not inactive yet")
	due := 0
	for s := 6; s <= 60; s++ { // one heartbeat per second for a minute of silence
		if c.recoveryDue(t0.Add(time.Duration(s) * time.Second)) {
			due++
		}
	}
	require.LessOrEqual(t, due, 11, "about one recovery per inactivity timeout")
	require.GreaterOrEqual(t, due, 10)
	// an authenticated advancing UC ends the silence
	c.lastCertResponseTime.Store(t0.Add(61 * time.Second).UnixMilli())
	require.False(t, c.recoveryDue(t0.Add(62*time.Second)))
}

// The configured network, the exact epoch interval and the retirement boundary are enforced from the
// local history before the quorum counts, even for certificates signed by trusted keys.
func TestIntake_StrictSealContext(t *testing.T) {
	h := newHarness(t, 3, 3, 3)
	c := h.client
	reject := func(name string, mut func(*types.UnicitySeal), epoch uint64) {
		h.sealMut = mut
		defer func() { h.sealMut = nil }()
		before := c.lastCertResponseTime.Load()
		err := c.handleCertificationResponse(t.Context(), h.valid(9, epoch))
		require.Error(t, err, name)
		require.NotErrorIs(t, err, ErrEpochNotAdmitted, name)
		require.Equal(t, before, c.lastCertResponseTime.Load(), name)
		require.Empty(t, h.rm.startedRounds, name)
	}
	reject("foreign network", func(s *types.UnicitySeal) { s.NetworkID = 77 }, 2)
	reject("round before the epoch start", func(s *types.UnicitySeal) { s.RootChainRoundNumber = 1500 }, 2)
	reject("retired authority after the next epoch start", func(s *types.UnicitySeal) { s.RootChainRoundNumber = 2001 }, 1)
	// the same epochs inside their intervals are fine
	require.NoError(t, c.handleCertificationResponse(t.Context(), h.valid(9, 2)))
}

// A null nested authentication-path entry is rejected as malformed, never dereferenced.
func TestIntake_NullPartitionPathDoesNotPanic(t *testing.T) {
	h := newHarness(t, 2, 2, 2)
	resp := h.valid(1, 2)
	resp.UC.UnicityTreeCertificate.HashSteps = []*types.PathItem{nil}
	before := h.client.lastCertResponseTime.Load()
	require.NotPanics(t, func() {
		err := h.client.handleCertificationResponse(t.Context(), resp)
		require.Error(t, err)
	})
	require.Equal(t, before, h.client.lastCertResponseTime.Load())
}

// V1 frames are bounded before decoding, and an admitted root that floods does not take the intake from an
// honest root (real libp2p streams into the shared receive queue).
func TestIntake_LegacyFramesAreBoundedAndAFloodingRootDoesNotStarveAnother(t *testing.T) {
	h := newHarness(t, 1, 1, 1)
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	obs := &Observability{mp: mnoop.NewMeterProvider(), tp: tnoop.NewTracerProvider(), logger: log.Logger}
	newPeer := func() *network.Peer {
		raw := cryptolibp2pKey(t)
		key, err := cryptolibp2p.UnmarshalSecp256k1PrivateKey(raw)
		require.NoError(t, err)
		priv, _ := key.Raw()
		pub, _ := key.GetPublic().Raw()
		conf, err := network.NewPeerConfiguration("/ip4/127.0.0.1/tcp/0", nil, &network.PeerKeyPair{PublicKey: pub, PrivateKey: priv}, nil, nil)
		require.NoError(t, err)
		p, err := network.NewPeer(t.Context(), conf, log.Logger, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = p.Close() })
		return p
	}
	server, flooder, honest := newPeer(), newPeer(), newPeer()
	members := newMembership(nil, 1)
	set := peerSet{flooder.ID(): {}, honest.ID(): {}}
	members.set.Store(&set)
	opts := DefaultNetworkOptions
	opts.AdmitPeer, opts.RevalidatePeer = members.Admit, members.Contains
	srv, err := NewLibP2PNetwork(t.Context(), server, log, opts)
	require.NoError(t, err)

	sender := func(p *network.Peer) *network.LibP2PNetwork {
		nw, err := network.NewLibP2PNetwork(p, 1, obs)
		require.NoError(t, err)
		require.NoError(t, nw.RegisterSendProtocols([]network.SendProtocolDescription{{
			ProtocolID: network.ProtocolUnicityCertificates, Timeout: time.Second, MsgType: certification.CertificationResponse{},
		}}))
		p.Network().Peerstore().AddAddrs(server.ID(), server.MultiAddresses(), peerstore.PermanentAddrTTL)
		return nw
	}
	fl, ho := sender(flooder), sender(honest)

	// a V1 frame of 2 MiB from an admitted root is never decoded or enqueued
	big := h.valid(1, 1)
	big.Technical.Leader = strings.Repeat("x", 2<<20)
	_ = fl.Send(t.Context(), big, server.ID())
	select {
	case <-srv.ReceivedChannel():
		t.Fatal("an oversized legacy frame must not reach the intake queue")
	case <-time.After(500 * time.Millisecond):
	}

	// a flood of well-formed frames is limited by the per-peer budget ...
	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = fl.Send(t.Context(), h.valid(1, 1), server.ID()) }()
	}
	wg.Wait()
	time.Sleep(500 * time.Millisecond)
	queued := len(srv.ReceivedChannel())
	require.Less(t, queued, 120, "the flooding root is held to its budget (queue %d)", queued)

	// ... and the other root is still served
	for len(srv.ReceivedChannel()) > 0 {
		<-srv.ReceivedChannel()
	}
	require.NoError(t, ho.Send(t.Context(), h.valid(2, 1), server.ID()))
	select {
	case m := <-srv.ReceivedChannel():
		require.IsType(t, &certification.CertificationResponse{}, m)
	case <-time.After(3 * time.Second):
		t.Fatal("the honest root's response was not delivered while another root floods")
	}
}
