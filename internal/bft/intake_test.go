package bft

import (
	"context"
	"crypto"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cryptolibp2p "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-core/network"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	cryptobft "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"

	mnoop "go.opentelemetry.io/otel/metric/noop"
	tnoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/trustbase"
)

const (
	testPartition = types.PartitionID(1)
	fakeRoundTime = uint64(1_900_000_000)
)

// ---------------------------------------------------------------- fixtures --

// testChain is a root trust base history: every epoch has its own (new) root nodes and is signed by
// the validators of the previous epoch, as the trust base validator requires.
type testChain struct {
	tbs     map[uint64]*types.RootTrustBaseV1
	signers map[uint64]map[string]cryptobft.Signer
	ids     map[uint64][]peer.ID
}

func newTestChain(t *testing.T, epochs int, nodesPerEpoch int) *testChain {
	t.Helper()
	c := &testChain{
		tbs:     map[uint64]*types.RootTrustBaseV1{},
		signers: map[uint64]map[string]cryptobft.Signer{},
		ids:     map[uint64][]peer.ID{},
	}
	for e := uint64(1); e <= uint64(epochs); e++ {
		nodes := make([]*types.NodeInfo, 0, nodesPerEpoch)
		signers := map[string]cryptobft.Signer{}
		for range nodesPerEpoch {
			signer, err := cryptobft.NewInMemorySecp256K1Signer()
			require.NoError(t, err)
			verifier, err := signer.Verifier()
			require.NoError(t, err)
			pub, err := verifier.MarshalPublicKey()
			require.NoError(t, err)
			libKey, err := cryptolibp2p.UnmarshalSecp256k1PublicKey(pub)
			require.NoError(t, err)
			id, err := peer.IDFromPublicKey(libKey)
			require.NoError(t, err)
			nodes = append(nodes, &types.NodeInfo{NodeID: id.String(), SigKey: pub, Stake: 1})
			signers[id.String()] = signer
			c.ids[e] = append(c.ids[e], id)
		}
		opts := []types.Option{types.WithEpoch(e)}
		if e > 1 {
			prevHash, err := c.tbs[e-1].Hash(crypto.SHA256)
			require.NoError(t, err)
			opts = append(opts, types.WithEpochStart(1000*e), types.WithPreviousTrustBaseHash(prevHash))
		}
		tb, err := types.NewTrustBase(types.NetworkLocal, nodes, opts...)
		require.NoError(t, err)
		signWith := signers
		if e > 1 {
			signWith = c.signers[e-1]
		}
		for id, s := range signWith {
			require.NoError(t, tb.Sign(id, s))
		}
		c.tbs[e], c.signers[e] = tb, signers
	}
	return c
}

// memStore is an in-memory trust base store that counts reads.
type memStore struct {
	mu       sync.Mutex
	tbs      map[uint64]*types.RootTrustBaseV1
	getReads atomic.Int64
	headRead atomic.Int64
}

func newMemStore(c *testChain, upTo uint64) *memStore {
	m := &memStore{tbs: map[uint64]*types.RootTrustBaseV1{}}
	for e := uint64(1); e <= upTo; e++ {
		m.tbs[e] = c.tbs[e]
	}
	return m
}

func (m *memStore) GetByEpoch(_ context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	m.getReads.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if tb, ok := m.tbs[epoch]; ok {
		return tb, nil
	}
	return nil, interfaces.ErrTrustBaseNotFound
}

func (m *memStore) GetLatest(context.Context) (*types.RootTrustBaseV1, error) {
	m.headRead.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	var best *types.RootTrustBaseV1
	for _, tb := range m.tbs {
		if best == nil || tb.GetEpoch() > best.GetEpoch() {
			best = tb
		}
	}
	if best == nil {
		return nil, interfaces.ErrTrustBaseNotFound
	}
	return best, nil
}

func (m *memStore) Store(_ context.Context, tb types.RootTrustBase) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tbs[tb.GetEpoch()]; ok {
		return interfaces.ErrTrustBaseAlreadyExists
	}
	m.tbs[tb.GetEpoch()] = tb.(*types.RootTrustBaseV1)
	return nil
}

// fakeREST serves the epochs of the chain up to `available` and counts calls.
type fakeREST struct {
	chain     *testChain
	available atomic.Uint64
	calls     atomic.Int64
}

func (f *fakeREST) GetTrustBases(_ context.Context, from, to uint64) ([]*types.RootTrustBaseV1, error) {
	f.calls.Add(1)
	if from != to {
		return nil, fmt.Errorf("unexpected range %d-%d", from, to)
	}
	if from > f.available.Load() {
		return nil, nil
	}
	return []*types.RootTrustBaseV1{f.chain.tbs[from]}, nil
}

type harness struct {
	t      *testing.T
	chain  *testChain
	store  *memStore
	rest   *fakeREST
	client *BFTClientImpl
	rm     *stubRoundManager
	clock  *fakeClock
	// handshakes counts catch-up triggered handshakes
	handshakes atomic.Int64
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// newHarness builds a client whose admitted history is 1..admitted and whose BFT node REST serves
// 1..available.
func newHarness(t *testing.T, epochs, admitted, available int) *harness {
	t.Helper()
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	chain := newTestChain(t, epochs, 3)
	store := newMemStore(chain, uint64(admitted))
	rest := &fakeREST{chain: chain}
	rest.available.Store(uint64(available))
	manager := trustbase.NewTrustBaseManager(log, store, rest, trustbase.NewTrustBaseValidator(store))
	rm := &stubRoundManager{}
	c := &BFTClientImpl{
		logger:         log,
		partitionID:    testPartition,
		shardID:        types.ShardID{},
		roundManager:   rm,
		trustBaseStore: manager,
	}
	c.status.Store(normal)
	h := &harness{t: t, chain: chain, store: store, rest: rest, client: c, rm: rm, clock: &fakeClock{t: time.Unix(1_900_000_000, 0)}}
	c.ensureIntake()
	c.catchup.now = h.clock.now
	c.catchup.onCaughtUp = func(context.Context) { h.handshakes.Add(1) }
	require.NoError(t, c.members.Refresh(t.Context()))
	head, err := manager.Head(t.Context())
	require.NoError(t, err)
	c.admittedHead.Store(head)
	return h
}

// response builds a certification response for UC number n (partition round n, root round n+10) whose
// seal is of `epoch` and signed with `signWith` (nil = junk signatures).
func (h *harness) response(n uint64, epoch uint64, signWith map[string]cryptobft.Signer) *certification.CertificationResponse {
	h.t.Helper()
	hash := func(i uint64) []byte {
		b := make([]byte, 32)
		b[0], b[31] = byte(i), 0x5a
		return b
	}
	ir := &types.InputRecord{
		Version:      1,
		RoundNumber:  n,
		Epoch:        epoch,
		PreviousHash: hash(n - 1),
		Hash:         hash(n),
		BlockHash:    hash(n + 100),
		SummaryValue: []byte{},
		Timestamp:    fakeRoundTime + n,
	}
	tr := certification.TechnicalRecord{Round: n + 1, Epoch: epoch, Leader: "leader", StatHash: []byte{1}, FeeHash: []byte{2}}
	trHash, err := tr.Hash()
	require.NoError(h.t, err)
	tree, err := types.CreateShardTree(types.ShardingScheme{}, []types.ShardTreeInput{{Shard: types.ShardID{}, IR: ir, TRHash: trHash}}, crypto.SHA256)
	require.NoError(h.t, err)
	stCert, err := tree.Certificate(types.ShardID{})
	require.NoError(h.t, err)
	ut, err := types.NewUnicityTree(crypto.SHA256, []*types.UnicityTreeData{{Partition: testPartition, ShardTreeRoot: tree.RootHash()}})
	require.NoError(h.t, err)
	utCert, err := ut.Certificate(testPartition)
	require.NoError(h.t, err)
	seal := &types.UnicitySeal{
		Version:              1,
		NetworkID:            types.NetworkLocal,
		RootChainRoundNumber: epoch*1000 + n + 10,
		Epoch:                epoch,
		Timestamp:            fakeRoundTime + n,
		PreviousHash:         hash(n + 200),
		Hash:                 ut.RootHash(),
	}
	if signWith == nil {
		seal.Signatures = types.SignatureMap{"junk": []byte{1, 2, 3}}
	} else {
		for id, s := range signWith {
			require.NoError(h.t, seal.Sign(id, s))
		}
	}
	return &certification.CertificationResponse{
		Partition: testPartition,
		Shard:     types.ShardID{},
		Technical: tr,
		UC: types.UnicityCertificate{
			Version: 1, InputRecord: ir, TRHash: trHash, ShardTreeCertificate: stCert,
			UnicityTreeCertificate: utCert, UnicitySeal: seal,
		},
	}
}

func (h *harness) valid(n, epoch uint64) *certification.CertificationResponse {
	return h.response(n, epoch, h.chain.signers[epoch])
}

// ------------------------------------------------------------------- tests --

// Steady state with a known epoch adds no network exchange: no REST call, no head read, no catch-up
// attempt, no handshake, and no store read beyond one trust base lookup per message.
func TestIntake_SteadyStateAddsNoNetworkExchange(t *testing.T) {
	h := newHarness(t, 3, 3, 3)
	// the first certificate of an epoch makes it the live epoch (membership republished); that is an
	// event, not steady state
	require.NoError(t, h.client.handleCertificationResponse(t.Context(), h.valid(1, 2)))
	reads0 := h.store.getReads.Load()
	heads0 := h.store.headRead.Load()
	const n = 50
	for i := uint64(2); i <= n+1; i++ {
		require.NoError(t, h.client.handleCertificationResponse(t.Context(), h.valid(i, 2)))
	}
	require.EqualValues(t, 0, h.rest.calls.Load(), "no REST call")
	require.EqualValues(t, 0, h.store.headRead.Load()-heads0, "no head read")
	require.EqualValues(t, 0, h.client.catchup.attempts.Load(), "no catch-up attempt")
	require.EqualValues(t, 0, h.handshakes.Load(), "no handshake")
	require.LessOrEqual(t, h.store.getReads.Load()-reads0, int64(n), "at most one local trust base lookup per message")
	require.Len(t, h.rm.startedRounds, n+1)
	require.EqualValues(t, n+1, h.client.receivedResponses.Load())
}

// Forged epochs cost no store read and no REST call per message; the single worker's work is bounded
// by its cooldown whatever number of forged messages arrive.
func TestIntake_ForgedEpochsHaveBoundedAggregateWork(t *testing.T) {
	h := newHarness(t, 3, 3, 3)
	reads0 := h.store.getReads.Load()
	for i := 0; i < 2000; i++ {
		epoch := []uint64{4, 5, 19, 1 << 40}[i%4]
		err := h.client.handleCertificationResponse(t.Context(), h.response(7, epoch, nil))
		require.ErrorIs(t, err, ErrEpochNotAdmitted)
	}
	require.Zero(t, h.store.getReads.Load()-reads0, "no store read for epochs beyond the admitted head")
	require.Zero(t, h.rest.calls.Load(), "the message path never calls the BFT node")
	// the worker coalesces all signals into at most one pending run, and one failed attempt starts
	// the cooldown for that frontier
	for i := 0; i < 3; i++ { // 750 ms of "worker wake-ups", all inside the one second cooldown
		h.clock.advance(minRunInterval)
		h.client.catchup.runOnce(t.Context())
	}
	require.EqualValues(t, 1, h.rest.calls.Load(), "one failed-frontier attempt inside the cooldown")
	for i := 0; i < 4*60; i++ { // a minute of continuous wake-ups: at most one retry per second
		h.clock.advance(minRunInterval)
		h.client.catchup.runOnce(t.Context())
	}
	require.LessOrEqual(t, h.rest.calls.Load(), int64(62), "REST budget: one failed-frontier retry per second")
	require.Greater(t, h.rest.calls.Load(), int64(30), "recovery is not frozen")
	require.Zero(t, h.client.catchup.admissions.Load())
}

// Nothing but an authenticated, sequence-checked UC with an advancing root round refreshes the
// recovery timer.
func TestIntake_OnlyAdvancingAuthenticatedUCsRefreshTheRecoveryTimer(t *testing.T) {
	h := newHarness(t, 3, 3, 3)
	c := h.client
	old := time.Now().Add(-time.Hour).UnixMilli()
	reset := func() { c.lastCertResponseTime.Store(old) }
	untouched := func(what string) {
		require.Equal(t, old, c.lastCertResponseTime.Load(), what)
	}

	reset()
	bad := h.valid(1, 2)
	bad.Partition = 0
	require.Error(t, c.handleCertificationResponse(t.Context(), bad))
	untouched("malformed response")

	wrong := h.valid(1, 2)
	wrong.Shard, _ = types.ShardID{}.Split()
	require.Error(t, c.handleCertificationResponse(t.Context(), wrong))
	untouched("wrong shard")

	require.Error(t, c.handleCertificationResponse(t.Context(), h.response(1, 2, nil)))
	untouched("bad signature")

	require.ErrorIs(t, c.handleCertificationResponse(t.Context(), h.valid(1, 9)), ErrEpochNotAdmitted)
	untouched("unknown epoch")

	require.NoError(t, c.handleCertificationResponse(t.Context(), h.valid(5, 2)))
	require.Greater(t, c.lastCertResponseTime.Load(), old, "an authenticated, advancing UC refreshes")

	reset()
	require.Error(t, c.handleCertificationResponse(t.Context(), h.valid(3, 2)), "an older valid UC is a sequence error")
	untouched("stale replay")
	require.NoError(t, c.handleCertificationResponse(t.Context(), h.valid(5, 2)), "duplicate of the latest is ignored")
	untouched("duplicate replay")

	require.NoError(t, c.handleCertificationResponse(t.Context(), h.valid(6, 2)))
	require.Greater(t, c.lastCertResponseTime.Load(), old)
	require.EqualValues(t, 8, c.receivedResponses.Load(), "raw receptions are counted separately from liveness")
}

// "Not admitted yet" is not "invalid": after a verified catch-up the same otherwise valid certificate
// is accepted, also when it was claimed many epochs ahead, and the latest UC is requested again.
func TestIntake_ValidCertificateIsAcceptedAfterVerifiedCatchUp(t *testing.T) {
	h := newHarness(t, 6, 1, 6)
	c := h.client
	resp := h.valid(4, 6)
	require.ErrorIs(t, c.handleCertificationResponse(t.Context(), resp), ErrEpochNotAdmitted)
	require.Zero(t, h.rest.calls.Load())
	require.Empty(t, h.rm.startedRounds, "nothing accepted")

	c.catchup.runOnce(t.Context())
	require.EqualValues(t, 5, h.client.catchup.admissions.Load(), "walked forward epoch by epoch")
	require.EqualValues(t, 1, h.handshakes.Load(), "the latest UC is re-requested through the handshake path")
	require.NoError(t, c.handleCertificationResponse(t.Context(), resp))
	require.Len(t, h.rm.startedRounds, 1)
}

// A complete replacement of the root nodes: the new roots are outsiders until the history is
// extended; the extension happens without any admitted traffic (startup / inactivity trigger).
func TestIntake_FullMembershipReplacement(t *testing.T) {
	h := newHarness(t, 2, 1, 2)
	c := h.client
	oldRoot, newRoot := h.chain.ids[1][0], h.chain.ids[2][0]
	require.True(t, c.members.Contains(oldRoot))
	require.False(t, c.members.Contains(newRoot), "the new roots are not known yet")
	require.False(t, c.members.Admit(newRoot))

	c.catchup.Signal() // what Start and the inactivity timer do
	ctx, cancel := context.WithCancel(t.Context())
	go c.catchup.Run(ctx)
	require.Eventually(t, func() bool { return c.members.Contains(newRoot) }, 3*time.Second, 5*time.Millisecond)
	cancel()
	require.True(t, c.members.Admit(newRoot))
	require.EqualValues(t, 1, h.handshakes.Load())

	// the new epoch becomes live: the old roots (epoch 1, adjacent to 2) overlap and then retire
	require.NoError(t, c.handleCertificationResponse(t.Context(), h.valid(1, 2)))
	require.True(t, c.members.Contains(oldRoot), "transition overlap")
	require.Equal(t, uint64(2), c.members.live.Load())
}

// Retiring older memberships: with live epoch E at the head only E-1, E, E+1 are admitted.
func TestMembership_BoundedUnionAndRetirement(t *testing.T) {
	chain := newTestChain(t, 5, 2)
	store := newMemStore(chain, 5)
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	manager := trustbase.NewTrustBaseManager(log, store, &fakeREST{chain: chain}, trustbase.NewTrustBaseValidator(store))

	m := newMembership(manager, 5)
	require.NoError(t, m.Refresh(t.Context()))
	for e := uint64(1); e <= 5; e++ {
		for _, id := range chain.ids[e] {
			require.Equal(t, e >= 4, m.Contains(id), "epoch %d", e)
		}
	}
	// catching up (E behind H): H and H-1 are admitted too
	m = newMembership(manager, 2)
	require.NoError(t, m.Refresh(t.Context()))
	for e := uint64(1); e <= 5; e++ {
		for _, id := range chain.ids[e] {
			require.Equal(t, e <= 3 || e >= 4, m.Contains(id), "epoch %d", e)
		}
	}
	// advancing the live epoch retires the oldest memberships
	require.NoError(t, m.Advance(t.Context(), 5))
	require.False(t, m.Contains(chain.ids[2][0]))
	require.True(t, m.Contains(chain.ids[4][0]))
	require.False(t, m.Contains(peer.ID("someone else")))
}

// An admitted but flooding root is limited per peer and does not take capacity from the others.
func TestMembership_FloodingPeerDoesNotStarveAnHonestPeer(t *testing.T) {
	chain := newTestChain(t, 1, 3)
	store := newMemStore(chain, 1)
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	manager := trustbase.NewTrustBaseManager(log, store, &fakeREST{chain: chain}, trustbase.NewTrustBaseValidator(store))
	m := newMembership(manager, 1)
	require.NoError(t, m.Refresh(t.Context()))
	flooder, honest := chain.ids[1][0], chain.ids[1][1]

	admittedFlood := 0
	for i := 0; i < 5000; i++ {
		if m.Admit(flooder) {
			admittedFlood++
		}
		if i%250 == 0 {
			require.True(t, m.Admit(honest), "the honest root keeps being admitted")
		}
	}
	require.LessOrEqual(t, admittedFlood, int(peerBurst)+int(peerRate)*2, "flood bounded by the per-peer budget")
	require.Greater(t, admittedFlood, 0)
}

// More than one bounded run is needed beyond 16 epochs; each run admits at most 16.
func TestCatchUp_BeyondSixteenEpochs(t *testing.T) {
	const total = 40
	h := newHarness(t, total, 1, total)
	c := h.client
	runs := 0
	for c.admittedHead.Load() < total {
		before := c.catchup.admissions.Load()
		h.clock.advance(2 * minRunInterval)
		c.catchup.runOnce(t.Context())
		got := c.catchup.admissions.Load() - before
		require.LessOrEqual(t, got, uint64(maxAdmissionsPerRun))
		require.NotZero(t, got)
		runs++
		require.Less(t, runs, 10)
	}
	require.Equal(t, 3, runs)
	require.EqualValues(t, total-1, c.catchup.admissions.Load())
	require.EqualValues(t, 3, h.handshakes.Load())
	select {
	case <-c.catchup.wake:
	default:
		t.Fatal("a further bounded run must have been scheduled after the 16 admission bound")
	}
}

// An admission by another HA node changes the frontier, so a pending cooldown does not apply, and the
// message path sees the new epoch once the worker has noticed the moved head.
func TestCatchUp_HAAdmissionInvalidatesTheCooldown(t *testing.T) {
	h := newHarness(t, 3, 1, 1)
	c := h.client
	c.catchup.runOnce(t.Context())
	require.EqualValues(t, 1, h.rest.calls.Load(), "failed attempt for epoch 2")
	h.clock.advance(minRunInterval)
	c.catchup.runOnce(t.Context())
	require.EqualValues(t, 1, h.rest.calls.Load(), "still in the cooldown")
	h.clock.advance(minRunInterval)

	// another node admits epoch 2 into the shared store
	require.NoError(t, h.store.Store(t.Context(), h.chain.tbs[2]))
	h.rest.available.Store(2)
	c.catchup.runOnce(t.Context()) // inside the old cooldown time, but the frontier moved
	require.EqualValues(t, 2, c.admittedHead.Load(), "the moved head is noticed")
	require.EqualValues(t, 2, h.rest.calls.Load(), "one attempt for the new frontier (epoch 3), which fails")
	require.EqualValues(t, 1, h.handshakes.Load(), "the latest UC is requested after the head moved")
	require.NoError(t, h.client.handleCertificationResponse(t.Context(), h.valid(1, 2)))
}

// The admission callback is applied by the real libp2p receive handler, before anything is enqueued,
// for the certification response protocol.
func TestIntake_OutsiderIsRejectedBeforeEnqueue(t *testing.T) {
	h := newHarness(t, 1, 1, 1)
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)

	newPeer := func() *network.Peer {
		priv, pubKey, err := cryptolibp2p.GenerateSecp256k1Key(rand.Reader)
		require.NoError(t, err)
		privBytes, err := priv.Raw()
		require.NoError(t, err)
		pub, err := pubKey.Raw()
		require.NoError(t, err)
		priv2 := privBytes
		conf, err := network.NewPeerConfiguration("/ip4/127.0.0.1/tcp/0", nil, &network.PeerKeyPair{PublicKey: pub, PrivateKey: priv2}, nil, nil)
		require.NoError(t, err)
		p, err := network.NewPeer(t.Context(), conf, log.Logger, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = p.Close() })
		return p
	}
	server := newPeer()
	honest, outsider := newPeer(), newPeer()
	// admit the honest sender only
	members := newMembership(nil, 1)
	set := peerSet{honest.ID(): {}}
	members.set.Store(&set)
	opts := DefaultNetworkOptions
	opts.AdmitPeer = members.Admit
	srvNet, err := NewLibP2PNetwork(t.Context(), server, log, opts)
	require.NoError(t, err)

	send := func(p *network.Peer) {
		nw, err := network.NewLibP2PNetwork(p, 1, &Observability{mp: mnoop.NewMeterProvider(), tp: tnoop.NewTracerProvider(), logger: log.Logger})
		require.NoError(t, err)
		require.NoError(t, nw.RegisterSendProtocols([]network.SendProtocolDescription{{
			ProtocolID: network.ProtocolUnicityCertificates, Timeout: time.Second, MsgType: certification.CertificationResponse{},
		}}))
		p.Network().Peerstore().AddAddrs(server.ID(), server.MultiAddresses(), peerstore.PermanentAddrTTL)
		_ = nw.Send(t.Context(), h.valid(1, 1), server.ID())
	}
	send(outsider)
	select {
	case <-srvNet.ReceivedChannel():
		t.Fatal("a response from a peer that is not admitted must not be enqueued")
	case <-time.After(500 * time.Millisecond):
	}
	send(honest)
	select {
	case m := <-srvNet.ReceivedChannel():
		require.IsType(t, &certification.CertificationResponse{}, m)
	case <-time.After(3 * time.Second):
		t.Fatal("the admitted root's response was not delivered")
	}
}
