package service

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/nifixture"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial"
	"github.com/unicitynetwork/aggregator-go/internal/proofpub"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
	"github.com/unicitynetwork/aggregator-go/pkg/noninclusion"
)

func newBlock(n int, root, uc []byte) *models.Block {
	b := models.NewBlock(api.NewBigIntFromUint64(uint64(n)), "unicity", 0, "1.0", "mainnet",
		api.NewHexBytes(root), api.NewHexBytes(make([]byte, 32)), api.NewHexBytes(uc), 1)
	b.Finalized = true
	return b
}

type niShard struct{ err error }

func (s niShard) ValidateShardID(api.StateID) error { return s.err }

type niEnv struct {
	auth *nifixture.Authority
	be   *backend.MemoryBackend
	cat  *proofview.Catalog
	pub  *proofpub.Publisher
	svc  *NonInclusionService
	n    int
}

func newNIEnv(t *testing.T, cfg NonInclusionConfig) *niEnv {
	auth, err := nifixture.NewAuthority()
	require.NoError(t, err)
	be := backend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	cat, err := proofview.NewCatalog(proofview.DefaultLimits(), proofview.AccountantFor(be))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	log, err := logger.New("error", "json", "", false)
	require.NoError(t, err)
	pub := proofpub.New(cat, be, nifixture.Partition, nil, log)
	pub.SetSealVerifier(func(context.Context) types.SealVerifier { return types.TrustBaseSealVerifier(auth.TrustBase()) })
	return &niEnv{auth: auth, be: be, cat: cat, pub: pub, svc: NewNonInclusionService(NonInclusionCatalogSource(cat), niShard{}, cfg)}
}

func niKey(n int) []byte {
	k := make([]byte, 32)
	k[0], k[1], k[31] = byte(n*37), byte(n), byte(n>>8)
	return k
}

// commit adds the leaves, certifies the new root and publishes the block through the publisher.
func (e *niEnv) commit(t *testing.T, from, to int) []byte {
	ctx := context.Background()
	var in []backend.LeafInput
	for i := from; i < to; i++ {
		in = append(in, backend.LeafInput{Key: niKey(i), Value: append(make([]byte, 31), byte(i+1))})
	}
	snap, err := e.be.CreateSnapshot(ctx)
	require.NoError(t, err)
	res, err := snap.AddLeavesClassified(ctx, in)
	require.NoError(t, err)
	e.n++
	require.NoError(t, snap.Commit(ctx, backend.CommitMetadata{BlockNumber: api.NewBigIntFromUint64(uint64(e.n)), RootHash: res.CandidateRoot}))
	_, ucb, err := e.auth.CertifyRoot(res.CandidateRoot)
	require.NoError(t, err)
	b := newBlock(e.n, res.CandidateRoot, ucb)
	e.pub.PublishFinalized(ctx, b)
	return res.CandidateRoot
}

func (e *niEnv) call(t *testing.T, key []byte) (*api.GetNonInclusionProofResponseV1, *jsonrpc.Error) {
	params, _ := json.Marshal(map[string]string{"stateId": hex.EncodeToString(key)})
	res, rerr := e.svc.HandleRPC(context.Background(), params)
	if rerr != nil {
		return nil, rerr
	}
	// the result travels as one hex string and decodes back through the public codec
	j, err := json.Marshal(res)
	require.NoError(t, err)
	var back api.GetNonInclusionProofResponseV1
	require.NoError(t, json.Unmarshal(j, &back))
	return &back, nil
}

func (e *niEnv) verifier(t *testing.T, min uint64) *noninclusion.Verifier {
	tb := e.auth.TrustBase()
	v, err := noninclusion.NewVerifier(noninclusion.Context{
		Partition: nifixture.Partition, MinRootRound: min,
		Resolver: func(*types.UnicitySeal) (types.RootTrustBase, error) { return tb, nil },
	})
	require.NoError(t, err)
	return v
}

func errData(t *testing.T, e *jsonrpc.Error) jsonrpc.ErrorData {
	d, ok := e.Data.(jsonrpc.ErrorData)
	require.True(t, ok, "%#v", e.Data)
	return d
}

func TestNonInclusionServiceServesAVerifiableAbsence(t *testing.T) {
	e := newNIEnv(t, NonInclusionConfig{})
	e.commit(t, 0, 30)
	resp, rerr := e.call(t, niKey(500))
	require.Nil(t, rerr)
	require.EqualValues(t, 1, resp.BlockNumber)
	// the complete verifier authenticates the UC (anchored authority, partition) and the relation
	bound, err := resp.Proof.ForState(niKey(500))
	require.NoError(t, err)
	res, err := e.verifier(t, 0).VerifyFor(niKey(500), bound)
	require.NoError(t, err)
	require.False(t, res.EmptyTree)
	// the same proof does not prove another state, nor a stale-free policy
	_, err = e.verifier(t, 0).VerifyFor(niKey(501), resp.Proof)
	require.Equal(t, noninclusion.VerdictCertInvalid, noninclusion.VerdictOf(err))
	_, err = e.verifier(t, res.RootRound+1).VerifyFor(niKey(500), resp.Proof)
	require.Equal(t, noninclusion.VerdictStale, noninclusion.VerdictOf(err))
	// a present state is reported as included, with the shared code, not as a proof
	_, rerr = e.call(t, niKey(3))
	require.NotNil(t, rerr)
	require.Equal(t, api.NonInclusionCodeStateIncluded, rerr.Code)
	require.Equal(t, "state_included", errData(t, rerr).Detail)
}

// Pin root A, insert the query at B: absence at A stays valid and retained, the latest snapshot reports included.
func TestNonInclusionIsASnapshotStatement(t *testing.T) {
	e := newNIEnv(t, NonInclusionConfig{})
	rootA := e.commit(t, 0, 20)
	respA, rerr := e.call(t, niKey(900))
	require.Nil(t, rerr)
	e.commit(t, 900, 901)
	_, rerr = e.call(t, niKey(900))
	require.NotNil(t, rerr)
	require.Equal(t, api.NonInclusionCodeStateIncluded, rerr.Code)
	// the earlier response is a historical fact: still verifies against its own UC
	_, err := e.verifier(t, 0).VerifyFor(niKey(900), respA.Proof)
	require.NoError(t, err)
	// and the retained older snapshot still proves it
	l, err := e.cat.AcquireRoot(context.Background(), rootA)
	require.NoError(t, err)
	defer l.Release()
	cert, err := l.GetNonInclusionCert(niKey(900))
	require.NoError(t, err)
	require.NoError(t, cert.VerifyAgainstTrustedRoot(niKey(900), rootA, api.SHA256))
}

func TestNonInclusionBeforeAnySnapshotIsUnavailable(t *testing.T) {
	e := newNIEnv(t, NonInclusionConfig{})
	_, rerr := e.call(t, niKey(1))
	require.NotNil(t, rerr)
	require.Equal(t, api.NonInclusionCodeNotFound, rerr.Code)
	require.Equal(t, "no_finalized_view", errData(t, rerr).Detail)
}

func TestNonInclusionOfTheCertifiedEmptyTree(t *testing.T) {
	ctx := context.Background()
	e := newNIEnv(t, NonInclusionConfig{})
	uc, ucb, err := e.auth.CertifyRepeat()
	require.NoError(t, err)
	d, _, err := proofmaterial.DigestOfUC(uc)
	require.NoError(t, err)
	require.NoError(t, e.pub.PublishProofMaterial(ctx, &proofmaterial.Material{Kind: proofmaterial.KindInitial, UC: uc, UCBytes: ucb, UCDigest: d}))
	resp, rerr := e.call(t, niKey(7))
	require.Nil(t, rerr)
	require.True(t, resp.Proof.Certificate.IsEmptyTree())
	res, err := e.verifier(t, 0).VerifyFor(niKey(7), resp.Proof)
	require.NoError(t, err)
	require.True(t, res.EmptyTree)
	// the first real block supersedes it as latest
	e.commit(t, 0, 5)
	resp, rerr = e.call(t, niKey(700))
	require.Nil(t, rerr)
	require.False(t, resp.Proof.Certificate.IsEmptyTree())
}

// A UC certifying 32 zero bytes is an ordinary root: there is nothing to prove an absence against.
func TestNonInclusionAtAZeroRootIsUnavailable(t *testing.T) {
	ctx := context.Background()
	e := newNIEnv(t, NonInclusionConfig{})
	zero := make([]byte, 32)
	_, ucb, err := e.auth.CertifyRoot(zero)
	require.NoError(t, err)
	snap, err := e.be.CreateSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, snap.Commit(ctx, backend.CommitMetadata{BlockNumber: api.NewBigIntFromUint64(1), RootHash: zero}))
	e.pub.PublishFinalized(ctx, newBlock(1, zero, ucb))
	_, rerr := e.call(t, niKey(1))
	require.NotNil(t, rerr)
	require.Equal(t, api.NonInclusionCodeNotFound, rerr.Code)
	require.Equal(t, "empty_snapshot_not_provable", errData(t, rerr).Detail)
}

type niFakeLease struct {
	ctx      context.Context
	release  func()
	cert     *api.NonInclusionCert
	err      error
	root, uc []byte
	released bool
}

func (l *niFakeLease) Context() context.Context { return l.ctx }
func (l *niFakeLease) Root() []byte             { return l.root }
func (l *niFakeLease) EmptyTree() bool          { return false }
func (l *niFakeLease) Block() proofview.BlockIdentity {
	return proofview.BlockIdentity{Number: api.NewBigIntFromUint64(1)}
}
func (l *niFakeLease) UC() []byte { return l.uc }
func (l *niFakeLease) GetNonInclusionCert([]byte) (*api.NonInclusionCert, error) {
	return l.cert, l.err
}
func (l *niFakeLease) Release() {
	l.released = true
	if l.release != nil {
		l.release()
	}
}

type niFakeSource struct {
	lease *niFakeLease
	err   error
	block chan struct{}
}

func (s *niFakeSource) AcquireLatest(ctx context.Context) (NonInclusionLease, error) {
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.lease, nil
}

func TestNonInclusionErrorMappingAndLeaseRelease(t *testing.T) {
	root := make([]byte, 32)
	for name, tc := range map[string]struct {
		src     *niFakeSource
		shard   error
		code    int
		detail  string
		release bool
	}{
		"unsupported capability": {src: &niFakeSource{lease: &niFakeLease{ctx: context.Background(), err: backend.ErrNonInclusionUnsupported, root: root, uc: []byte{1}}},
			code: api.NonInclusionCodeNotFound, detail: "unsupported_configuration", release: true},
		"storage failure is not an absence": {src: &niFakeSource{lease: &niFakeLease{ctx: context.Background(), err: errors.New("disk on fire"), root: root, uc: []byte{1}}},
			code: jsonrpc.InternalErrorCode, release: true},
		"busy pins":    {src: &niFakeSource{err: proofview.ErrBusyPins}, code: api.NonInclusionCodeBusy, detail: "pinned_views"},
		"busy budget":  {src: &niFakeSource{err: proofview.ErrBusyBudget}, code: api.NonInclusionCodeBusy, detail: "retention_budget"},
		"not retained": {src: &niFakeSource{err: proofview.ErrViewNotRetained}, code: api.NonInclusionCodeNotFound, detail: "view_not_retained"},
		"wrong shard":  {src: &niFakeSource{}, shard: errors.New("elsewhere"), code: jsonrpc.InvalidParamsCode, detail: "wrong_shard"},
		"cert not valid for the root": {src: &niFakeSource{lease: &niFakeLease{ctx: context.Background(), cert: mustCert(t), root: root, uc: []byte{1}}},
			code: jsonrpc.InternalErrorCode, release: true},
	} {
		t.Run(name, func(t *testing.T) {
			svc := NewNonInclusionService(tc.src, niShard{tc.shard}, NonInclusionConfig{})
			params, _ := json.Marshal(map[string]string{"stateId": hex.EncodeToString(niKey(1))})
			_, rerr := svc.HandleRPC(context.Background(), params)
			require.NotNil(t, rerr)
			require.Equal(t, tc.code, rerr.Code)
			if tc.detail != "" {
				require.Equal(t, tc.detail, errData(t, rerr).Detail)
			}
			if rerr.Code == jsonrpc.InternalErrorCode {
				require.Nil(t, rerr.Data, "no internal text reaches the client")
			}
			if tc.release {
				require.True(t, tc.src.lease.released, "the lease is released on every exit")
			}
		})
	}
}

func mustCert(t *testing.T) *api.NonInclusionCert {
	var c api.NonInclusionCert
	b := make([]byte, 96)
	b[32] = 1
	require.NoError(t, c.UnmarshalBinary(b))
	return &c
}

func TestNonInclusionAdmissionAndDeadline(t *testing.T) {
	block := make(chan struct{})
	src := &niFakeSource{block: block, lease: &niFakeLease{ctx: context.Background(), root: make([]byte, 32), uc: []byte{1}, cert: mustCert(t)}}
	svc := NewNonInclusionService(src, niShard{}, NonInclusionConfig{MaxConcurrent: 1, MaxExecution: 150 * time.Millisecond})
	params, _ := json.Marshal(map[string]string{"stateId": hex.EncodeToString(niKey(1))})
	done := make(chan *jsonrpc.Error, 1)
	go func() { _, e := svc.HandleRPC(context.Background(), params); done <- e }()
	time.Sleep(30 * time.Millisecond)
	_, rerr := svc.HandleRPC(context.Background(), params) // the only slot is taken
	require.NotNil(t, rerr)
	require.Equal(t, api.NonInclusionCodeBusy, rerr.Code)
	require.Equal(t, "active_requests", errData(t, rerr).Detail)
	first := <-done // the blocked acquisition ends at its execution deadline
	require.NotNil(t, first)
	require.Equal(t, api.NonInclusionCodeBusy, first.Code)
	require.Equal(t, "deadline", errData(t, first).Detail)
	// the slot is free again
	close(block)
	_, rerr = svc.HandleRPC(context.Background(), params)
	require.NotNil(t, rerr) // the fake certificate does not verify, but it was not busy
	require.NotEqual(t, api.NonInclusionCodeBusy, rerr.Code)
}

func TestNonInclusionParamsAreStrict(t *testing.T) {
	e := newNIEnv(t, NonInclusionConfig{})
	for _, p := range []string{``, `null`, `{}`, `{"stateId":"ab"}`, `{"stateId":"` + hex.EncodeToString(niKey(1)) + `","stateRoot":"latest"}`, `["x"]`} {
		_, rerr := e.svc.HandleRPC(context.Background(), json.RawMessage(p))
		require.NotNil(t, rerr, p)
		require.Equal(t, jsonrpc.InvalidParamsCode, rerr.Code, p)
	}
}
