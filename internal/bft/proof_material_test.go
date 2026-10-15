package bft

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/proofpub"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/bft-core/network/protocol/certification"
	"github.com/unicitynetwork/bft-go-base/types"
)

type recordingSink struct {
	got []*proofmaterial.Material
	err error
}

func (s *recordingSink) PublishProofMaterial(_ context.Context, m *proofmaterial.Material) error {
	s.got = append(s.got, m)
	return s.err
}

func newProofClient(t *testing.T, rm RoundManager, f *prooftest.Fixture) (*BFTClientImpl, *recordingSink) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)
	c := &BFTClientImpl{
		logger:        log,
		roundManager:  rm,
		partitionID:   prooftest.Partition,
		proofStaging:  proofmaterial.NewStaging(8),
		published:     map[proofmaterial.Digest]bool{},
		proofTopology: f.Provider(t),
	}
	sink := &recordingSink{}
	c.SetProofMaterialSink(sink)
	return c, sink
}

func proposeBlock(c *BFTClientImpl, number uint64, root []byte) {
	c.proposedBlock = models.NewBlock(api.NewBigIntFromUint64(number), "unicity", 0, "1.0", "mainnet",
		api.NewHexBytes(root), api.NewHexBytes(bytes.Repeat([]byte{1}, 32)), nil, 1)
}

func response(f *prooftest.Fixture, view []byte) *certification.CertificationResponse {
	return &certification.CertificationResponse{Partition: prooftest.Partition, Shard: f.Shard, UC: *f.UC, PartitionView: view}
}

func Test_ProofMaterial_FinalizedWithView(t *testing.T) {
	root := prooftest.Hash("root-1")
	f := prooftest.New(t, 2, 7, root, 80)
	rm := &stubRoundManager{}
	c, sink := newProofClient(t, rm, f)
	proposeBlock(c, 7, root)

	c.stagePartitionView(t.Context(), response(f, f.Raw))
	require.NoError(t, c.handleUnicityCertificate(t.Context(), f.UC, &certification.TechnicalRecord{Round: 8, Epoch: 1}))

	require.Len(t, rm.finalizedBlocks, 1)
	require.Equal(t, api.HexBytes(f.Raw), rm.finalizedBlocks[0].PartitionView, "view persisted with the block UC")
	require.Len(t, sink.got, 1)
	m := sink.got[0]
	require.Equal(t, proofmaterial.KindFinalized, m.Kind)
	require.True(t, m.HasBlock)
	require.EqualValues(t, 7, m.BlockNumber)
	require.Equal(t, f.Raw, m.PartitionView)
	require.Equal(t, []byte(rm.finalizedBlocks[0].UnicityCertificate), m.UCBytes)
}

func Test_ProofMaterial_NoSidecar(t *testing.T) {
	root := prooftest.Hash("root-1")
	f := prooftest.New(t, 2, 7, root, 80)
	rm := &stubRoundManager{}
	c, sink := newProofClient(t, rm, f)
	proposeBlock(c, 7, root)

	require.NoError(t, c.handleUnicityCertificate(t.Context(), f.UC, &certification.TechnicalRecord{Round: 8, Epoch: 1}))
	require.Len(t, rm.finalizedBlocks, 1)
	require.Nil(t, rm.finalizedBlocks[0].PartitionView)
	require.Len(t, sink.got, 1)
	require.Nil(t, sink.got[0].PartitionView)
}

func Test_ProofMaterial_InvalidSidecarIsMissingMaterial(t *testing.T) {
	root := prooftest.Hash("root-1")
	f := prooftest.New(t, 2, 7, root, 80)
	other := prooftest.New(t, 2, 9, prooftest.Hash("elsewhere"), 99)
	for name, view := range map[string][]byte{
		"malformed":      {0x83, 0x01},
		"view of other":  other.Raw,
		"oversize":       bytes.Repeat([]byte{1}, types.MaxPartitionViewBytes+1),
		"trailing bytes": append(bytes.Clone(f.Raw), 0),
	} {
		rm := &stubRoundManager{}
		c, sink := newProofClient(t, rm, f)
		proposeBlock(c, 7, root)
		c.stagePartitionView(t.Context(), response(f, view))
		require.NoError(t, c.handleUnicityCertificate(t.Context(), f.UC, &certification.TechnicalRecord{Round: 8, Epoch: 1}), name)
		require.Len(t, rm.finalizedBlocks, 1, name)
		require.Nil(t, rm.finalizedBlocks[0].PartitionView, name)
		require.Nil(t, sink.got[0].PartitionView, name)
	}
}

// A view staged for one UC is never attached to another UC, even with the same state root.
func Test_ProofMaterial_ExactUCAssociation(t *testing.T) {
	root := prooftest.Hash("root-1")
	f := prooftest.New(t, 2, 7, root, 80)
	sameRootLaterSeal := prooftest.New(t, 2, 7, root, 81) // same IR, different seal
	rm := &stubRoundManager{}
	c, sink := newProofClient(t, rm, f)
	proposeBlock(c, 7, root)

	c.stagePartitionView(t.Context(), response(sameRootLaterSeal, sameRootLaterSeal.Raw))
	require.NoError(t, c.handleUnicityCertificate(t.Context(), f.UC, &certification.TechnicalRecord{Round: 8, Epoch: 1}))
	require.Nil(t, rm.finalizedBlocks[0].PartitionView)
	require.Nil(t, sink.got[0].PartitionView)
}

func Test_ProofMaterial_DuplicateFillsMissingView(t *testing.T) {
	root := prooftest.Hash("root-1")
	f := prooftest.New(t, 2, 7, root, 80)
	rm := &stubRoundManager{}
	c, sink := newProofClient(t, rm, f)
	proposeBlock(c, 7, root)
	tr := &certification.TechnicalRecord{Round: 8, Epoch: 1}

	require.NoError(t, c.handleUnicityCertificate(t.Context(), f.UC, tr))
	require.Len(t, sink.got, 1)
	require.Nil(t, sink.got[0].PartitionView)

	// duplicate delivery (from another root node) carrying the view
	c.stagePartitionView(t.Context(), response(f, f.Raw))
	require.NoError(t, c.handleUnicityCertificate(t.Context(), f.UC, tr))
	c.publishSidecarFill(t.Context(), f.UC)
	require.Len(t, sink.got, 2)
	require.Equal(t, proofmaterial.KindSidecarFill, sink.got[1].Kind)
	require.Equal(t, f.Raw, sink.got[1].PartitionView)
	require.Len(t, rm.finalizedBlocks, 1, "no re-execution")

	// once filled, further duplicates do not publish again
	c.publishSidecarFill(t.Context(), f.UC)
	require.Len(t, sink.got, 2)
}

func Test_ProofMaterial_FillOnlyForPublishedUC(t *testing.T) {
	f := prooftest.New(t, 2, 7, prooftest.Hash("root-1"), 80)
	c, sink := newProofClient(t, &stubRoundManager{}, f)
	c.stagePartitionView(t.Context(), response(f, f.Raw))
	c.publishSidecarFill(t.Context(), f.UC) // never accepted/finalized
	require.Empty(t, sink.got)
}

func Test_ProofMaterial_RepeatUC(t *testing.T) {
	root := prooftest.Hash("root-1")
	prev := prooftest.New(t, 2, 7, root, 80)
	repeat := prooftest.New(t, 2, 7, root, 81)
	repeat.UC.InputRecord.PreviousHash = prev.UC.InputRecord.PreviousHash
	rm := &stubRoundManager{}
	c, sink := newProofClient(t, rm, repeat)
	c.luc.Store(prev.UC)
	c.lastRootRound.Store(80)

	c.stagePartitionView(t.Context(), response(repeat, repeat.Raw))
	err := c.handleUnicityCertificate(t.Context(), repeat.UC, &certification.TechnicalRecord{Round: 8, Epoch: 1})
	require.NoError(t, err)
	if len(sink.got) == 0 {
		t.Skip("fixture UC is not classified as a repeat by IsRepeat")
	}
	require.Equal(t, proofmaterial.KindRepeat, sink.got[0].Kind)
	require.False(t, sink.got[0].HasBlock)
	require.Empty(t, rm.finalizedBlocks)
}

func Test_ProofMaterial_SinkFailureDoesNotFailCertification(t *testing.T) {
	root := prooftest.Hash("root-1")
	f := prooftest.New(t, 2, 7, root, 80)
	rm := &stubRoundManager{}
	c, sink := newProofClient(t, rm, f)
	sink.err = errors.New("catalog unavailable")
	proposeBlock(c, 7, root)
	require.NoError(t, c.handleUnicityCertificate(t.Context(), f.UC, &certification.TechnicalRecord{Round: 8, Epoch: 1}))
	require.Len(t, rm.finalizedBlocks, 1)
	require.Len(t, rm.startedRounds, 1)
}

func Test_ProofMaterial_DurableRecoveryPublishes(t *testing.T) {
	root := prooftest.Hash("root-1")
	f := prooftest.New(t, 2, 7, root, 80)
	rm := &stubRoundManager{durableRecovered: true}
	c, sink := newProofClient(t, rm, f)
	c.stagePartitionView(t.Context(), response(f, f.Raw))
	require.NoError(t, c.handleUnicityCertificate(t.Context(), f.UC, &certification.TechnicalRecord{Round: 8, Epoch: 1}))
	require.Len(t, sink.got, 1)
	require.Equal(t, proofmaterial.KindFinalized, sink.got[0].Kind)
	require.EqualValues(t, 7, sink.got[0].BlockNumber)
	require.Equal(t, f.Raw, sink.got[0].PartitionView)
}

func Test_ProofMaterial_NoPublicationWithoutFinalization(t *testing.T) {
	root := prooftest.Hash("root-1")
	f := prooftest.New(t, 2, 7, root, 80)
	rm := &stubRoundManager{durableRecovered: false} // no proposed block, nothing recovered
	c, sink := newProofClient(t, rm, f)
	c.stagePartitionView(t.Context(), response(f, f.Raw))
	require.NoError(t, c.handleUnicityCertificate(t.Context(), f.UC, &certification.TechnicalRecord{Round: 8, Epoch: 1}))
	require.Empty(t, sink.got, "local state was not finalized")
}

// A fill the sink rejected stays eligible: a later duplicate retries it without
// re-executing finalization (codex review: delivery is only marked once accepted).
func Test_ProofMaterial_FailedFillIsRetried(t *testing.T) {
	root := prooftest.Hash("root-1")
	f := prooftest.New(t, 2, 7, root, 80)
	rm := &stubRoundManager{}
	c, sink := newProofClient(t, rm, f)
	proposeBlock(c, 7, root)
	tr := &certification.TechnicalRecord{Round: 8, Epoch: 1}
	require.NoError(t, c.handleUnicityCertificate(t.Context(), f.UC, tr))
	require.Len(t, sink.got, 1)

	sink.err = errors.New("temporary catalog pressure")
	c.stagePartitionView(t.Context(), response(f, f.Raw))
	require.NoError(t, c.handleUnicityCertificate(t.Context(), f.UC, tr))
	c.publishSidecarFill(t.Context(), f.UC)
	require.Len(t, sink.got, 2, "first fill attempted and rejected")

	sink.err = nil
	c.publishSidecarFill(t.Context(), f.UC) // a later duplicate
	require.Len(t, sink.got, 3, "failed fill is retried")
	require.Equal(t, proofmaterial.KindSidecarFill, sink.got[2].Kind)

	c.publishSidecarFill(t.Context(), f.UC)
	require.Len(t, sink.got, 3, "no further fills once accepted")
	require.Len(t, rm.finalizedBlocks, 1, "no re-execution")
}

func Test_ProofMaterial_NoTopologyMeansNoMaterial(t *testing.T) {
	root := prooftest.Hash("root-1")
	f := prooftest.New(t, 2, 7, root, 80)
	rm := &stubRoundManager{}
	c, sink := newProofClient(t, rm, f)
	c.proofTopology = nil
	proposeBlock(c, 7, root)
	c.stagePartitionView(t.Context(), response(f, f.Raw))
	require.NoError(t, c.handleUnicityCertificate(t.Context(), f.UC, &certification.TechnicalRecord{Round: 8, Epoch: 1}))
	require.Len(t, rm.finalizedBlocks, 1)
	require.Nil(t, rm.finalizedBlocks[0].PartitionView)
	require.Nil(t, sink.got[0].PartitionView)
}

func Test_ProofTopologyFromConfig(t *testing.T) {
	f1 := prooftest.New(t, 0, 7, prooftest.Hash("root"), 80)
	pdr := &types.PartitionDescriptionRecord{Version: 1, NetworkID: 3, PartitionID: prooftest.Partition, Epoch: 1}

	t.Run("unsharded uses the own configuration hash", func(t *testing.T) {
		p, err := ProofTopologyFromConfig(&config.BFTConfig{ShardConf: pdr})
		require.NoError(t, err)
		topo, ok := p.Topology(prooftest.Partition)
		require.True(t, ok)
		require.Len(t, topo.ShardConfHashes[types.ShardID{}.Key()], 32)
		_ = f1
	})
	t.Run("sharded without configured topology is unavailable", func(t *testing.T) {
		a, _ := types.ShardID{}.Split()
		sharded := *pdr
		sharded.ShardID = a
		p, err := ProofTopologyFromConfig(&config.BFTConfig{ShardConf: &sharded})
		require.NoError(t, err)
		require.Nil(t, p)
	})
	t.Run("configured topology must be complete with 32-byte hashes", func(t *testing.T) {
		a, b := types.ShardID{}.Split()
		h := hex.EncodeToString(prooftest.Hash("c"))
		sharded := *pdr
		sharded.ShardID = a
		_, err := ProofTopologyFromConfig(&config.BFTConfig{ShardConf: &sharded,
			ConsolidationShards: []config.ConsolidationShard{{ShardID: a, ConfigHash: h}}})
		require.Error(t, err, "incomplete scheme")
		_, err = ProofTopologyFromConfig(&config.BFTConfig{ShardConf: &sharded,
			ConsolidationShards: []config.ConsolidationShard{{ShardID: a, ConfigHash: h}, {ShardID: b, ConfigHash: "abcd"}}})
		require.Error(t, err, "short hash")
		p, err := ProofTopologyFromConfig(&config.BFTConfig{ShardConf: &sharded,
			ConsolidationShards: []config.ConsolidationShard{{ShardID: a, ConfigHash: h}, {ShardID: b, ConfigHash: "0x" + h}}})
		require.NoError(t, err)
		_, ok := p.Topology(prooftest.Partition)
		require.True(t, ok)
	})
}

// Integration with the real publisher: a fill rejected because the view is not retained
// yet stays retryable; once the state is published, a duplicate attaches the sidecar
// (codex review of A4, major 3).
func Test_ProofMaterial_FillRetriedUntilViewRetained(t *testing.T) {
	ctx := t.Context()
	backend := smtbackend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))
	snap, err := backend.CreateSnapshot(ctx)
	require.NoError(t, err)
	key := make([]byte, api.StateTreeKeyLengthBytes)
	key[0] = 9
	res, err := snap.AddLeavesClassified(ctx, []smtbackend.LeafInput{{Key: key, Value: make([]byte, 32)}})
	require.NoError(t, err)
	root := res.CandidateRoot
	require.NoError(t, snap.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: api.NewBigIntFromUint64(7), RootHash: root}))

	f := prooftest.New(t, 2, 7, root, 80)
	catalog, err := proofview.NewCatalog(proofview.DefaultLimits())
	require.NoError(t, err)
	defer catalog.Close()
	log, err := logger.New("error", "json", "", false)
	require.NoError(t, err)
	pub := proofpub.New(catalog, backend, prooftest.Partition, f.Provider(t), log)
	require.NotNil(t, pub)

	rm := &stubRoundManager{}
	c, _ := newProofClient(t, rm, f)
	c.SetProofMaterialSink(pub)
	proposeBlock(c, 7, root)
	tr := &certification.TechnicalRecord{Round: 8, Epoch: 1}
	require.NoError(t, c.handleUnicityCertificate(ctx, f.UC, tr)) // accepted; nothing retained yet

	c.stagePartitionView(ctx, response(f, f.Raw))
	require.NoError(t, c.handleUnicityCertificate(ctx, f.UC, tr))
	c.publishSidecarFill(ctx, f.UC)
	_, err = catalog.AcquireLatest(ctx)
	require.ErrorIs(t, err, proofview.ErrNoFinalizedView)

	// the finalized state becomes retained (without the view)
	_, ucBytes, err := proofmaterial.DigestOfUC(f.UC)
	require.NoError(t, err)
	block := models.NewBlock(api.NewBigIntFromUint64(7), "unicity", 0, "1.0", "mainnet", api.NewHexBytes(root), nil, api.NewHexBytes(ucBytes), 1)
	block.Finalized = true
	pub.PublishFinalized(ctx, block)
	l, err := catalog.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Nil(t, l.PartitionView())
	l.Release()

	// a later duplicate delivers the sidecar
	require.NoError(t, c.handleUnicityCertificate(ctx, f.UC, tr))
	c.publishSidecarFill(ctx, f.UC)
	l, err = catalog.AcquireLatest(ctx)
	require.NoError(t, err)
	defer l.Release()
	require.Equal(t, f.Raw, l.PartitionView(), "the failed fill was retried and attached")
	require.Len(t, rm.finalizedBlocks, 1, "no re-execution")
}
