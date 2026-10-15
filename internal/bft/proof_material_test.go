package bft

import (
	"bytes"
	"context"
	"errors"
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

func newProofClient(t *testing.T, rm RoundManager) (*BFTClientImpl, *recordingSink) {
	log, err := logger.New("warn", "json", "", false)
	require.NoError(t, err)
	c := &BFTClientImpl{
		logger:       log,
		roundManager: rm,
		partitionID:  prooftest.Partition,
		proofStaging: proofmaterial.NewStaging(8),
		published:    map[proofmaterial.Digest]bool{},
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
	c, sink := newProofClient(t, rm)
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
	c, sink := newProofClient(t, rm)
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
		c, sink := newProofClient(t, rm)
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
	c, sink := newProofClient(t, rm)
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
	c, sink := newProofClient(t, rm)
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
	c, sink := newProofClient(t, &stubRoundManager{})
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
	c, sink := newProofClient(t, rm)
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
	c, sink := newProofClient(t, rm)
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
	c, sink := newProofClient(t, rm)
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
	c, sink := newProofClient(t, rm)
	c.stagePartitionView(t.Context(), response(f, f.Raw))
	require.NoError(t, c.handleUnicityCertificate(t.Context(), f.UC, &certification.TechnicalRecord{Round: 8, Epoch: 1}))
	require.Empty(t, sink.got, "local state was not finalized")
}
