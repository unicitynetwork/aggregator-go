package round

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/suite"

	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/internal/storage/mongodb"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ViewRetentionSuite covers the retention of the partition view sidecar in the block documents:
// the new finalized pair is kept, the previous view is removed after it, and a cleanup problem
// never fails finalization.
type ViewRetentionSuite struct{ proofPubBase }

func TestViewRetentionSuite(t *testing.T) { suite.Run(t, new(ViewRetentionSuite)) }

type pruneFailBlockStorage struct {
	*mongodb.BlockStorage
	err error
}

func (s *pruneFailBlockStorage) PruneRecentPartitionViews(context.Context, *api.BigInt, int) (int, error) {
	return 0, s.err
}

func (s *pruneFailBlockStorage) PrunePartitionViews(context.Context, *api.BigInt, int) (int, error) {
	return 0, s.err
}

type pruneFailStorage struct {
	interfaces.Storage
	bs *pruneFailBlockStorage
}

func (s *pruneFailStorage) BlockStorage() interfaces.BlockStorage { return s.bs }

func (s *ViewRetentionSuite) stored(n int64) *models.Block {
	b, err := getBlockAnyFinalization(context.Background(), s.storage, api.NewBigInt(big.NewInt(n)))
	s.Require().NoError(err)
	s.Require().NotNil(b)
	return b
}

func (s *ViewRetentionSuite) Test1_FinalizingTheNextBlockRemovesThePreviousView() {
	ctx := context.Background()
	rmA, _ := s.newRM()
	b1, f1 := s.proposal(rmA, 301, "vr1a", true)
	s.Require().NoError(rmA.FinalizeBlock(ctx, b1))
	s.Require().Equal(api.HexBytes(f1.Raw), s.stored(301).PartitionView, "the finalized pair is kept")

	rmB, _ := s.newRM()
	b2, f2 := s.proposal(rmB, 302, "vr1b", true)
	s.Require().NoError(rmB.FinalizeBlock(ctx, b2))
	s.Require().Empty(s.stored(301).PartitionView, "superseded by the new finalized block")
	s.Require().Equal(api.HexBytes(f2.Raw), s.stored(302).PartitionView, "the new UC/view pair stays together")
	s.Require().Equal(b2.UnicityCertificate, s.stored(302).UnicityCertificate)
	s.Require().Equal(b1.UnicityCertificate, s.stored(301).UnicityCertificate, "the UC of the old block is untouched")
}

func (s *ViewRetentionSuite) Test2_NullViewLatestStillRemovesThePreviousView() {
	ctx := context.Background()
	rmA, _ := s.newRM()
	b1, _ := s.proposal(rmA, 311, "vr2a", true)
	s.Require().NoError(rmA.FinalizeBlock(ctx, b1))
	rmB, _ := s.newRM()
	b2, _ := s.proposal(rmB, 312, "vr2b", false)
	s.Require().NoError(rmB.FinalizeBlock(ctx, b2))
	s.Require().Empty(s.stored(311).PartitionView)
	s.Require().Empty(s.stored(312).PartitionView, "no view is invented for the null-view block")
}

func (s *ViewRetentionSuite) Test3_CleanupFailureIsObservableAndNeverFailsFinalization() {
	ctx := context.Background()
	rmA, _ := s.newRM()
	b1, f1 := s.proposal(rmA, 321, "vr3a", true)
	s.Require().NoError(rmA.FinalizeBlock(ctx, b1))

	rmB, _ := s.newRM()
	b2, _ := s.proposal(rmB, 322, "vr3b", true)
	bs, ok := s.storage.BlockStorage().(*mongodb.BlockStorage)
	s.Require().True(ok)
	rmB.storage = &pruneFailStorage{Storage: s.storage, bs: &pruneFailBlockStorage{BlockStorage: bs, err: errors.New("mongo hiccup")}}
	before := testutil.ToFloat64(metrics.PartitionViewPruneTotal.WithLabelValues("error"))

	s.Require().NoError(rmB.FinalizeBlock(ctx, b2), "a cleanup problem must not fail finalization")
	s.Require().True(s.stored(322).Finalized)
	s.Require().Equal(before+1, testutil.ToFloat64(metrics.PartitionViewPruneTotal.WithLabelValues("error")), "observable")
	s.Require().Equal(api.HexBytes(f1.Raw), s.stored(321).PartitionView, "the leftover is harmless and stays until a retry")

	// the retry (the next finalization or the startup sweep) cleans it
	rmB.storage = s.storage
	rmB.pruneRecentViews(ctx, s.stored(322))
	s.Require().Empty(s.stored(321).PartitionView)
}

// The startup sweep is bounded per run; what it leaves is cleaned by the following runs, and the
// attachment of the finalized head is never touched.
func (s *ViewRetentionSuite) Test4_StartupSweepIsBoundedAndKeepsTheHead() {
	ctx := context.Background()
	rm, _ := s.newRM()
	// views left by the earlier tests of this shared database are not under test here
	rm.pruneSupersededViews(ctx, &models.Block{Index: api.NewBigInt(big.NewInt(1000))}, partitionViewPruneBatchesOnStartup)
	const n = partitionViewPruneBatch + 6
	for i := 0; i < n+1; i++ {
		b := models.NewBlock(api.NewBigInt(big.NewInt(int64(1000+i))), "unicity", 0, "1.0", "mainnet",
			api.NewHexBytes([]byte(fmt.Sprintf("root%d", i))), nil, api.NewHexBytes([]byte(fmt.Sprintf("uc%d", i))), 1)
		b.PartitionView = api.NewHexBytes([]byte("old-history"))
		b.Finalized = false
		s.Require().NoError(s.storage.BlockStorage().Store(ctx, b))
		s.Require().NoError(s.storage.BlockStorage().(interface {
			SetFinalizedWithCertificate(context.Context, *models.Block) error
		}).SetFinalizedWithCertificate(ctx, b))
	}
	head := s.stored(int64(1000 + n))
	removed := func() int {
		c := 0
		for i := 0; i < n; i++ {
			if len(s.stored(int64(1000+i)).PartitionView) == 0 {
				c++
			}
		}
		return c
	}

	rm.pruneSupersededViews(ctx, head, 1)
	s.Require().Equal(partitionViewPruneBatch, removed(), "one run cleans at most one batch")
	rm.pruneSupersededViews(ctx, head, 1)
	s.Require().Equal(n, removed(), "the next run finishes the sweep")
	s.Require().Equal(api.HexBytes("old-history"), head.PartitionView)
	s.Require().Equal(api.HexBytes("old-history"), s.stored(int64(1000+n)).PartitionView, "the head keeps its view")
}
