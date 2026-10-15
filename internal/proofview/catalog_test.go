package proofview

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// fakeReader is a backend.ReadView with controllable cost.
type fakeReader struct {
	root   []byte
	bytes  atomic.Uint64
	closes atomic.Int32
	inUse  atomic.Int32
}

func newFakeReader(n byte, bytes uint64) *fakeReader {
	r := &fakeReader{root: rootOf(n)}
	r.bytes.Store(bytes)
	return r
}

func rootOf(n byte) []byte {
	root := make([]byte, 32)
	root[31] = n
	return root
}

func (f *fakeReader) RootHash() []byte      { return append([]byte(nil), f.root...) }
func (f *fakeReader) RetainedBytes() uint64 { return f.bytes.Load() }
func (f *fakeReader) Close() error          { f.closes.Add(1); return nil }
func (f *fakeReader) GetInclusionCert(ctx context.Context, key []byte) (*api.InclusionCert, error) {
	if f.closes.Load() > 0 {
		return nil, errors.New("read after close")
	}
	f.inUse.Add(1)
	defer f.inUse.Add(-1)
	return &api.InclusionCert{}, ctx.Err()
}

func ucFor(t testing.TB, root []byte, round uint64) []byte {
	t.Helper()
	uc := types.UnicityCertificate{
		Version: 1,
		InputRecord: &types.InputRecord{
			Version: 1, RoundNumber: round, Hash: root, SummaryValue: []byte{},
		},
		UnicitySeal: &types.UnicitySeal{Version: 1, RootChainRoundNumber: round},
	}
	b, err := types.Cbor.Marshal(&uc)
	require.NoError(t, err)
	return b
}

func block(n uint64) BlockIdentity {
	return BlockIdentity{Number: api.NewBigIntFromUint64(n), ConfigLineage: []byte("cfg")}
}

func testLimits() Limits {
	l := DefaultLimits()
	l.RetentionBudgetBytes = 1 << 20
	return l
}

func newCatalog(t testing.TB, l Limits) *Catalog {
	t.Helper()
	c, err := NewCatalog(l)
	require.NoError(t, err)
	return c
}

func publish(t testing.TB, c *Catalog, n byte, round uint64, r *fakeReader, view []byte) {
	t.Helper()
	require.NoError(t, c.Publish(context.Background(), PublishInput{
		Block: block(uint64(n)), Root: rootOf(n), Reader: r,
		Cert: Certification{UC: ucFor(t, rootOf(n), round), PartitionView: view},
	}))
}

func TestLimitsDefaultsAndValidation(t *testing.T) {
	l := DefaultLimits()
	require.NoError(t, l.Validate())
	require.Equal(t, 16, l.MaxViews)
	require.Equal(t, 4, l.MaxAttachmentsPerView)
	require.EqualValues(t, 256<<20, l.RetentionBudgetBytes)
	require.Equal(t, 8, l.MaxConcurrentPins)
	require.Equal(t, 5*time.Second, l.MaxLeaseDuration)
	for _, mut := range []func(*Limits){
		func(l *Limits) { l.MaxViews = 0 },
		func(l *Limits) { l.MaxAttachmentsPerView = 0 },
		func(l *Limits) { l.RetentionBudgetBytes = 0 },
		func(l *Limits) { l.MaxConcurrentPins = 0 },
		func(l *Limits) { l.MaxLeaseDuration = 0 },
	} {
		bad := DefaultLimits()
		mut(&bad)
		require.Error(t, bad.Validate())
		_, err := NewCatalog(bad)
		require.Error(t, err)
	}
}

func TestEmptyCatalogHasNoFinalizedView(t *testing.T) {
	c := newCatalog(t, testLimits())
	_, err := c.AcquireLatest(context.Background())
	require.ErrorIs(t, err, ErrNoFinalizedView)
	_, err = c.AcquireRoot(context.Background(), rootOf(1))
	require.ErrorIs(t, err, ErrViewNotRetained)
}

func TestPublishAndAcquireLatestAndRoot(t *testing.T) {
	c := newCatalog(t, testLimits())
	ctx := context.Background()
	publish(t, c, 1, 10, newFakeReader(1, 100), nil)
	publish(t, c, 2, 11, newFakeReader(2, 100), []byte("view-2"))

	l, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, rootOf(2), l.Root())
	require.EqualValues(t, 2, l.Block().Number.Uint64())
	require.Equal(t, ucFor(t, rootOf(2), 11), l.UC())
	require.Equal(t, []byte("view-2"), l.PartitionView())
	_, err = l.GetInclusionCert(make([]byte, 32))
	require.NoError(t, err)
	l.Release()

	l, err = c.AcquireRoot(ctx, rootOf(1))
	require.NoError(t, err)
	require.Equal(t, rootOf(1), l.Root())
	require.Nil(t, l.PartitionView())
	l.Release()

	// A root that was never published never substitutes another view.
	_, err = c.AcquireRoot(ctx, rootOf(9))
	require.ErrorIs(t, err, ErrViewNotRetained)
}

func TestPublishValidatesRootAgreementAndClosesReader(t *testing.T) {
	c := newCatalog(t, testLimits())
	ctx := context.Background()
	for name, in := range map[string]func(r *fakeReader) PublishInput{
		"declared root differs from reader": func(r *fakeReader) PublishInput {
			return PublishInput{Block: block(1), Root: rootOf(2), Reader: r, Cert: Certification{UC: ucFor(t, rootOf(2), 1)}}
		},
		"UC root differs from view": func(r *fakeReader) PublishInput {
			return PublishInput{Block: block(1), Root: rootOf(1), Reader: r, Cert: Certification{UC: ucFor(t, rootOf(7), 1)}}
		},
		"missing UC": func(r *fakeReader) PublishInput {
			return PublishInput{Block: block(1), Root: rootOf(1), Reader: r}
		},
		"garbage UC": func(r *fakeReader) PublishInput {
			return PublishInput{Block: block(1), Root: rootOf(1), Reader: r, Cert: Certification{UC: []byte{1, 2, 3}}}
		},
		"missing block number": func(r *fakeReader) PublishInput {
			return PublishInput{Root: rootOf(1), Reader: r, Cert: Certification{UC: ucFor(t, rootOf(1), 1)}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := newFakeReader(1, 10)
			err := c.Publish(ctx, in(r))
			require.ErrorIs(t, err, ErrInvalidPublication)
			require.EqualValues(t, 1, r.closes.Load(), "rejected reader must be closed")
		})
	}
	require.Zero(t, c.Stats().Views)
	_, err := c.AcquireLatest(ctx)
	require.ErrorIs(t, err, ErrNoFinalizedView)
}

func TestEvictsOldestByViewCountAndClosesUnpinned(t *testing.T) {
	l := testLimits()
	l.MaxViews = 3
	c := newCatalog(t, l)
	readers := map[byte]*fakeReader{}
	for n := byte(1); n <= 5; n++ {
		readers[n] = newFakeReader(n, 10)
		publish(t, c, n, uint64(n), readers[n], nil)
	}
	require.Equal(t, 3, c.Stats().Views)
	require.EqualValues(t, 1, readers[1].closes.Load())
	require.EqualValues(t, 1, readers[2].closes.Load())
	for n := byte(3); n <= 5; n++ {
		require.Zero(t, readers[n].closes.Load())
	}
	_, err := c.AcquireRoot(context.Background(), rootOf(1))
	require.ErrorIs(t, err, ErrViewNotRetained)
	lease, err := c.AcquireRoot(context.Background(), rootOf(3))
	require.NoError(t, err)
	lease.Release()
}

func TestPinnedViewSurvivesEvictionUntilReleased(t *testing.T) {
	l := testLimits()
	l.MaxViews = 2
	c := newCatalog(t, l)
	r1 := newFakeReader(1, 10)
	publish(t, c, 1, 1, r1, nil)
	lease, err := c.AcquireRoot(context.Background(), rootOf(1))
	require.NoError(t, err)

	publish(t, c, 2, 2, newFakeReader(2, 10), nil)
	pin2, err := c.AcquireRoot(context.Background(), rootOf(2))
	require.NoError(t, err)
	defer pin2.Release()
	publish(t, c, 3, 3, newFakeReader(3, 10), nil) // both older views pinned: view 1 retires

	st := c.Stats()
	require.Equal(t, 2, st.Views)
	require.Equal(t, 1, st.RetiredPinned)
	require.Zero(t, r1.closes.Load(), "reader must not close under a live lease")
	_, err = lease.GetInclusionCert(make([]byte, 32))
	require.NoError(t, err)
	// New requests no longer see it.
	_, err = c.AcquireRoot(context.Background(), rootOf(1))
	require.ErrorIs(t, err, ErrViewNotRetained)

	lease.Release()
	lease.Release() // idempotent
	require.EqualValues(t, 1, r1.closes.Load(), "closed exactly once after last release")
	require.Zero(t, c.Stats().RetiredPinned)
	_, err = lease.GetInclusionCert(make([]byte, 32))
	require.ErrorIs(t, err, ErrLeaseReleased)
}

func TestRefCountSharedByMultipleLeases(t *testing.T) {
	l := testLimits()
	l.MaxViews = 1
	c := newCatalog(t, l)
	r1 := newFakeReader(1, 10)
	publish(t, c, 1, 1, r1, nil)
	a, err := c.AcquireLatest(context.Background())
	require.NoError(t, err)
	b, err := c.AcquireLatest(context.Background())
	require.NoError(t, err)
	publish(t, c, 2, 2, newFakeReader(2, 10), nil)
	a.Release()
	require.Zero(t, r1.closes.Load())
	b.Release()
	require.EqualValues(t, 1, r1.closes.Load())
}

func TestSameRootAttachmentsBoundedLatestExplicitAndLeasedSurvive(t *testing.T) {
	l := testLimits()
	l.MaxAttachmentsPerView = 2
	c := newCatalog(t, l)
	ctx := context.Background()
	publish(t, c, 1, 100, newFakeReader(1, 10), nil)

	first, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	firstUC := first.UC()

	for round := uint64(101); round <= 103; round++ {
		require.NoError(t, c.AttachCertification(ctx, block(1), rootOf(1), Certification{UC: ucFor(t, rootOf(1), round)}))
	}
	require.Equal(t, 1, c.Stats().Views, "repeat UCs share one view")

	latest, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, ucFor(t, rootOf(1), 103), latest.UC())
	byRoot, err := c.AcquireRoot(ctx, rootOf(1))
	require.NoError(t, err)
	require.Equal(t, ucFor(t, rootOf(1), 103), byRoot.UC(), "exact-root also serves the latest genuine UC")
	latest.Release()
	byRoot.Release()

	// The first lease keeps the original, replaced attachment.
	require.Equal(t, ucFor(t, rootOf(1), 100), firstUC)
	require.Equal(t, firstUC, first.UC())
	require.Equal(t, 2, attachmentCount(c), "attachment cap is 2")
	require.Len(t, c.retiredAtts, 1)
	first.Release()
	require.Empty(t, c.retiredAtts)
}

func attachmentCount(c *Catalog) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.views {
		n += len(v.atts)
	}
	return n
}

func TestLateSidecarFillsExistingAttachmentButNeverOverwrites(t *testing.T) {
	c := newCatalog(t, testLimits())
	ctx := context.Background()
	publish(t, c, 1, 5, newFakeReader(1, 10), nil)
	uc := ucFor(t, rootOf(1), 5)

	require.NoError(t, c.AttachCertification(ctx, block(1), rootOf(1), Certification{UC: uc, PartitionView: []byte("v1")}))
	l, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, uc, l.UC())
	require.Equal(t, []byte("v1"), l.PartitionView())
	l.Release()
	require.Equal(t, 1, attachmentCount(c), "same UC replaced in place, not duplicated")

	// A second, different view for the same UC does not overwrite.
	require.NoError(t, c.AttachCertification(ctx, block(1), rootOf(1), Certification{UC: uc, PartitionView: []byte("other")}))
	l, err = c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), l.PartitionView())
	l.Release()

	// A bare duplicate of a UC that already has its view is a no-op.
	require.NoError(t, c.AttachCertification(ctx, block(1), rootOf(1), Certification{UC: uc}))
	l, err = c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), l.PartitionView())
	l.Release()
}

func TestLateSidecarOnLeasedAttachmentDoesNotChangeTheLease(t *testing.T) {
	c := newCatalog(t, testLimits())
	ctx := context.Background()
	publish(t, c, 1, 5, newFakeReader(1, 10), nil)
	old, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.NoError(t, c.AttachCertification(ctx, block(1), rootOf(1),
		Certification{UC: ucFor(t, rootOf(1), 5), PartitionView: []byte("late")}))
	require.Nil(t, old.PartitionView(), "leased attachment is immutable")
	cur, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("late"), cur.PartitionView())
	cur.Release()
	old.Release()
}

func TestAttachCertificationRequiresRetainedMatchingState(t *testing.T) {
	c := newCatalog(t, testLimits())
	ctx := context.Background()
	publish(t, c, 1, 5, newFakeReader(1, 10), nil)

	err := c.AttachCertification(ctx, block(2), rootOf(1), Certification{UC: ucFor(t, rootOf(1), 6)})
	require.ErrorIs(t, err, ErrViewNotRetained, "different block identity")
	wrongLineage := BlockIdentity{Number: block(1).Number, ConfigLineage: []byte("other")}
	err = c.AttachCertification(ctx, wrongLineage, rootOf(1), Certification{UC: ucFor(t, rootOf(1), 6)})
	require.ErrorIs(t, err, ErrViewNotRetained, "different configuration lineage")
	err = c.AttachCertification(ctx, block(1), rootOf(1), Certification{UC: ucFor(t, rootOf(3), 6)})
	require.ErrorIs(t, err, ErrInvalidPublication, "UC of another root")
	err = c.AttachCertification(ctx, block(7), rootOf(7), Certification{UC: ucFor(t, rootOf(7), 6)})
	require.ErrorIs(t, err, ErrViewNotRetained)
}

func TestPublishingSameStateAgainAddsAttachmentAndClosesRedundantReader(t *testing.T) {
	c := newCatalog(t, testLimits())
	first := newFakeReader(1, 10)
	publish(t, c, 1, 5, first, nil)
	again := newFakeReader(1, 10)
	publish(t, c, 1, 6, again, []byte("pv"))
	require.EqualValues(t, 1, again.closes.Load())
	require.Zero(t, first.closes.Load())
	require.Equal(t, 1, c.Stats().Views)
	l, err := c.AcquireLatest(context.Background())
	require.NoError(t, err)
	require.Equal(t, ucFor(t, rootOf(1), 6), l.UC())
	l.Release()
}

func TestNewestSameRootViewWinsAndRootNeverSubstitutes(t *testing.T) {
	// Two finalized blocks with an identical root (an empty block).
	c := newCatalog(t, testLimits())
	ctx := context.Background()
	rootA := rootOf(1)
	for n := uint64(10); n <= 11; n++ {
		require.NoError(t, c.Publish(ctx, PublishInput{
			Block: block(n), Root: rootA, Reader: newFakeReader(1, 10),
			Cert: Certification{UC: ucFor(t, rootA, n)},
		}))
	}
	l, err := c.AcquireRoot(ctx, rootA)
	require.NoError(t, err)
	require.EqualValues(t, 11, l.Block().Number.Uint64())
	l.Release()
}

func TestPinLimitIsNonBlocking(t *testing.T) {
	l := testLimits()
	l.MaxConcurrentPins = 2
	c := newCatalog(t, l)
	publish(t, c, 1, 1, newFakeReader(1, 10), nil)
	ctx := context.Background()
	a, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	b, err := c.AcquireRoot(ctx, rootOf(1))
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { _, err := c.AcquireLatest(ctx); done <- err }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrBusyPins)
	case <-time.After(2 * time.Second):
		t.Fatal("acquire blocked at the pin limit")
	}
	a.Release()
	c3, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	b.Release()
	c3.Release()
	require.Zero(t, c.Stats().ActivePins)
}

func TestLeaseContextCarriesDeadlineAndCallerCancellation(t *testing.T) {
	l := testLimits()
	l.MaxLeaseDuration = 50 * time.Millisecond
	c := newCatalog(t, l)
	publish(t, c, 1, 1, newFakeReader(1, 10), nil)

	lease, err := c.AcquireLatest(context.Background())
	require.NoError(t, err)
	require.Eventually(t, func() bool { return lease.Context().Err() != nil }, 2*time.Second, 5*time.Millisecond)
	_, err = lease.GetInclusionCert(make([]byte, 32))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	lease.Release()

	cctx, cancel := context.WithCancel(context.Background())
	lease, err = c.AcquireLatest(cctx)
	require.NoError(t, err)
	cancel()
	require.Error(t, lease.Context().Err())
	lease.Release()
	_, err = c.AcquireLatest(cctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestByteBudgetEvictsUnpinnedHistoryOldestFirst(t *testing.T) {
	l := testLimits()
	l.RetentionBudgetBytes = 5000
	c := newCatalog(t, l)
	var readers []*fakeReader
	for n := byte(1); n <= 6; n++ {
		r := newFakeReader(n, 1000)
		readers = append(readers, r)
		publish(t, c, n, uint64(n), r, nil)
	}
	st := c.Stats()
	require.LessOrEqual(t, st.RetainedBytes, l.RetentionBudgetBytes)
	require.Less(t, st.Views, 6)
	// The newest is retained, the oldest are gone.
	lease, err := c.AcquireLatest(context.Background())
	require.NoError(t, err)
	require.Equal(t, rootOf(6), lease.Root())
	lease.Release()
	require.EqualValues(t, 1, readers[0].closes.Load())
	require.Zero(t, readers[5].closes.Load())
}

func TestBudgetCountsGrowthOfRetainedReaderBytes(t *testing.T) {
	l := testLimits()
	l.RetentionBudgetBytes = 5000
	c := newCatalog(t, l)
	r1, r2 := newFakeReader(1, 100), newFakeReader(2, 100)
	publish(t, c, 1, 1, r1, nil)
	publish(t, c, 2, 2, r2, nil)
	require.Equal(t, 2, c.Stats().Views)
	r1.bytes.Store(1 << 20) // the backend charges view 1 for what view 2 replaced
	publish(t, c, 3, 3, newFakeReader(3, 100), nil)
	require.LessOrEqual(t, c.Stats().RetainedBytes, l.RetentionBudgetBytes)
	require.EqualValues(t, 1, r1.closes.Load())
}

func TestOversizedViewIsRefusedAndLatestIsUnavailableNotOlder(t *testing.T) {
	l := testLimits()
	l.RetentionBudgetBytes = 5000
	c := newCatalog(t, l)
	ctx := context.Background()
	publish(t, c, 1, 1, newFakeReader(1, 100), nil)

	big := newFakeReader(2, 1<<20)
	err := c.Publish(ctx, PublishInput{Block: block(2), Root: rootOf(2), Reader: big,
		Cert: Certification{UC: ucFor(t, rootOf(2), 2)}})
	require.ErrorIs(t, err, ErrBudget)
	require.EqualValues(t, 1, big.closes.Load())

	_, err = c.AcquireLatest(ctx)
	require.ErrorIs(t, err, ErrViewNotRetained, "never serve an older view as latest")
	// Older history is untouched and still servable by exact root.
	old, err := c.AcquireRoot(ctx, rootOf(1))
	require.NoError(t, err)
	old.Release()

	// A later successful publication restores latest.
	publish(t, c, 3, 3, newFakeReader(3, 100), nil)
	l3, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, rootOf(3), l3.Root())
	l3.Release()
}

func TestPressureStopsNewPinsAndCancelsRetiredLeasesWithoutClosingReaders(t *testing.T) {
	l := testLimits()
	l.MaxViews = 1
	l.RetentionBudgetBytes = 3000
	c := newCatalog(t, l)
	ctx := context.Background()
	r1 := newFakeReader(1, 2000)
	publish(t, c, 1, 1, r1, nil)
	lease, err := c.AcquireLatest(ctx)
	require.NoError(t, err)

	// View 1 retires while pinned; together with view 2 it exceeds the budget.
	publish(t, c, 2, 2, newFakeReader(2, 2000), nil)
	st := c.Stats()
	require.Equal(t, 1, st.RetiredPinned)
	require.Greater(t, st.RetainedBytes, l.RetentionBudgetBytes, "pinned retired view still counts")
	require.Error(t, lease.Context().Err(), "pressure cancels the retiring lease")
	require.EqualValues(t, 1, c.Stats().PressureKills)
	require.Zero(t, r1.closes.Load(), "cancellation never closes a live reader")

	_, err = c.AcquireLatest(ctx)
	require.ErrorIs(t, err, ErrBusyBudget)

	lease.Release()
	require.EqualValues(t, 1, r1.closes.Load())
	l2, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, rootOf(2), l2.Root())
	l2.Release()
}

func TestCloseCancelsLeasesAndClosesReadersOnLastRelease(t *testing.T) {
	c := newCatalog(t, testLimits())
	r1, r2 := newFakeReader(1, 10), newFakeReader(2, 10)
	publish(t, c, 1, 1, r1, nil)
	publish(t, c, 2, 2, r2, nil)
	lease, err := c.AcquireRoot(context.Background(), rootOf(1))
	require.NoError(t, err)

	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
	require.Error(t, lease.Context().Err())
	require.Zero(t, r1.closes.Load())
	require.EqualValues(t, 1, r2.closes.Load())
	_, err = c.AcquireLatest(context.Background())
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, c.Publish(context.Background(), PublishInput{Block: block(3), Root: rootOf(3),
		Reader: newFakeReader(3, 1), Cert: Certification{UC: ucFor(t, rootOf(3), 3)}}), ErrClosed)
	lease.Release()
	require.EqualValues(t, 1, r1.closes.Load())
}

func TestPublishedMaterialIsCopied(t *testing.T) {
	c := newCatalog(t, testLimits())
	uc := ucFor(t, rootOf(1), 1)
	view := []byte("view")
	num := api.NewBigIntFromUint64(1)
	lineage := []byte("cfg")
	require.NoError(t, c.Publish(context.Background(), PublishInput{
		Block: BlockIdentity{Number: num, ConfigLineage: lineage}, Root: rootOf(1),
		Reader: newFakeReader(1, 1), Cert: Certification{UC: uc, PartitionView: view},
	}))
	uc[0] ^= 0xff
	view[0] = 'X'
	num.SetUint64(99)
	lineage[0] = 'X'
	l, err := c.AcquireLatest(context.Background())
	require.NoError(t, err)
	defer l.Release()
	require.Equal(t, ucFor(t, rootOf(1), 1), l.UC())
	require.Equal(t, []byte("view"), l.PartitionView())
	require.EqualValues(t, 1, l.Block().Number.Uint64())
	require.Equal(t, []byte("cfg"), l.Block().ConfigLineage)
	// The returned identity is a copy too.
	l.Block().Number.SetUint64(5)
	require.EqualValues(t, 1, l.Block().Number.Uint64())
}

func TestConcurrentPublishAcquireReleaseUnderRace(t *testing.T) {
	l := testLimits()
	l.MaxViews = 4
	l.MaxConcurrentPins = 64
	c := newCatalog(t, l)
	var readers sync.Map
	publishOne := func(n byte) {
		r := newFakeReader(n, 10)
		readers.Store(n, r)
		require.NoError(t, c.Publish(context.Background(), PublishInput{
			Block: block(uint64(n)), Root: rootOf(n), Reader: r,
			Cert: Certification{UC: ucFor(t, rootOf(n), uint64(n))},
		}))
	}
	publishOne(1)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var served atomic.Int64
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				lease, err := c.AcquireLatest(context.Background())
				if err != nil {
					continue
				}
				if _, err := lease.GetInclusionCert(make([]byte, 32)); err != nil && lease.Context().Err() == nil {
					t.Errorf("read under lease failed: %v", err)
				}
				served.Add(1)
				lease.Release()
			}
		}()
	}
	for n := byte(2); n < 120; n++ {
		publishOne(n)
		if n%7 == 0 {
			_ = c.AttachCertification(context.Background(), block(uint64(n)), rootOf(n),
				Certification{UC: ucFor(t, rootOf(n), uint64(n)+1000), PartitionView: []byte(fmt.Sprint(n))})
		}
	}
	close(stop)
	wg.Wait()
	require.NoError(t, c.Close())
	require.Positive(t, served.Load())
	require.Zero(t, c.Stats().ActivePins)
	readers.Range(func(k, v any) bool {
		require.LessOrEqual(t, v.(*fakeReader).closes.Load(), int32(1), "reader %v closed more than once", k)
		return true
	})
}

func TestLimitsFromConfig(t *testing.T) {
	l, err := LimitsFromConfig(config.SMTConfig{})
	require.NoError(t, err)
	require.Equal(t, DefaultLimits(), l, "zero config selects defaults")

	l, err = LimitsFromConfig(config.SMTConfig{
		RetainedViews: 3, RetainedAttachmentsPerView: 2, RetentionBudgetMB: 8,
		MaxConcurrentBatches: 5, BatchTimeout: time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, Limits{MaxViews: 3, MaxAttachmentsPerView: 2, RetentionBudgetBytes: 8 << 20,
		MaxConcurrentPins: 5, MaxLeaseDuration: time.Second}, l)
}

// Regression: a rejected newer publication must keep latest unavailable even
// when a late sidecar is attached to the older retained UC.
func TestLateSidecarOnOlderViewDoesNotRestoreRejectedLatest(t *testing.T) {
	l := testLimits()
	l.RetentionBudgetBytes = 5000
	c := newCatalog(t, l)
	ctx := context.Background()
	publish(t, c, 1, 1, newFakeReader(1, 100), nil)
	err := c.Publish(ctx, PublishInput{Block: block(2), Root: rootOf(2), Reader: newFakeReader(2, 1<<20),
		Cert: Certification{UC: ucFor(t, rootOf(2), 2)}})
	require.ErrorIs(t, err, ErrBudget)

	require.NoError(t, c.AttachCertification(ctx, block(1), rootOf(1),
		Certification{UC: ucFor(t, rootOf(1), 1), PartitionView: []byte("late")}))
	_, err = c.AcquireLatest(ctx)
	require.ErrorIs(t, err, ErrViewNotRetained)

	// A repeat UC for the old state is older than the rejected publication.
	require.NoError(t, c.AttachCertification(ctx, block(1), rootOf(1), Certification{UC: ucFor(t, rootOf(1), 1)}))
	_, err = c.AcquireLatest(ctx)
	require.ErrorIs(t, err, ErrViewNotRetained)

	// A genuinely newer published view restores latest.
	publish(t, c, 3, 3, newFakeReader(3, 100), nil)
	lease, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, rootOf(3), lease.Root())
	lease.Release()
}

// Regression: a newer same-root UC rejected for budget keeps latest
// unavailable when an older UC's sidecar is filled in; only a newer UC clears it.
func TestRejectedNewerUCKeepsLatestUnavailableUntilNewerOne(t *testing.T) {
	l := testLimits()
	l.RetentionBudgetBytes = 1000
	c := newCatalog(t, l)
	ctx := context.Background()
	r := newFakeReader(1, 100)
	publish(t, c, 1, 1, r, nil)

	r.bytes.Store(2000) // the view alone now exceeds the budget: a new UC cannot be retained
	err := c.AttachCertification(ctx, block(1), rootOf(1), Certification{UC: ucFor(t, rootOf(1), 2)})
	require.ErrorIs(t, err, ErrBudget)
	_, err = c.AcquireLatest(ctx)
	require.ErrorIs(t, err, ErrViewNotRetained)

	r.bytes.Store(100)
	require.NoError(t, c.AttachCertification(ctx, block(1), rootOf(1),
		Certification{UC: ucFor(t, rootOf(1), 1), PartitionView: []byte("old")}))
	_, err = c.AcquireLatest(ctx)
	require.ErrorIs(t, err, ErrViewNotRetained, "filling an older UC must not restore latest")

	require.NoError(t, c.AttachCertification(ctx, block(1), rootOf(1), Certification{UC: ucFor(t, rootOf(1), 3)}))
	lease, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, ucFor(t, rootOf(1), 3), lease.UC())
	lease.Release()
}

// Regression: redelivering a UC whose attachment was evicted must not promote it.
func TestEvictedUCRedeliveryDoesNotBecomeLatest(t *testing.T) {
	l := testLimits()
	l.MaxAttachmentsPerView = 2
	c := newCatalog(t, l)
	ctx := context.Background()
	publish(t, c, 1, 1, newFakeReader(1, 10), nil) // A
	for round := uint64(2); round <= 3; round++ {  // B, C evict A
		require.NoError(t, c.AttachCertification(ctx, block(1), rootOf(1), Certification{UC: ucFor(t, rootOf(1), round)}))
	}
	// A's late sidecar (or plain redelivery) arrives.
	require.NoError(t, c.AttachCertification(ctx, block(1), rootOf(1),
		Certification{UC: ucFor(t, rootOf(1), 1), PartitionView: []byte("late-A")}))
	require.NoError(t, c.AttachCertification(ctx, block(1), rootOf(1), Certification{UC: ucFor(t, rootOf(1), 1)}))
	require.Equal(t, 2, attachmentCount(c))
	for _, acquire := range []func() (*Lease, error){
		func() (*Lease, error) { return c.AcquireLatest(ctx) },
		func() (*Lease, error) { return c.AcquireRoot(ctx, rootOf(1)) },
	} {
		lease, err := acquire()
		require.NoError(t, err)
		require.Equal(t, ucFor(t, rootOf(1), 3), lease.UC())
		lease.Release()
	}
}

// Regression: eviction removes unpinned history before a pinned entry.
func TestBudgetEvictionPrefersUnpinnedOverPinnedOldest(t *testing.T) {
	l := testLimits()
	l.RetentionBudgetBytes = 2500 // fits two 1000-byte views, not three
	c := newCatalog(t, l)
	ctx := context.Background()
	ra := newFakeReader(1, 1000)
	publish(t, c, 1, 1, ra, nil)
	pinA, err := c.AcquireRoot(ctx, rootOf(1))
	require.NoError(t, err)
	publish(t, c, 2, 2, newFakeReader(2, 1000), nil)
	publish(t, c, 3, 3, newFakeReader(3, 1000), nil)

	a, err := c.AcquireRoot(ctx, rootOf(1))
	require.NoError(t, err, "pinned A must stay reachable: evicting B alone satisfies the budget")
	a.Release()
	_, err = c.AcquireRoot(ctx, rootOf(2))
	require.ErrorIs(t, err, ErrViewNotRetained)
	require.Zero(t, ra.closes.Load())
	require.Zero(t, c.Stats().RetiredPinned)
	pinA.Release()
}

func TestViewCountLimitRetiresPinnedOnlyWhenNothingUnpinned(t *testing.T) {
	l := testLimits()
	l.MaxViews = 2
	c := newCatalog(t, l)
	ctx := context.Background()
	publish(t, c, 1, 1, newFakeReader(1, 10), nil)
	pin1, err := c.AcquireRoot(ctx, rootOf(1))
	require.NoError(t, err)
	publish(t, c, 2, 2, newFakeReader(2, 10), nil)
	pin2, err := c.AcquireRoot(ctx, rootOf(2))
	require.NoError(t, err)
	publish(t, c, 3, 3, newFakeReader(3, 10), nil) // all older views pinned: oldest retires
	require.Equal(t, 2, c.Stats().Views)
	require.Equal(t, 1, c.Stats().RetiredPinned)
	_, err = c.AcquireRoot(ctx, rootOf(1))
	require.ErrorIs(t, err, ErrViewNotRetained)
	pin1.Release()
	pin2.Release()
}
