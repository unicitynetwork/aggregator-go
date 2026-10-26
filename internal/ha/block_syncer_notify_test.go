package ha

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/ha/state"
	"github.com/unicitynetwork/aggregator-go/internal/longpoll"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// recordingPublisher stands in for the retained-view publisher: it records the order of events.
type recordingPublisher struct {
	mu     sync.Mutex
	events []string
	latest int64
}

func (p *recordingPublisher) PublishFinalized(_ context.Context, b *models.Block) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latest = b.Index.Int64()
	p.events = append(p.events, "publish")
}

func (p *recordingPublisher) RetryLatest(context.Context, *models.Block) bool { return false }

func (p *recordingPublisher) note(e string) {
	p.mu.Lock()
	p.events = append(p.events, e)
	p.mu.Unlock()
}

func (p *recordingPublisher) Latest() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.latest
}

// C03/T6: a follower catching up through B1..B3 skips the intermediate retained views. A waiter
// whose state entered at B1 must be rechecked against the newest local published view (B3); a
// per-block ID wake for B3 alone would miss it. The notification comes after the target's
// publication and the evaluator reads the latest view, not the block's ID list.
func TestBlockSyncerNotify_CatchUpWakesAfterTargetPublication(t *testing.T) {
	ctx := t.Context()
	fixture := newBlockSyncerFixture(t)
	fixture.addBlock(t, 1, 1)
	fixture.addBlock(t, 2, 1)
	fixture.addBlock(t, 3, 1)

	reg := longpoll.New(longpoll.Config{MaxWaiters: 4, Concurrency: 8})
	w, a := reg.Admit()
	require.Equal(t, longpoll.Admitted, a)
	defer w.Close()

	pub := &recordingPublisher{}
	syncer := NewBlockSyncer(testBlockSyncerLogger(t), &mockLeaderSelector{}, fixture.storage, newFakeDiskBackend(), 0, time.Second, state.NewSyncStateTracker())
	syncer.SetViewPublisher(pub)
	notifies := 0
	var seenLatest []int64
	syncer.SetNotify(func() {
		notifies++
		seenLatest = append(seenLatest, pub.Latest())
		pub.note("notify")
		reg.Notify()
	})

	// the waiter evaluates before the sync: "included at some published block >= 1"?
	included := func() bool { return pub.Latest() >= 1 }
	seen := w.Generation()
	require.False(t, included())
	woke := make(chan longpoll.Wake, 1)
	go func() { woke <- w.Wait(ctx, seen, time.Now().Add(time.Minute)) }()
	time.Sleep(20 * time.Millisecond)

	require.NoError(t, syncer.SyncToLatestBlock(ctx))

	select {
	case k := <-woke:
		require.Contains(t, []longpoll.Wake{longpoll.WakeSignal, longpoll.WakeChanged}, k)
	case <-time.After(5 * time.Second):
		t.Fatal("the catch-up never woke the waiter")
	}
	require.True(t, included(), "recheck against the latest local published view sees the state entered at B1")
	require.Equal(t, int64(3), pub.Latest())
	require.Equal(t, 3, notifies, "one signal per replayed block, each after that block's publication attempt")
	require.Equal(t, int64(3), seenLatest[len(seenLatest)-1], "the final notification follows the target's publication")
	require.Equal(t, []string{"publish", "notify"}, pub.events[len(pub.events)-2:], "publication first, then the signal")
	require.Equal(t, big.NewInt(3), syncer.stateTracker.GetLastSyncedBlock())
}

// A backend without a retained catalog still signals (legacy read view readiness).
func TestBlockSyncerNotify_WithoutPublisherStillSignals(t *testing.T) {
	ctx := t.Context()
	fixture := newBlockSyncerFixture(t)
	fixture.addBlock(t, 1, 1)
	fixture.addBlock(t, 2, 1)
	syncer := NewBlockSyncer(testBlockSyncerLogger(t), &mockLeaderSelector{}, fixture.storage, newFakeDiskBackend(), 0, time.Second, state.NewSyncStateTracker())
	n := 0
	syncer.SetNotify(func() { n++ })
	require.NoError(t, syncer.SyncToLatestBlock(ctx))
	require.Equal(t, 2, n)
}

// A failing replay signals what it did publish and nothing after the failure.
func TestBlockSyncerNotify_FailedReplayDoesNotSignalTheFailedBlock(t *testing.T) {
	ctx := t.Context()
	fixture := newBlockSyncerFixture(t)
	fixture.addBlock(t, 1, 1)
	fixture.addBlock(t, 2, 1)
	fixture.storage.blocks.byNumber["2"].RootHash = api.HexBytes(bytesOf(32, 9))
	syncer := NewBlockSyncer(testBlockSyncerLogger(t), &mockLeaderSelector{}, fixture.storage, newFakeDiskBackend(), 0, time.Second, state.NewSyncStateTracker())
	n := 0
	syncer.SetNotify(func() { n++ })
	require.Error(t, syncer.SyncToLatestBlock(ctx))
	require.LessOrEqual(t, n, 1)
}
