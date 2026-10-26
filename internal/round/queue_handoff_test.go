package round

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/testutil"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type dequeueBarrierContext struct {
	context.Context
	ready   chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *dequeueBarrierContext) Done() <-chan struct{} {
	c.once.Do(func() {
		close(c.ready)
		<-c.release
	})
	return c.Context.Done()
}

func TestRoundHandoffDoesNotConsumeReplay(t *testing.T) {
	rm := newStatusRM(t)
	rm.config = &config.Config{
		Processing: config.ProcessingConfig{CollectPhaseDuration: time.Minute},
		Sharding:   config.ShardingConfig{Mode: config.ShardingModeBFTShard},
	}
	rm.commitmentStream = make(chan *models.CertificationRequest, 1)
	rm.startStatusRound(t, 1, 1000)
	old := rm.currentRound
	replay := testutil.CreateTestCertificationRequest(t, "handoff_replay")
	ctx, cancel := context.WithCancel(t.Context())
	gate := &dequeueBarrierContext{Context: ctx, ready: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	unblock := func() { release.Do(func() { close(gate.release) }) }
	done := make(chan struct{})
	var processErr error
	go func() {
		defer close(done)
		processErr = rm.processRound(gate, old)
	}()
	t.Cleanup(func() {
		cancel()
		unblock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("round worker did not stop")
		}
	})
	select {
	case <-gate.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("round worker did not reach dequeue")
	}

	replacement := &Round{Number: api.NewBigIntFromUint64(2), ReferenceTime: 1001}
	// An atomic dequeue holds the lock here, so handoff must wait. Otherwise,
	// complete handoff first and expose a replay to the obsolete worker.
	if rm.roundMutex.TryLock() {
		rm.currentRound = replacement
		rm.commitmentStream <- replay
		rm.roundMutex.Unlock()
		unblock()
	} else {
		handoff := make(chan struct{})
		go func() {
			rm.roundMutex.Lock()
			rm.currentRound = replacement
			rm.roundMutex.Unlock()
			rm.commitmentStream <- replay
			close(handoff)
		}()
		unblock()
		select {
		case <-handoff:
		case <-time.After(5 * time.Second):
			t.Fatal("round handoff did not complete")
		}
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("superseded worker did not stop")
	}
	require.NoError(t, processErr)
	require.Empty(t, old.Commitments)
	select {
	case available := <-rm.commitmentStream:
		require.Same(t, replay, available)
	default:
		t.Fatal("superseded round consumed the replay without retaining it")
	}
}
