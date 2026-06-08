package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
)

func TestSyncBeforeActivationCancelsOnLeadershipLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	log, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)
	eventBus := events.NewEventBus(log)
	syncer := &blockingBlockSyncer{started: make(chan struct{})}
	leader := &testLeaderSelector{leader: true}

	result := make(chan error, 1)
	go func() {
		result <- syncBeforeActivation(ctx, log, syncer, leader, eventBus)
	}()

	select {
	case <-syncer.started:
	case <-time.After(time.Second):
		require.Fail(t, "sync did not start")
	}

	eventBus.Publish(events.TopicLeaderChanged, &events.LeaderChangedEvent{IsLeader: false})

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		require.Fail(t, "sync was not cancelled after leadership loss")
	}
}

type blockingBlockSyncer struct {
	started chan struct{}
}

func (s *blockingBlockSyncer) Start(context.Context) {}
func (s *blockingBlockSyncer) Stop()                 {}
func (s *blockingBlockSyncer) SyncToLatestFinalizedBlock(ctx context.Context) (*models.Block, error) {
	close(s.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

type testLeaderSelector struct {
	leader bool
}

func (s *testLeaderSelector) IsLeader(context.Context) (bool, error) {
	return s.leader, nil
}

func (s *testLeaderSelector) VerifyLeadership(context.Context) (bool, error) {
	return s.leader, nil
}

func (s *testLeaderSelector) Resign(context.Context) error {
	s.leader = false
	return nil
}

func (s *testLeaderSelector) Start(context.Context) {}
func (s *testLeaderSelector) Stop(context.Context)  {}
