package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/events"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/longpoll"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/round"
)

type transitionManager struct {
	round.Manager
	onActivate, onDeactivate func(context.Context) error
}

func (m *transitionManager) Activate(ctx context.Context) error   { return m.onActivate(ctx) }
func (m *transitionManager) Deactivate(ctx context.Context) error { return m.onDeactivate(ctx) }

// parent mode needs no block sync before activation, which keeps the promotion path simple
func parentModeConfig() *config.Config {
	c := &config.Config{}
	c.Sharding.Mode = config.ShardingModeParent
	return c
}

type okSyncer struct{}

func (okSyncer) Start(context.Context) {}
func (okSyncer) Stop()                 {}
func (okSyncer) SyncToLatestFinalizedBlock(context.Context) (*models.Block, error) {
	return nil, nil
}

// C06/T6: waits end on a local role transition (demotion here) and are admitted again once the
// transition settled; rapid repeated changes keep doing so, including a failing promotion.
func TestLeaderChangeDrainsWaitsAndReopens(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	bus := events.NewEventBus(log)
	reg := longpoll.New(longpoll.Config{MaxWaiters: 8, Concurrency: 16})

	var sawDraining atomic.Int32
	inTransition := func(context.Context) error {
		if _, a := reg.Admit(); a == longpoll.ShedDraining {
			sawDraining.Add(1)
		}
		return nil
	}
	mgr := &transitionManager{onDeactivate: inTransition, onActivate: inTransition}
	leader := &testLeaderSelector{leader: true}
	cfgDone := make(chan struct{})
	go func() {
		defer close(cfgDone)
		_ = startLeaderChangedEventListener(ctx, log, parentModeConfig(), mgr, okSyncer{}, leader, bus, reg)
	}()

	w, a := reg.Admit()
	require.Equal(t, longpoll.Admitted, a)
	woke := make(chan longpoll.Wake, 1)
	go func() { woke <- w.Wait(ctx, w.Generation(), time.Now().Add(time.Minute)) }()
	require.Eventually(t, func() bool { return reg.Stats().Reserved == 1 }, time.Second, time.Millisecond)

	// EventBus.Publish reaches only current subscribers: publish until the listener (which
	// subscribes in its own goroutine) has taken the event, whatever the host load.
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
publish:
	for {
		bus.Publish(events.TopicLeaderChanged, &events.LeaderChangedEvent{IsLeader: false})
		select {
		case k := <-woke:
			require.Equal(t, longpoll.WakeDrain, k)
			break publish
		case <-tick.C:
		case <-deadline:
			t.Fatal("the demotion did not end the wait")
		}
	}
	w.Close()
	require.Eventually(t, func() bool {
		w2, a := reg.Admit()
		if a == longpoll.Admitted {
			w2.Close()
		}
		return a == longpoll.Admitted
	}, 3*time.Second, 5*time.Millisecond, "admission reopens after the transition settled")
	require.GreaterOrEqual(t, sawDraining.Load(), int32(1), "admission was closed while Deactivate ran")

	// a promotion which fails still reopens; two quick changes in a row
	mgr.onActivate = func(context.Context) error { return errors.New("activate failed") }
	bus.Publish(events.TopicLeaderChanged, &events.LeaderChangedEvent{IsLeader: true})
	bus.Publish(events.TopicLeaderChanged, &events.LeaderChangedEvent{IsLeader: false})
	require.Eventually(t, func() bool {
		w2, a := reg.Admit()
		if a == longpoll.Admitted {
			w2.Close()
		}
		return a == longpoll.Admitted && sawDraining.Load() >= 2
	}, 3*time.Second, 5*time.Millisecond)

	// a shutdown drain is permanent
	reg.Drain(true)
	reg.Reopen()
	_, a = reg.Admit()
	require.Equal(t, longpoll.ShedDraining, a)
}
