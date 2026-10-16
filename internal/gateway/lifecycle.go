package gateway

import (
	"context"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/longpoll"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// SetLongPoll enables the optional waitMs long-poll on get_inclusion_proof.v2 (the batch method
// is wired through service.NewBatchProofHandlerWithLongPoll with the same coordinator). Call
// before Start. Without it waitMs is validated and ignored. It also registers what the
// concurrency layer needs: which requests own their permit (those carrying a valid waitMs), the
// opt-in request bounds (64 KiB request, 32 KiB params, 128 ID bytes) and the resume scheduler hook.
func (s *Server) SetLongPoll(c *longpoll.Coordinator) {
	s.longPoll = c
	if c == nil {
		return
	}
	hasWait := func(params []byte) bool {
		w, err := api.ScanWaitMs(params)
		return err == nil && w.Present
	}
	for _, m := range []string{"get_inclusion_proof.v2", BatchProofsMethod} {
		s.rpcServer.SetOwnershipPredicate(m, hasWait)
	}
	s.rpcServer.SetOptInLimits("get_inclusion_proof.v2", jsonrpc.MethodLimits{
		MaxRequestBytes: api.InclusionProofsMaxRequestBytes,
		MaxParamsBytes:  api.InclusionProofsMaxParamsBytes,
		MaxIDBytes:      api.InclusionProofsMaxIDBytes,
	})
	s.rpcServer.SetReleaseHook(c.Reg.Schedule)
}

// connTracker remembers every accepted connection until it is closed. h2c hijacks its
// connections, which http.Server.Shutdown neither tracks nor waits for, so the gateway tracks
// them itself to drain handlers and to close the leftovers at the shutdown deadline.
type connTracker struct {
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newConnTracker() *connTracker { return &connTracker{conns: map[net.Conn]struct{}{}} }

type trackedConn struct {
	net.Conn
	t    *connTracker
	once sync.Once
}

func (c *trackedConn) Close() error {
	c.once.Do(func() {
		c.t.mu.Lock()
		delete(c.t.conns, c)
		c.t.mu.Unlock()
	})
	return c.Conn.Close()
}

func (t *connTracker) wrap(c net.Conn) net.Conn {
	tc := &trackedConn{Conn: c, t: t}
	t.mu.Lock()
	t.conns[tc] = struct{}{}
	t.mu.Unlock()
	return tc
}

// Open is the number of tracked connections.
func (t *connTracker) Open() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.conns)
}

// CloseAll closes every tracked connection and returns how many there were.
func (t *connTracker) CloseAll() int {
	t.mu.Lock()
	open := make([]net.Conn, 0, len(t.conns))
	for c := range t.conns {
		open = append(open, c)
	}
	t.mu.Unlock()
	for _, c := range open {
		_ = c.Close()
	}
	return len(open)
}

type trackedListener struct {
	net.Listener
	t *connTracker
}

func (l trackedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return l.t.wrap(c), nil
}

// trackInflight counts the handlers in flight, hijacked h2c streams included (they all pass the
// handler), so shutdown can wait for them while storage is still open.
func (s *Server) trackInflight(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&s.inflightN, 1)
		defer atomic.AddInt64(&s.inflightN, -1)
		h.ServeHTTP(w, r)
	})
}

// Inflight is the number of HTTP handlers currently running.
func (s *Server) Inflight() int { return int(atomic.LoadInt64(&s.inflightN)) }

// drain stops long-poll admission, signals every wait, and waits until the handlers and the waits
// have finished (storage is still open for them), or ctx ends.
func (s *Server) drain(ctx context.Context) error {
	if s.longPoll != nil {
		s.longPoll.Reg.Drain(true)
	}
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for s.Inflight() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	if s.longPoll != nil {
		return s.longPoll.Reg.Join(ctx)
	}
	return nil
}
