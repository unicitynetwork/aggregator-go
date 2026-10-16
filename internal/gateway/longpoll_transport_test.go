package gateway

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/longpoll"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type liveGateway struct {
	srv  *Server
	reg  *longpoll.Registry
	addr string
	url  string
}

// startGateway runs the production gateway (gin, middleware stack, h2c, the tracked listener) on
// a loopback port with long-poll wired.
func startGateway(t *testing.T, svc Service, waiters, c int) *liveGateway {
	t.Helper()
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	cfg := &config.Config{}
	cfg.Server = config.ServerConfig{Host: "127.0.0.1", Port: strconv.Itoa(port), ConcurrencyLimit: c,
		ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute,
		EnableH2C: true, HTTP2MaxConcurrentStreams: 100}
	cfg.Sharding.Mode = config.ShardingModeStandalone
	srv := NewServer(cfg, log, svc)
	reg := longpoll.New(longpoll.Config{MaxWaiters: waiters, Concurrency: c})
	srv.SetLongPoll(&longpoll.Coordinator{Reg: reg, MaxWait: 10 * time.Second, Budget: 30 * time.Second,
		Reserve: 200 * time.Millisecond, ResumeReserveOverride: 500 * time.Millisecond})
	go func() { _ = srv.Start() }()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
		}
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	})
	return &liveGateway{srv: srv, reg: reg, addr: addr, url: "http://" + addr + "/"}
}

func (g *liveGateway) body(wait int) string {
	return `{"jsonrpc":"2.0","id":1,"method":"get_inclusion_proof.v2","params":{"stateId":"` + stateIDHex + `","includeStatus":true,"waitMs":` + strconv.Itoa(wait) + `}}`
}

func h1Client() *http.Client {
	return &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
}

func h2cClient() *http.Client {
	return &http.Client{Transport: &http2.Transport{AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}}}
}

func post(ctx context.Context, c *http.Client, url, body string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

func pendingService() *waitService {
	svc := &waitService{}
	svc.hint.Store(api.ProofStatusPending)
	return svc
}

func (g *liveGateway) cleanBaseline(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		return g.reg.Stats().Reserved == 0 && g.reg.Stats().Running == 0 && g.srv.rpcServer.ActiveRequests() == 0
	}, 3*time.Second, 5*time.Millisecond, "reservations, resumed reads and active work return to baseline")
}

// HTTP/1.1: a client that goes away frees its wait at once (no leaked reservation, timer or token).
func TestTransport_HTTP1DisconnectFreesTheWait(t *testing.T) {
	g := startGateway(t, pendingService(), 8, 20)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := post(ctx, h1Client(), g.url, g.body(8000)); done <- err }()
	require.Eventually(t, func() bool { return g.reg.Stats().Reserved == 1 }, 3*time.Second, 5*time.Millisecond)
	cancel()
	require.Error(t, <-done)
	g.cleanBaseline(t)
}

// h2c prior knowledge: a reset stream (RST_STREAM from the client cancelling the request) cancels
// the wait; sibling streams on the same connection are untouched and complete normally.
func TestTransport_H2CResetStreamFreesOnlyThatWait(t *testing.T) {
	svc := pendingService()
	g := startGateway(t, svc, 8, 20)
	c := h2cClient()
	ctx1, cancel1 := context.WithCancel(context.Background())
	res1 := make(chan error, 1)
	res2 := make(chan string, 1)
	go func() { _, err := post(ctx1, c, g.url, g.body(8000)); res1 <- err }()
	go func() { b, _ := post(context.Background(), c, g.url, g.body(8000)); res2 <- b }()
	require.Eventually(t, func() bool { return g.reg.Stats().Reserved == 2 }, 3*time.Second, 5*time.Millisecond)
	require.Zero(t, g.srv.rpcServer.ActiveRequests(), "parked streams hold no active-work token")

	cancel1() // RST_STREAM(CANCEL) for stream 1 only
	require.Error(t, <-res1)
	require.Eventually(t, func() bool { return g.reg.Stats().Reserved == 1 }, 3*time.Second, 5*time.Millisecond)

	svc.included.Store(true)
	g.reg.Notify()
	body := <-res2
	require.Contains(t, body, `"status":"included"`)
	require.Contains(t, body, `"waitedMs":`)
	g.cleanBaseline(t)
}

// One h2c connection multiplexes many waits; the global cap, not the stream limit, bounds them.
func TestTransport_H2CManyStreamsRespectTheGlobalCap(t *testing.T) {
	svc := pendingService()
	g := startGateway(t, svc, 3, 20) // cap 3
	c := h2cClient()
	type out struct{ body string }
	res := make(chan string, 6)
	for i := 0; i < 6; i++ {
		go func() { b, _ := post(context.Background(), c, g.url, g.body(8000)); res <- b }()
	}
	// three park, three are shed at once with a zero echo and the ordinary pending outcome
	var shed []string
	for len(shed) < 3 {
		select {
		case b := <-res:
			shed = append(shed, b)
		case <-time.After(3 * time.Second):
			t.Fatalf("expected 3 immediate (shed) answers, got %d", len(shed))
		}
	}
	for _, b := range shed {
		require.Contains(t, b, `"status":"pending","waitedMs":0}`)
	}
	require.EqualValues(t, 3, g.reg.Stats().Reserved)
	svc.included.Store(true)
	g.reg.Notify()
	for i := 0; i < 3; i++ {
		require.Contains(t, <-res, `"status":"included"`)
	}
	g.cleanBaseline(t)
}

// Shutdown: waits are signalled, answered with a final ordinary read while the "storage" (the
// service) is still open, tracked h2c work is drained, and nothing touches the service afterwards.
func TestTransport_ShutdownDrainsHTTP1AndH2CWaitsBeforeStorageCloses(t *testing.T) {
	svc := pendingService()
	var closed atomic.Bool
	var readAfterClose atomic.Int32
	guard := &guardedService{waitService: svc, closed: &closed, bad: &readAfterClose}
	g := startGateway(t, guard, 8, 20)
	res := make(chan string, 3)
	go func() {
		b, err := post(context.Background(), h1Client(), g.url, g.body(8000))
		res <- fmt.Sprint(b, err)
	}()
	go func() {
		b, err := post(context.Background(), h1Client(), g.url, g.body(8000))
		res <- fmt.Sprint(b, err)
	}()
	c2 := h2cClient()
	go func() { b, err := post(context.Background(), c2, g.url, g.body(8000)); res <- fmt.Sprint(b, err) }()
	require.Eventually(t, func() bool { return g.reg.Stats().Reserved == 3 }, 3*time.Second, 5*time.Millisecond)
	require.Positive(t, g.srv.conns.Open())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	require.NoError(t, g.srv.Stop(ctx))
	require.Less(t, time.Since(start), 3*time.Second, "drain is prompt, not the grace period")
	closed.Store(true) // main closes storage only after Stop returned

	for i := 0; i < 3; i++ {
		b := <-res
		require.Contains(t, b, `"status":"pending","waitedMs":`, "the final ordinary read with an echo, not an error")
	}
	require.Zero(t, g.srv.Inflight())
	require.Zero(t, g.reg.Stats().Reserved)
	require.Zero(t, g.srv.conns.Open(), "tracked (hijacked h2c included) connections are closed")
	time.Sleep(100 * time.Millisecond)
	require.Zero(t, readAfterClose.Load(), "no handler used the service after shutdown completed")
	_, err := post(context.Background(), h1Client(), g.url, g.body(100))
	require.Error(t, err, "the listener is closed")
}

type guardedService struct {
	*waitService
	closed *atomic.Bool
	bad    *atomic.Int32
}

func (s *guardedService) GetInclusionProofV2(ctx context.Context, r *api.GetInclusionProofRequestV2) (*api.GetInclusionProofResponseV2, error) {
	if s.closed.Load() {
		s.bad.Add(1)
	}
	return s.waitService.GetInclusionProofV2(ctx, r)
}

// A handler that does not cooperate is cut at the shutdown deadline: Stop reports it and closes
// the tracked connections instead of leaving the work to touch closed storage.
func TestTransport_ShutdownDeadlineClosesLeftovers(t *testing.T) {
	svc := &blockingService{release: make(chan struct{}), entered: make(chan struct{}, 4)}
	g := startGateway(t, svc, 8, 20)
	res := make(chan error, 1)
	go func() { _, err := post(context.Background(), h2cClient(), g.url, g.body(0)); res <- err }()
	select {
	case <-svc.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("handler never started")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := g.srv.Stop(ctx)
	require.ErrorIs(t, err, ErrShutdownIncomplete, "a handler that ignores cancellation is still running: the caller must not close storage")
	require.Zero(t, g.srv.conns.Open(), "leftover connections were closed")
	require.Error(t, <-res, "the client of the stuck handler lost its connection")
	close(svc.release)
}

type blockingService struct {
	fixedV2Service
	release chan struct{}
	entered chan struct{}
}

func (s *blockingService) GetInclusionProofV2(ctx context.Context, _ *api.GetInclusionProofRequestV2) (*api.GetInclusionProofResponseV2, error) {
	s.entered <- struct{}{}
	<-s.release
	return emptyV2Fixture(), nil
}

// h2c Upgrade: the upgraded connection is a hijacked one; it is tracked and closed at shutdown.
func TestTransport_H2CUpgradeConnectionIsTrackedAndClosedAtShutdown(t *testing.T) {
	g := startGateway(t, pendingService(), 8, 20)
	conn, err := net.Dial("tcp", g.addr)
	require.NoError(t, err)
	defer conn.Close()
	body := g.body(0)
	fmt.Fprintf(conn, "POST / HTTP/1.1\r\nHost: x\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQAAP__\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, line, "101", "switched to h2c")
	require.Positive(t, g.srv.conns.Open())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, g.srv.Stop(ctx))
	require.Zero(t, g.srv.conns.Open())
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		if _, err := br.ReadByte(); err != nil {
			break // EOF / reset: the connection is closed
		}
	}
}

// Shutdown closes admission on the connections that are already open: while one request is held,
// a sibling stream on the same h2c connection is refused without reaching the service, and Stop
// returns with nothing in flight.
func TestTransport_ShutdownRefusesNewStreamsOnOpenH2CConnections(t *testing.T) {
	svc := &blockingService{release: make(chan struct{}), entered: make(chan struct{}, 64)}
	g := startGateway(t, svc, 8, 20)
	c := h2cClient()
	held := make(chan error, 1)
	go func() { _, err := post(context.Background(), c, g.url, g.body(0)); held <- err }()
	<-svc.entered

	stopped := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopped <- g.srv.Stop(ctx)
	}()
	require.Eventually(t, func() bool { g.srv.admitMu.Lock(); defer g.srv.admitMu.Unlock(); return g.srv.closing }, 2*time.Second, time.Millisecond)

	// continuous arrivals on the same connection while draining
	for i := 0; i < 20; i++ {
		resp, err := c.Post(g.url, "application/json", strings.NewReader(g.body(0)))
		if err == nil {
			require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "refused, not served")
			_ = resp.Body.Close()
		}
	}
	select {
	case <-svc.entered:
		t.Fatal("a request entered the service after shutdown began")
	default:
	}
	require.EqualValues(t, 1, g.srv.Inflight(), "only the admitted request is still in flight")

	close(svc.release)
	require.NoError(t, <-stopped)
	require.Zero(t, g.srv.Inflight())
	require.Zero(t, g.srv.rpcServer.ActiveRequests())
	<-held
}

// An opt-in request whose worker outlives its (timeout) response keeps its token: Stop does not
// report completion while the worker runs, and returns once it stopped; if it never does, Stop
// says so and main must not close storage.
func TestTransport_ShutdownJoinsWorkersOfImmediateOptInRequests(t *testing.T) {
	old := MethodTimeout
	MethodTimeout = 50 * time.Millisecond
	defer func() { MethodTimeout = old }()

	svc := &blockingService{release: make(chan struct{}), entered: make(chan struct{}, 4)}
	g := startGateway(t, svc, 0, 20) // cap 0: waitMs requests are served immediately, no reservation
	res := make(chan string, 1)
	go func() { b, _ := post(context.Background(), h2cClient(), g.url, g.body(0)); res <- b }()
	<-svc.entered
	body := <-res
	require.Contains(t, body, "Request timeout")
	require.Contains(t, body, `"waitedMs":0`, "the middleware's error carries the echo of an opt-in request")
	require.Equal(t, 1, g.srv.rpcServer.ActiveRequests(), "the proof worker is still running and accounted")
	require.Zero(t, g.srv.Inflight(), "the outer handler is long gone: this is the case an in-flight count misses")

	// deadline passes while the worker blocks: reported as incomplete
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	err := g.srv.Stop(ctx)
	cancel()
	require.ErrorIs(t, err, ErrShutdownIncomplete)

	// once the worker ends, a later Stop completes
	close(svc.release)
	require.Eventually(t, func() bool { return g.srv.rpcServer.ActiveRequests() == 0 }, 2*time.Second, 5*time.Millisecond)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	require.NoError(t, g.srv.Stop(ctx2))
}

type observingService struct {
	fixedV2Service
	entered chan struct{}
	running atomic.Int32
}

func (s *observingService) GetInclusionProofV2(ctx context.Context, _ *api.GetInclusionProofRequestV2) (*api.GetInclusionProofResponseV2, error) {
	s.running.Add(1)
	defer s.running.Add(-1)
	s.entered <- struct{}{}
	<-ctx.Done() // a cooperative storage call: returns when its request context is cancelled
	return nil, ctx.Err()
}

// At the deadline residual work is cancelled through its context, joined, and nothing runs when
// Stop returns.
func TestTransport_ShutdownDeadlineCancelsAndJoinsCooperativeWork(t *testing.T) {
	svc := &observingService{entered: make(chan struct{}, 4)}
	g := startGateway(t, svc, 8, 20)
	go func() { _, _ = post(context.Background(), h2cClient(), g.url, g.body(0)) }()
	<-svc.entered
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
	defer cancel()
	err := g.srv.Stop(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, ErrShutdownIncomplete)
	require.Zero(t, svc.running.Load(), "the work was cancelled and joined before Stop returned")
	require.Zero(t, g.srv.Inflight())
	require.Zero(t, g.srv.rpcServer.ActiveRequests())
}
