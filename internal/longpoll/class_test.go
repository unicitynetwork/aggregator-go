package longpoll

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// fakeAdmitter is a method capacity of `free` units.
type fakeAdmitter struct {
	mu     sync.Mutex
	free   int
	denied int
	live   int
}

func (f *fakeAdmitter) TryAdmit() (Resources, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.free <= 0 {
		f.denied++
		return nil, false
	}
	f.free--
	f.live++
	return &fakeRes{f: f}, true
}

func (f *fakeAdmitter) set(free int) {
	f.mu.Lock()
	f.free = free
	f.mu.Unlock()
}

func (f *fakeAdmitter) state() (free, live, denied int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.free, f.live, f.denied
}

type fakeRes struct {
	f    *fakeAdmitter
	once sync.Once
}

func (r *fakeRes) Release() {
	r.once.Do(func() {
		r.f.mu.Lock()
		r.f.free++
		r.f.live--
		r.f.mu.Unlock()
	})
}

func parkedPermit(t *testing.T, srv *jsonrpc.Server) *jsonrpc.Permit {
	t.Helper()
	p, ok := srv.AcquirePermit()
	require.True(t, ok)
	p.Release()
	return p
}

// The class cap bounds simultaneously running resumed reads of the class inside the global quota.
func TestClass_CapBoundsRunningReads(t *testing.T) {
	reg, srv := newPair(t, 16, 40) // global quota 10
	adm := &fakeAdmitter{free: 100}
	cls := reg.NewClass("batch", 2, adm)
	require.Equal(t, 2, cls.Cap())
	require.Equal(t, 1, reg.NewClass("x", 0, nil).Cap(), "at least one")
	require.Equal(t, reg.MaxRunning(), reg.NewClass("y", 1000, nil).Cap(), "never above the global quota")

	var ws []*Waiter
	var ps []*jsonrpc.Permit
	grants := make(chan *Grant, 5)
	for i := 0; i < 5; i++ {
		p := parkedPermit(t, srv)
		w, _ := reg.Admit()
		ws, ps = append(ws, w), append(ps, p)
		go func() {
			g, err := w.ResumeClass(context.Background(), p, time.Now().Add(5*time.Second), cls)
			if err == nil {
				grants <- g
			}
		}()
	}
	var running []*Grant
	for i := 0; i < 2; i++ {
		select {
		case g := <-grants:
			running = append(running, g)
		case <-time.After(2 * time.Second):
			t.Fatal("expected two grants")
		}
	}
	select {
	case <-grants:
		t.Fatal("a third class read was granted beyond the cap")
	case <-time.After(100 * time.Millisecond):
	}
	require.Equal(t, 2, reg.Stats().Running)
	_, live, _ := adm.state()
	require.Equal(t, 2, live)
	for len(running) > 0 { // each finished read lets the next one in
		running[0].Done()
		running = running[1:]
		select {
		case g := <-grants:
			running = append(running, g)
		case <-time.After(300 * time.Millisecond):
		}
	}
	for i := range ps {
		ps[i].Close()
		ws[i].Close()
	}
	_, live, _ = adm.state()
	require.Zero(t, live, "the unused admissions went back with Done")
	require.Zero(t, srv.ActiveRequests())
}

// A denied class admission rolls the HTTP permit back completely, queues the ticket without
// spinning, and a capacity release schedules it.
func TestClass_AdmissionContentionRollsBackAndWaitsForARelease(t *testing.T) {
	reg, srv := newPair(t, 8, 8)
	adm := &fakeAdmitter{free: 0}
	cls := reg.NewClass("batch", 4, adm)
	p := parkedPermit(t, srv)
	w, _ := reg.Admit()
	got := make(chan error, 1)
	go func() {
		g, err := w.ResumeClass(context.Background(), p, time.Now().Add(5*time.Second), cls)
		if err == nil {
			defer g.Done()
		}
		got <- err
	}()
	require.Eventually(t, func() bool { return reg.Stats().Queued == 1 }, time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	_, _, denied := adm.state()
	require.Positive(t, denied)
	require.False(t, p.Held(), "the HTTP permit taken for the grant was given back")
	require.Zero(t, srv.ActiveRequests(), "complete rollback")
	require.Zero(t, reg.Stats().Running)
	deniedBefore := denied
	time.Sleep(100 * time.Millisecond)
	_, _, denied = adm.state()
	require.Equal(t, deniedBefore, denied, "no polling: it only retries when something is released")

	adm.set(1)
	reg.Schedule() // what the batch service's release hook does
	require.NoError(t, <-got)
	p.Close()
	w.Close()
	require.Zero(t, srv.ActiveRequests())
}

// FIFO holds inside a class; a blocked class does not block the other classes.
func TestClass_BlockedClassDoesNotStallOthersAndKeepsItsOwnOrder(t *testing.T) {
	reg, srv := newPair(t, 16, 40)
	adm := &fakeAdmitter{free: 0}
	batch := reg.NewClass("batch", 4, adm)

	order := make(chan string, 4)
	run := func(name string, cls *Class) (*Waiter, *jsonrpc.Permit) {
		p := parkedPermit(t, srv)
		w, _ := reg.Admit()
		go func() {
			g, err := w.ResumeClass(context.Background(), p, time.Now().Add(5*time.Second), cls)
			if err == nil {
				order <- name
				g.Done()
			}
		}()
		return w, p
	}
	w1, p1 := run("batch1", batch)
	require.Eventually(t, func() bool { return reg.Stats().Enqueued >= 1 }, time.Second, time.Millisecond)
	w2, p2 := run("batch2", batch)
	require.Eventually(t, func() bool { return reg.Stats().Enqueued >= 2 }, time.Second, time.Millisecond)
	w3, p3 := run("v2", nil)
	require.Equal(t, "v2", <-order, "the class-less read passes the blocked batch reads")

	adm.set(1)
	reg.Schedule()
	require.Equal(t, "batch1", <-order, "FIFO inside the class")
	reg.Schedule()
	require.Equal(t, "batch2", <-order)
	for _, p := range []*jsonrpc.Permit{p1, p2, p3} {
		p.Close()
	}
	for _, w := range []*Waiter{w1, w2, w3} {
		w.Close()
	}
}

// One deadline covers all contention: it is never reset by retries.
func TestClass_SharedDeadlineExpiresWhileAdmissionIsDenied(t *testing.T) {
	reg, srv := newPair(t, 8, 8)
	adm := &fakeAdmitter{free: 0}
	cls := reg.NewClass("batch", 4, adm)
	p := parkedPermit(t, srv)
	w, _ := reg.Admit()
	start := time.Now()
	done := make(chan error, 1)
	go func() {
		_, err := w.ResumeClass(context.Background(), p, start.Add(150*time.Millisecond), cls)
		done <- err
	}()
	// capacity churn that never fits this ticket must not extend its deadline
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				reg.Schedule()
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()
	require.ErrorIs(t, <-done, ErrResumeTimeout)
	close(stop)
	require.Less(t, time.Since(start), 600*time.Millisecond)
	require.Zero(t, reg.Stats().Queued)
	require.Zero(t, srv.ActiveRequests())
	p.Close()
	w.Close()
}

// The admission follows the grant: handed to the read, given back when the grant is revoked.
func TestClass_AdmissionIsHandedOverAndReleasedOnRevocation(t *testing.T) {
	reg, srv := newPair(t, 8, 8)
	adm := &fakeAdmitter{free: 3}
	cls := reg.NewClass("batch", 4, adm)

	p := parkedPermit(t, srv)
	w, _ := reg.Admit()
	g, err := w.ResumeClass(context.Background(), p, time.Now().Add(time.Second), cls)
	require.NoError(t, err)
	require.NotNil(t, g.Resources())
	free, live, _ := adm.state()
	require.Equal(t, 2, free)
	require.Equal(t, 1, live)
	g.Done()
	_, live, _ = adm.state()
	require.Zero(t, live, "Done returns what the read did not consume")

	// closed between the grant and the delivery: token, quota and admission all come back
	p.Release()
	w2, _ := reg.Admit()
	reg.testHookDeliver = func() { w2.Close() }
	_, err = w2.ResumeClass(context.Background(), p, time.Now().Add(time.Second), cls)
	reg.testHookDeliver = nil
	require.ErrorIs(t, err, ErrWaiterClosed)
	free, live, _ = adm.state()
	require.Equal(t, 3, free)
	require.Zero(t, live)
	require.Zero(t, reg.Stats().Running)
	require.False(t, p.Held())

	// the waiter's context ends after the grant: same
	p2 := parkedPermit(t, srv)
	w3, _ := reg.Admit()
	ctx, cancel := context.WithCancel(context.Background())
	reg.testHookDeliver = func() { cancel() }
	_, err = w3.ResumeClass(ctx, p2, time.Now().Add(time.Second), cls)
	reg.testHookDeliver = nil
	require.ErrorIs(t, err, context.Canceled)
	_, live, _ = adm.state()
	require.Zero(t, live)
	require.Zero(t, srv.ActiveRequests())
	p.Close()
	p2.Close()
	w.Close()
	w3.Close()
}
