package proofview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Reserved pins count against the pin limit for everyone else, are converted by
// AcquireLatestReserved without a second limit check, and are idempotently released.
func TestPinReservation(t *testing.T) {
	l := testLimits()
	l.MaxConcurrentPins = 4
	c := newCatalog(t, l)
	publish(t, c, 1, 1, newFakeReader(1, 10), nil)
	ctx := context.Background()

	// leave=1: three of four may be reserved
	var rs []*PinReservation
	for i := 0; i < 3; i++ {
		r, ok := c.TryReservePin(1)
		require.True(t, ok, i)
		rs = append(rs, r)
	}
	_, ok := c.TryReservePin(1)
	require.False(t, ok, "the last pin is left to fresh requests")

	// a fresh acquisition gets that last pin, and then the limit applies to it too
	fresh, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	_, err = c.AcquireLatest(ctx)
	require.ErrorIs(t, err, ErrBusyPins, "reserved pins are not available to fresh acquisitions")
	_, ok = c.TryReservePin(0)
	require.False(t, ok)

	// a reservation converts into a lease without re-checking the (full) limit
	l1, err := c.AcquireLatestReserved(ctx, rs[0])
	require.NoError(t, err)
	require.Equal(t, 2, c.Stats().ActivePins, "fresh + converted")
	// converting twice does not take a second pin: the reservation is spent, the limit applies
	_, err = c.AcquireLatestReserved(ctx, rs[0])
	require.ErrorIs(t, err, ErrBusyPins)

	// releasing is idempotent, also after conversion
	rs[0].Release()
	rs[1].Release()
	rs[1].Release()
	require.Equal(t, 2, c.Stats().ActivePins)
	r4, ok := c.TryReservePin(0)
	require.True(t, ok, "released capacity is reservable again")
	r4.Release()

	// a failed acquisition leaves the reservation to its owner
	c2 := newCatalog(t, l)
	r, ok := c2.TryReservePin(0)
	require.True(t, ok)
	_, err = c2.AcquireLatestReserved(ctx, r) // nothing published yet
	require.ErrorIs(t, err, ErrNoFinalizedView)
	r.Release()
	r.Release()
	for i := 0; i < 4; i++ {
		x, ok := c2.TryReservePin(0)
		require.True(t, ok)
		defer x.Release()
	}

	rs[2].Release()
	fresh.Release()
	l1.Release()
	require.Zero(t, c.Stats().ActivePins)
	for i := 0; i < 4; i++ { // nothing leaked from the reservation counter
		x, ok := c.TryReservePin(0)
		require.True(t, ok, i)
		defer x.Release()
	}
	var nilRes *PinReservation
	nilRes.Release()
}

func TestPinReservationOnAClosedCatalog(t *testing.T) {
	c := newCatalog(t, testLimits())
	require.NoError(t, c.Close())
	_, ok := c.TryReservePin(0)
	require.False(t, ok)
}
