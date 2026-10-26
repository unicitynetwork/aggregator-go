package gateway

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

type halfCloseConn struct {
	net.Conn
	closedWrite int
}

func (c *halfCloseConn) CloseWrite() error { c.closedWrite++; return nil }

func TestTrackedConnForwardsCloseWrite(t *testing.T) {
	tr := newConnTracker()
	inner := &halfCloseConn{}
	tc := tr.wrap(inner)
	cw, ok := tc.(interface{ CloseWrite() error })
	require.True(t, ok, "net/http half-closes through this interface after an early error")
	require.NoError(t, cw.CloseWrite())
	require.Equal(t, 1, inner.closedWrite)

	// an inner connection without a half-close is harmless
	a, b := net.Pipe()
	defer b.Close()
	plain := tr.wrap(a)
	require.NoError(t, plain.(interface{ CloseWrite() error }).CloseWrite())
	require.NoError(t, plain.Close())
}
