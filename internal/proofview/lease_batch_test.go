package proofview

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestLeaseGetInclusionCerts(t *testing.T) {
	ctx := context.Background()
	m := newMemChain(t, testLimits())
	m.publishBlock(40)

	l, err := m.c.AcquireLatest(ctx)
	require.NoError(t, err)
	// the keys of the published leaves: regenerate the same spread
	keys := m.keysOf(0, 3, 30)
	certs, err := l.GetInclusionCerts(keys)
	require.NoError(t, err)
	require.Len(t, certs, 3)
	for i, k := range keys {
		single, err := l.GetInclusionCert(k)
		require.NoError(t, err)
		require.Equal(t, single.Siblings, certs[i].Siblings)
	}
	l.Release()
	_, err = l.GetInclusionCerts(keys)
	require.ErrorIs(t, err, ErrLeaseReleased)
}

// keysOf returns the tree keys of leaves publishBlock created (by sequence number).
func (m *memChain) keysOf(seq ...uint32) [][]byte {
	out := make([][]byte, 0, len(seq))
	for _, n := range seq {
		key := make([]byte, api.StateTreeKeyLengthBytes)
		binary.BigEndian.PutUint32(key[:4], n*2654435761)
		binary.BigEndian.PutUint32(key[len(key)-4:], n)
		out = append(out, key)
	}
	return out
}
