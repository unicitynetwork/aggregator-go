package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixedBytesToPath_UsesLSBFirstBitAddressing(t *testing.T) {
	key := make([]byte, StateTreeKeyLengthBytes)
	key[0] = 0x01

	path, err := FixedBytesToPath(key, StateTreeKeyLengthBits)
	require.NoError(t, err)

	// v2 SMT semantics: depth 0 is bit 0 of byte 0.
	require.Equal(t, uint(1), path.Bit(0))
	require.Equal(t, uint(0), path.Bit(248))
}

func TestPathToFixedBytes_RoundtripLSBFirst(t *testing.T) {
	key := []byte{
		0x8d, 0x17, 0x23, 0x41, 0x99, 0xfe, 0x00, 0x7c,
		0x11, 0xaa, 0x52, 0x02, 0x7f, 0x03, 0x10, 0x20,
		0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xa0,
		0xb0, 0xc0, 0xd0, 0xe0, 0xf0, 0x12, 0x34, 0x56,
	}
	require.Len(t, key, StateTreeKeyLengthBytes)

	path, err := FixedBytesToPath(key, StateTreeKeyLengthBits)
	require.NoError(t, err)

	got, err := PathToFixedBytes(path, StateTreeKeyLengthBits)
	require.NoError(t, err)
	require.Equal(t, key, got)
}
