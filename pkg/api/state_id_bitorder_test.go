package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFixedBytesToPath_UsesBigEndianBitAddressing pins the yellowpaper
// big-endian bijection: key bit d (bit 7-d%8 of byte d/8, MSB-first) maps to
// big.Int path bit d. Canary keys with a single set bit per boundary depth
// distinguish big-endian from LSB-first (which would place these bits at the
// mirrored positions).
func TestFixedBytesToPath_UsesBigEndianBitAddressing(t *testing.T) {
	cases := []struct {
		name    string
		byteIdx int
		byteVal byte
		bit     int // expected path bit index
	}{
		{"depth0_msb_byte0", 0, 0x80, 0},
		{"depth1_byte0", 0, 0x40, 1},
		{"depth7_lsb_byte0", 0, 0x01, 7},
		{"depth8_msb_byte1", 1, 0x80, 8},
		{"depth200_msb_byte25", 25, 0x80, 200},
		{"depth255_lsb_byte31", 31, 0x01, 255},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			key := make([]byte, StateTreeKeyLengthBytes)
			key[c.byteIdx] = c.byteVal

			path, err := FixedBytesToPath(key, StateTreeKeyLengthBits)
			require.NoError(t, err)

			require.Equal(t, uint(1), path.Bit(c.bit), "path bit %d must be set", c.bit)
			// No other content bit may be set.
			for d := 0; d < StateTreeKeyLengthBits; d++ {
				if d == c.bit {
					continue
				}
				require.Equal(t, uint(0), path.Bit(d), "path bit %d must be clear", d)
			}
			// Sentinel bit at keyLengthBits is always set.
			require.Equal(t, uint(1), path.Bit(StateTreeKeyLengthBits))
		})
	}
}

func TestPathToFixedBytes_RoundtripBigEndian(t *testing.T) {
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

// TestKeyBitBE pins the big-endian bit accessor with an asymmetric canary so a
// half-flipped read cannot pass.
func TestKeyBitBE(t *testing.T) {
	key := make([]byte, StateTreeKeyLengthBytes)
	key[0] = 0b1010_0101 // MSB-first bits 0..7 = 1,0,1,0,0,1,0,1
	key[1] = 0x80        // bit 8 set (MSB of byte 1)
	key[31] = 0x01       // bit 255 set (LSB of byte 31)

	checks := []struct {
		pos  int
		want byte
	}{
		{0, 1}, {1, 0}, {2, 1}, {3, 0}, {4, 0}, {5, 1}, {6, 0}, {7, 1},
		{8, 1}, {9, 0},
		{248, 0}, {255, 1},
	}
	for _, c := range checks {
		if got := KeyBitBE(key, c.pos); got != c.want {
			t.Errorf("KeyBitBE(%d) = %d, want %d", c.pos, got, c.want)
		}
	}
}

// TestSetBitBE_RoundTripsKeyBitBE ensures the setter and getter agree at every
// boundary position.
func TestSetBitBE_RoundTripsKeyBitBE(t *testing.T) {
	for _, d := range []int{0, 1, 7, 8, 9, 200, 254, 255} {
		buf := make([]byte, StateTreeKeyLengthBytes)
		SetBitBE(buf, d)
		require.Equal(t, byte(1), KeyBitBE(buf, d), "bit %d must read back set", d)
		if d > 0 {
			require.Equal(t, byte(0), KeyBitBE(buf, d-1), "neighbor bit %d must be clear", d-1)
		}
	}
}

// TestClearSuffixBE_BoundaryDepths checks the big-endian suffix mask keeps the
// high (bitLen%8) bits of the boundary byte and zeroes everything at or beyond
// bitLen.
func TestClearSuffixBE_BoundaryDepths(t *testing.T) {
	for _, depth := range []int{0, 1, 7, 8, 200, 255, 256} {
		buf := make([]byte, StateTreeKeyLengthBytes)
		for i := range buf {
			buf[i] = 0xFF
		}
		ClearSuffixBE(buf, depth)
		for d := 0; d < StateTreeKeyLengthBits; d++ {
			want := byte(1)
			if d >= depth {
				want = 0
			}
			require.Equal(t, want, KeyBitBE(buf, d), "depth=%d bit=%d", depth, d)
		}
	}
}
