package cbor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateCoreDeterministic_MapKeyOrdering(t *testing.T) {
	require.NoError(t, ValidateCoreDeterministic([]byte{
		0xa2,
		0x01, 0x61, 'a',
		0x02, 0x61, 'b',
	}))

	err := ValidateCoreDeterministic([]byte{
		0xa2,
		0x02, 0x61, 'b',
		0x01, 0x61, 'a',
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotCanonical)

	err = ValidateCoreDeterministic([]byte{
		0xa2,
		0x01, 0x61, 'a',
		0x01, 0x61, 'b',
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotCanonical)
}

func TestValidateCoreDeterministic_RejectsReservedSimpleValue(t *testing.T) {
	err := ValidateCoreDeterministic([]byte{0xf8, 0x18})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotCanonical)
	require.Contains(t, err.Error(), "reserved or non-shortest simple value encoding")
}

func FuzzValidateCoreDeterministic(f *testing.F) {
	f.Add([]byte{0x01})
	f.Add([]byte{})
	f.Add([]byte{0xff})
	f.Add([]byte{0x9f, 0xff})
	f.Add([]byte{0xa2, 0x02, 0x61, 'b', 0x01, 0x61, 'a'})

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = ValidateCoreDeterministic(data)
	})
}
