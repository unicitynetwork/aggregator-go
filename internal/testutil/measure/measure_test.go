package measure

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSlope(t *testing.T) {
	xs := []float64{1, 2, 4, 8, 16, 32}
	lin, quad, flat := make([]float64, len(xs)), make([]float64, len(xs)), make([]float64, len(xs))
	for i, x := range xs {
		lin[i], quad[i], flat[i] = 7*x, 3*x*x, 5
	}
	require.InDelta(t, 1, Slope(xs, lin), 1e-9)
	require.InDelta(t, 2, Slope(xs, quad), 1e-9)
	require.InDelta(t, 0, Slope(xs, flat), 1e-9)
	require.Zero(t, Slope(xs, make([]float64, len(xs))))
}

func TestNewCertificationIsNativelyVerified(t *testing.T) {
	root := h32("root")
	for _, shards := range []int{1, 4, 8, 16} {
		c := NewCertification(t, shards, 4, root)
		require.NotEmpty(t, c.UC)
		if shards <= 8 {
			require.NotEmpty(t, c.View)
		} else {
			require.Nil(t, c.View)
		}
	}
}
