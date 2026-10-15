package backend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Views without a batch path (memory) are served one key at a time with the same results.
func TestInclusionCertsFallsBackPerKeyForMemoryViews(t *testing.T) {
	ctx := context.Background()
	b := newMemory()
	root := commitBatch(t, b, 1, spreadRange(0, 60))
	v, err := b.RetainReadView(ctx)
	require.NoError(t, err)
	defer v.Close()
	_, isBatch := v.(BatchReadView)
	require.False(t, isBatch)

	in := spreadRange(0, 60)
	keys := make([][]byte, 0, len(in))
	for _, l := range in {
		keys = append(keys, l.Key)
	}
	certs, err := InclusionCerts(ctx, v, keys)
	require.NoError(t, err)
	require.Len(t, certs, len(keys))
	for i, l := range in {
		require.NoError(t, certs[i].Verify(l.Key, l.Value, root, api.SHA256))
	}

	// an absent key is a nil entry here too, the others are still built
	certs, err = InclusionCerts(ctx, v, [][]byte{keys[0], spreadInput(9999).Key, keys[1]})
	require.NoError(t, err)
	require.NotNil(t, certs[0])
	require.Nil(t, certs[1])
	require.NotNil(t, certs[2])
}
