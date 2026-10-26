package proofview

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestHasLatestSupersedingCertification(t *testing.T) {
	c := newCatalog(t, testLimits())
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	root := rootOf(1)
	original := compatUC(t, root, 1, 0, nil, nil, nil, 1)
	require.NoError(t, c.Publish(t.Context(), PublishInput{
		Block: block(1), Root: root, Reader: newFakeReader(1, 100), Cert: Certification{UC: original},
	}))
	for round := uint64(2); round <= 6; round++ {
		require.NoError(t, c.AttachCertification(t.Context(), block(1), root,
			Certification{UC: compatUC(t, root, 1, 0, nil, nil, nil, round)}))
	}
	for _, tc := range []struct {
		name string
		uc   []byte
		want bool
	}{
		{"evicted original", original, true},
		{"retained repeat", compatUC(t, root, 1, 0, nil, nil, nil, 4), true},
		{"future certification", compatUC(t, root, 1, 0, nil, nil, nil, 7), false},
		{"other accepted round", compatUC(t, root, 2, 0, nil, nil, nil, 1), false},
		{"other epoch", compatUC(t, root, 1, 1, nil, nil, nil, 1), false},
		{"other block hash", compatUC(t, root, 1, 0, []byte("other"), nil, nil, 1), false},
		{"other configuration", compatUC(t, root, 1, 0, nil, nil, []byte("other"), 1), false},
		{"other root", compatUC(t, rootOf(2), 1, 0, nil, nil, nil, 1), false},
		{"malformed", []byte{0}, false},
		{"missing", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, c.HasLatest(block(1).Number, root, tc.uc))
		})
	}
	require.False(t, c.HasLatest(nil, root, original))
	require.False(t, c.HasLatest(block(2).Number, root, original))
	c.MarkLatestUnavailable(block(1).Number, 6)
	require.False(t, c.HasLatest(block(1).Number, root, original))
	require.NoError(t, c.Publish(t.Context(), PublishInput{
		Block: block(1), Root: root, Reader: newFakeReader(1, 100), Cert: Certification{UC: original},
	}))
	lease, err := c.AcquireLatest(t.Context())
	require.NoError(t, err)
	defer lease.Release()
	require.Equal(t, compatUC(t, root, 1, 0, nil, nil, nil, 6), lease.UC())
	require.True(t, c.HasLatest(block(1).Number, root, original))
	require.Equal(t, 4, attachmentCount(c), "the evicted original is not reattached")
}

func TestRejectedCertificationWhileUnavailableKeepsWatermark(t *testing.T) {
	limits := testLimits()
	limits.RetentionBudgetBytes = 1000
	c := newCatalog(t, limits)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	reader := newFakeReader(1, 100)
	publish(t, c, 1, 1, reader, nil)
	c.MarkLatestUnavailable(block(1).Number, 1)
	reader.bytes.Store(2000)
	rejected := compatUC(t, rootOf(1), 1, 0, nil, nil, nil, 2)
	require.ErrorIs(t, c.AttachCertification(t.Context(), block(1), rootOf(1),
		Certification{UC: rejected}), ErrBudget)
	reader.bytes.Store(100)
	c.MarkLatestUnavailable(block(1).Number, 1)
	publish(t, c, 1, 1, newFakeReader(1, 100), nil)
	_, err := c.AcquireLatest(t.Context())
	require.ErrorIs(t, err, ErrViewNotRetained)
	require.NoError(t, c.Publish(t.Context(), PublishInput{
		Block: block(1), Root: rootOf(1), Reader: newFakeReader(1, 100), Cert: Certification{UC: rejected},
	}))
	lease, err := c.AcquireLatest(t.Context())
	require.NoError(t, err)
	defer lease.Release()
	require.Equal(t, rejected, lease.UC())
}

func TestUnavailableHeadCannotBeRestoredByOlderPublication(t *testing.T) {
	for _, retained := range []bool{true, false} {
		name := "retained"
		if !retained {
			name = "evicted"
		}
		t.Run(name, func(t *testing.T) {
			limits := testLimits()
			limits.MaxViews = 1
			c := newCatalog(t, limits)
			t.Cleanup(func() { require.NoError(t, c.Close()) })
			publish(t, c, 1, 1, newFakeReader(1, 100), nil)
			if !retained {
				publish(t, c, 2, 2, newFakeReader(2, 100), nil)
			}
			number := api.NewBigIntFromUint64(3)
			c.MarkLatestUnavailable(number, 3)
			number.SetUint64(1)
			c.MarkLatestUnavailable(number, 1)
			err := c.Publish(t.Context(), PublishInput{
				Block: block(1), Root: rootOf(1), Reader: newFakeReader(1, 100),
				Cert: Certification{UC: ucFor(t, rootOf(1), 1)},
			})
			if retained {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrInvalidPublication)
			}
			_, err = c.AcquireLatest(t.Context())
			require.ErrorIs(t, err, ErrViewNotRetained)
			publish(t, c, 3, 3, newFakeReader(3, 100), nil)
			lease, err := c.AcquireLatest(t.Context())
			require.NoError(t, err)
			defer lease.Release()
			require.EqualValues(t, 3, lease.Block().Number.Uint64())
		})
	}
}
