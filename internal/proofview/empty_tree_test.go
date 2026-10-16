package proofview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type emptyStateReader struct{ *cReader }

func (emptyStateReader) IsEmptyState() bool { return true }

func emptyUC(t testing.TB, round uint64) []byte { return ucFor(t, nil, round) }

func TestCertifiedEmptyTreeIdentity(t *testing.T) {
	ctx := context.Background()
	w := &countingWorld{}
	c, err := NewCatalog(scalingLimits(8), WithAccountant(w))
	require.NoError(t, err)
	defer c.Close()
	id := BlockIdentity{Number: api.NewBigIntFromUint64(0), ConfigLineage: []byte("cfg")}
	reader := func() *emptyStateReader { return &emptyStateReader{w.newReader(make([]byte, 32), 10)} }

	// the empty identity needs the explicit flag, an empty root and a reader which declares the empty state
	require.ErrorIs(t, c.Publish(ctx, PublishInput{Block: id, Root: nil, Reader: reader(), Cert: Certification{UC: emptyUC(t, 1)}}), ErrInvalidPublication,
		"an empty root without the flag")
	require.ErrorIs(t, c.Publish(ctx, PublishInput{Block: id, Root: make([]byte, 32), Reader: reader(), Cert: Certification{UC: emptyUC(t, 1)}, EmptyTree: true}), ErrInvalidPublication,
		"the flag with a root: 32 zero bytes are not the empty tree")
	require.ErrorIs(t, c.Publish(ctx, PublishInput{Block: id, Reader: w.newReader(make([]byte, 32), 10), Cert: Certification{UC: emptyUC(t, 1)}, EmptyTree: true}), ErrInvalidPublication,
		"a reader without the explicit empty-state fact")
	require.ErrorIs(t, c.Publish(ctx, PublishInput{Block: id, Reader: reader(), Cert: Certification{UC: ucFor(t, rootN(1), 1)}, EmptyTree: true}), ErrInvalidPublication,
		"a UC certifying a root cannot be attached to the empty identity")
	require.Equal(t, 0, c.Stats().Views)
	checkInvariants(t, c)

	require.NoError(t, c.Publish(ctx, PublishInput{Block: id, Reader: reader(), Cert: Certification{UC: emptyUC(t, 1)}, EmptyTree: true}))
	l, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.True(t, l.EmptyTree())
	require.Empty(t, l.Root())
	l.Release()
	checkInvariants(t, c)

	// a later UC of the same state attaches; a UC of another root does not
	require.NoError(t, c.AttachCertification(ctx, id, nil, Certification{UC: emptyUC(t, 2)}))
	require.ErrorIs(t, c.AttachCertification(ctx, id, nil, Certification{UC: ucFor(t, rootN(1), 3)}), ErrInvalidPublication)
	l, err = c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.Equal(t, emptyUC(t, 2), l.UC())
	l.Release()
	checkInvariants(t, c)

	// an exact root never selects it
	for _, r := range [][]byte{nil, {}, make([]byte, 32)} {
		_, err = c.AcquireRoot(ctx, r)
		require.ErrorIs(t, err, ErrViewNotRetained)
	}
	// a real block published later is latest; the empty snapshot is older history
	require.NotNil(t, pub(t, c, w, 1, 5, 10))
	l, err = c.AcquireLatest(ctx)
	require.NoError(t, err)
	require.False(t, l.EmptyTree())
	l.Release()
	checkInvariants(t, c)
}
