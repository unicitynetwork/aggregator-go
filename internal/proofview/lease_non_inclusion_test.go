package proofview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type niReader struct {
	*cReader
	cert *api.NonInclusionCert
	err  error
}

func (r *niReader) GetNonInclusionCert(ctx context.Context, key []byte) (*api.NonInclusionCert, error) {
	return r.cert, r.err
}

func TestLeaseForwardsNonInclusionAndReportsMissingCapability(t *testing.T) {
	ctx := context.Background()
	w := &countingWorld{}
	c, err := NewCatalog(scalingLimits(4), WithAccountant(w))
	require.NoError(t, err)
	defer c.Close()

	// a reader without the capability: a capability failure, never an absence
	pub(t, c, w, 1, 1, 100)
	l, err := c.AcquireLatest(ctx)
	require.NoError(t, err)
	_, err = l.GetNonInclusionCert(make([]byte, 32))
	require.ErrorIs(t, err, backend.ErrNonInclusionUnsupported)
	l.Release()

	// a reader with it: forwarded as is (certificate, ErrStateIncluded, failures)
	root := rootN(2)
	r := &niReader{cReader: w.newReader(root, 100), cert: api.EmptyTreeNonInclusionCert()}
	require.NoError(t, c.Publish(ctx, PublishInput{
		Block: BlockIdentity{Number: api.NewBigIntFromUint64(2), ConfigLineage: []byte("cfg")}, Root: root, Reader: r,
		Cert: Certification{UC: ucFor(t, root, 2)},
	}))
	l, err = c.AcquireLatest(ctx)
	require.NoError(t, err)
	cert, err := l.GetNonInclusionCert(make([]byte, 32))
	require.NoError(t, err)
	require.True(t, cert.IsEmptyTree())
	r.cert, r.err = nil, api.ErrStateIncluded
	_, err = l.GetNonInclusionCert(make([]byte, 32))
	require.ErrorIs(t, err, api.ErrStateIncluded)
	l.Release()
	_, err = l.GetNonInclusionCert(make([]byte, 32))
	require.ErrorIs(t, err, ErrLeaseReleased)
}
