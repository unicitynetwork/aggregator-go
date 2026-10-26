package noninclusion_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/noninclusion"
	"github.com/unicitynetwork/bft-go-base/types"
)

func TestVerifier_TypedNilAuthority(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/uc-consolidation/v1/shards-1/ucs/root.cbor")
	require.NoError(t, err)
	v, err := noninclusion.NewVerifier(noninclusion.Context{
		Resolver: func(*types.UnicitySeal) (types.RootTrustBase, error) {
			return (*types.RootTrustBaseV1)(nil), nil
		},
	})
	require.NoError(t, err)
	result, err := v.Verify(make(api.StateID, 32), &api.NonInclusionProofV1{
		Certificate: api.EmptyTreeNonInclusionCert(), UnicityCertificate: raw,
	})
	require.ErrorIs(t, err, types.ErrRootValidatorInfoMissing)
	require.Equal(t, noninclusion.RejectionAuthority, noninclusion.RejectionOf(err))
	require.Nil(t, result)
}
