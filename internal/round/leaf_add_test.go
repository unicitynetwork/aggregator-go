package round

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// testRequestTimeout is far enough ahead of the fixture reference times that
// only the expiry tests reach it.
const testRequestTimeout uint64 = 1755003600

func testCommitment(t *testing.T) *models.CertificationRequest {
	t.Helper()
	return &models.CertificationRequest{
		Version: 2,
		StateID: api.RequireNewImprintV2("1111111111111111111111111111111111111111111111111111111111111111"),
		CertificationData: models.CertificationData{
			TransactionHash: api.RequireNewImprintV2("2222222222222222222222222222222222222222222222222222222222222222"),
			Timeout:         testRequestTimeout,
		},
	}
}

// The leaf a round inserts is built from the round's pinned reference time, and
// the commitment records that time so the record and the served proof report
// the value the leaf was actually built from.
func TestCommitmentLeafInputBindsTheRoundReferenceTime(t *testing.T) {
	const referenceTime uint64 = 1755000000
	commitment := testCommitment(t)

	leaf, err := commitmentLeafInput(commitment, referenceTime)
	require.NoError(t, err)

	require.Equal(t, referenceTime, commitment.ReferenceTime)
	require.Equal(t,
		api.LeafValue(commitment.CertificationData.TransactionHash.DataBytes(), referenceTime),
		leaf.Value)
	require.NotEqual(t, commitment.CertificationData.TransactionHash.DataBytes(), leaf.Value)
}

// A different round produces a different leaf for the same request.
func TestCommitmentLeafInputDiffersAcrossRounds(t *testing.T) {
	const referenceTime uint64 = 1755000000

	first, err := commitmentLeafInput(testCommitment(t), referenceTime)
	require.NoError(t, err)
	second, err := commitmentLeafInput(testCommitment(t), referenceTime+1)
	require.NoError(t, err)

	require.Equal(t, first.Key, second.Key)
	require.NotEqual(t, first.Value, second.Value)
}

func TestDefaultTimeoutCommitmentStillBindsReferenceTime(t *testing.T) {
	const referenceTime uint64 = 1755000000
	commitment := testCommitment(t)
	commitment.CertificationData.Timeout = 0
	commitment.EffectiveTimeout = referenceTime + 3600

	leaf, err := commitmentLeafInput(commitment, referenceTime)
	require.NoError(t, err)
	require.Equal(t, api.LeafValue(commitment.CertificationData.TransactionHash.DataBytes(), referenceTime), leaf.Value)
	require.NotEqual(t, commitment.CertificationData.TransactionHash.DataBytes(), leaf.Value)

	_, err = commitmentLeafInput(commitment, commitment.EffectiveTimeout)
	require.ErrorIs(t, err, ErrRequestExpired)
}

// A request may only be inserted in a round whose reference time is strictly
// below its timeout; an expired one is reported so it can be acked out of the
// queue rather than retried forever.
func TestCommitmentLeafInputRejectsAnExpiredRequest(t *testing.T) {
	commitment := testCommitment(t)

	_, err := commitmentLeafInput(commitment, testRequestTimeout-1)
	require.NoError(t, err)

	_, err = commitmentLeafInput(commitment, testRequestTimeout)
	require.ErrorIs(t, err, ErrRequestExpired)

	_, err = commitmentLeafInput(commitment, testRequestTimeout+1)
	require.ErrorIs(t, err, ErrRequestExpired)
}
