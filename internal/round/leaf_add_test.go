package round

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// testExpiresAt is far enough ahead of the fixture reference times that
// only the expiry tests reach it.
const testExpiresAt uint64 = 1755003600

func testCommitment(t *testing.T) *models.CertificationRequest {
	t.Helper()
	return &models.CertificationRequest{
		Version: 2,
		StateID: api.RequireNewImprintV2("1111111111111111111111111111111111111111111111111111111111111111"),
		CertificationData: models.CertificationData{
			TransactionHash: api.RequireNewImprintV2("2222222222222222222222222222222222222222222222222222222222222222"),
			ExpiresAt:       ptr(testExpiresAt),
		},
	}
}

// The leaf a round inserts is built from the round's pinned reference time, and
// the commitment records that time so the record and the served proof report
// the value the leaf was actually built from.
func TestMaterializeCommitmentLeafBindsTheRoundReferenceTime(t *testing.T) {
	const referenceTime uint64 = 1755000000
	commitment := testCommitment(t)

	leaf, err := materializeCommitmentLeaf(commitment, referenceTime)
	require.NoError(t, err)

	require.Equal(t, referenceTime, commitment.ReferenceTime)
	require.Equal(t,
		api.LeafValue(commitment.CertificationData.TransactionHash.DataBytes(), referenceTime),
		leaf.Value)
	require.NotEqual(t, commitment.CertificationData.TransactionHash.DataBytes(), leaf.Value)
}

// A different round produces a different leaf for the same request.
func TestMaterializeCommitmentLeafDiffersAcrossRounds(t *testing.T) {
	const referenceTime uint64 = 1755000000

	first, err := materializeCommitmentLeaf(testCommitment(t), referenceTime)
	require.NoError(t, err)
	second, err := materializeCommitmentLeaf(testCommitment(t), referenceTime+1)
	require.NoError(t, err)

	require.Equal(t, first.Key, second.Key)
	require.NotEqual(t, first.Value, second.Value)
}

func TestServiceAssignedDeadlineStillBindsReferenceTime(t *testing.T) {
	const referenceTime uint64 = 1755000000
	commitment := testCommitment(t)
	commitment.CertificationData.ExpiresAt = nil
	commitment.EffectiveTimeout = referenceTime + 3600

	leaf, err := materializeCommitmentLeaf(commitment, referenceTime)
	require.NoError(t, err)
	require.Equal(t, api.LeafValue(commitment.CertificationData.TransactionHash.DataBytes(), referenceTime), leaf.Value)
	require.NotEqual(t, commitment.CertificationData.TransactionHash.DataBytes(), leaf.Value)

	_, err = materializeCommitmentLeaf(commitment, commitment.EffectiveTimeout)
	require.ErrorIs(t, err, ErrRequestExpired)
}

// A request may only be inserted in a round whose reference time is strictly
// below its timeout; an expired one is reported so it can be acked out of the
// queue rather than retried forever.
func TestMaterializeCommitmentLeafRejectsAnExpiredRequest(t *testing.T) {
	commitment := testCommitment(t)

	_, err := materializeCommitmentLeaf(commitment, testExpiresAt-1)
	require.NoError(t, err)

	_, err = materializeCommitmentLeaf(commitment, testExpiresAt)
	require.ErrorIs(t, err, ErrRequestExpired)

	_, err = materializeCommitmentLeaf(commitment, testExpiresAt+1)
	require.ErrorIs(t, err, ErrRequestExpired)
}

// ptr returns a pointer to v, for the optional request deadline.
func ptr(v uint64) *uint64 { return &v }
