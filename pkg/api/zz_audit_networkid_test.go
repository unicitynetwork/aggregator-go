package api

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
)

// AUDIT: platform.tex:456 requires ensure(UC.C^r.alpha = T.alpha). The seal here
// claims NetworkTestNet while the trust base is NetworkMainNet; the same root
// key signs both, so only an explicit network-ID comparison can catch it.
func TestAudit_NetworkIDNotChecked(t *testing.T) {
	orig := auditSealNetworkID
	auditSealNetworkID = types.NetworkTestNet
	defer func() { auditSealNetworkID = orig }()

	proof, req, partitionID, tb, sid0, _ := buildProofCommittedInShard(
		t, "1111111111111111111111111111111111111111111111111111111111111111", 0)
	require.Equal(t, types.NetworkMainNet, tb.GetNetworkID())

	var uc types.UnicityCertificate
	require.NoError(t, types.Cbor.Unmarshal(proof.UnicityCertificate, &uc))
	t.Logf("seal.NetworkID=%d  trustbase.NetworkID=%d", uc.UnicitySeal.NetworkID, tb.GetNetworkID())

	err := proof.Verify(req, &VerifierContext{
		TrustBase: tb, PartitionID: partitionID, ExpectedShardID: sid0,
	})
	t.Logf("Verify() returned: %v", err)
	require.NoError(t, err, "AUDIT: cross-network UC accepted")
}
