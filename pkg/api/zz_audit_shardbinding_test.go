package api

import (
	"crypto"
	"testing"

	"github.com/stretchr/testify/require"
	test "github.com/unicitynetwork/bft-go-base/testutils"
	testsig "github.com/unicitynetwork/bft-go-base/testutils/sig"
	"github.com/unicitynetwork/bft-go-base/types"
)

// auditSealNetworkID is the network ID stamped into the UnicitySeal; the trust
// base is always built for types.NetworkMainNet.
var auditSealNetworkID = types.NetworkMainNet

// buildProofCommittedInShard is buildSignedSingleLeafProof, except the caller
// chooses which shard's InputRecord actually carries the single-leaf SMT root.
// That lets us commit a key whose canonical shard is sid0 inside sid1's tree.
func buildProofCommittedInShard(t *testing.T, stateIDHex string, committedIn int) (
	*InclusionProofV2, *CertificationRequest, types.PartitionID, types.RootTrustBase,
	types.ShardID, types.ShardID,
) {
	t.Helper()

	stateID := RequireNewImprintV2(stateIDHex)
	txHash := RequireNewImprintV2("2222222222222222222222222222222222222222222222222222222222222222")

	req := &CertificationRequest{
		StateID:           stateID,
		CertificationData: CertificationData{TransactionHash: txHash},
	}

	key, err := stateID.GetTreeKey()
	require.NoError(t, err)
	const referenceTime uint64 = 1755000000
	hasher := NewDataHasher(InclusionProofV2HashAlgorithm)
	hasher.Reset().
		AddData([]byte{0x00}).
		AddData(key).
		AddData(LeafValue(txHash.DataBytes(), referenceTime))
	leafRoot := append([]byte(nil), hasher.GetHash().RawHash...)

	cert := &InclusionCert{}
	certBytes, err := cert.MarshalBinary()
	require.NoError(t, err)

	sid0, sid1 := types.ShardID{}.Split()
	const partitionID types.PartitionID = 0x0f0f0f0f

	mkIR := func(h []byte, tag byte) *types.InputRecord {
		return &types.InputRecord{
			Version: 1, PreviousHash: []byte{0, 0, tag}, Hash: h,
			BlockHash: []byte{0, 0, tag + 1}, SummaryValue: []byte{0, 0, tag + 2},
			Timestamp: types.NewTimestamp(), RoundNumber: 1,
		}
	}
	var ir0, ir1 *types.InputRecord
	if committedIn == 0 {
		ir0, ir1 = mkIR(leafRoot, 1), mkIR(test.RandomBytes(32), 5)
	} else {
		ir0, ir1 = mkIR(test.RandomBytes(32), 1), mkIR(leafRoot, 5)
	}
	trHash0 := test.RandomBytes(32)
	trHash1 := test.RandomBytes(32)

	sTree, err := types.CreateShardTree(
		types.ShardingScheme{sid0, sid1},
		[]types.ShardTreeInput{
			{Shard: sid0, IR: ir0, TRHash: trHash0},
			{Shard: sid1, IR: ir1, TRHash: trHash1},
		}, crypto.SHA256)
	require.NoError(t, err)

	ownerShard, ownerIR, ownerTR := sid0, ir0, trHash0
	if committedIn == 1 {
		ownerShard, ownerIR, ownerTR = sid1, ir1, trHash1
	}
	stCert, err := sTree.Certificate(ownerShard)
	require.NoError(t, err)

	ut, err := types.NewUnicityTree(crypto.SHA256, []*types.UnicityTreeData{{
		Partition: partitionID, ShardTreeRoot: sTree.RootHash(),
	}})
	require.NoError(t, err)
	utCert, err := ut.Certificate(partitionID)
	require.NoError(t, err)

	signer, verifier := testsig.CreateSignerAndVerifier(t)
	sigKey, err := verifier.MarshalPublicKey()
	require.NoError(t, err)
	tb, err := types.NewTrustBase(types.NetworkMainNet, []*types.NodeInfo{
		{NodeID: "test", SigKey: sigKey, Stake: 1},
	})
	require.NoError(t, err)

	seal := &types.UnicitySeal{
		Version: 1, NetworkID: auditSealNetworkID, RootChainRoundNumber: 1,
		Timestamp: types.NewTimestamp(),
		PreviousHash: test.RandomBytes(32), Hash: ut.RootHash(),
	}
	require.NoError(t, seal.Sign("test", signer))

	ucBytes, err := types.Cbor.Marshal(types.UnicityCertificate{
		Version: 1, InputRecord: ownerIR, TRHash: ownerTR,
		ShardTreeCertificate: stCert, UnicityTreeCertificate: utCert, UnicitySeal: seal,
	})
	require.NoError(t, err)

	certifiedAt := referenceTime
	proof := &InclusionProofV2{
		CertificationData:  &req.CertificationData,
		ReferenceTime:      &certifiedAt,
		CertificateBytes:   certBytes,
		UnicityCertificate: ucBytes,
	}
	return proof, req, partitionID, tb, sid0, sid1
}

// AUDIT: state ID 0x11.. has MSB 0, so f_SH(sid) = shard "0". The leaf is
// nevertheless committed in shard "1"'s SMT and certified by shard "1"'s UC.
// Per platform.tex:459 VerifyInclusionProof MUST reject. Go accepts.
func TestAudit_ForeignShardKeyAccepted(t *testing.T) {
	proof, req, partitionID, tb, sid0, sid1 := buildProofCommittedInShard(
		t, "1111111111111111111111111111111111111111111111111111111111111111", 1)

	key, err := req.StateID.GetTreeKey()
	require.NoError(t, err)
	t.Logf("key[0]=%08b  f_SH(sid)=%q (sid0=%q, sid1=%q)", key[0], sid0.String(), sid0.String(), sid1.String())
	require.True(t, sid0.Comparator()(key), "key must route to shard 0")
	require.False(t, sid1.Comparator()(key), "key must NOT route to shard 1")

	err = proof.Verify(req, &VerifierContext{
		TrustBase:       tb,
		PartitionID:     partitionID,
		ExpectedShardID: sid1, // the shard that served the proof
	})
	t.Logf("Verify() returned: %v", err)
	require.NoError(t, err, "AUDIT: Go verifier accepted a foreign-shard key")
}

// AUDIT: the mandatory H(CD_beta) = UC.C^uni.dhash binding is skippable.
func TestAudit_ShardConfHashOptional(t *testing.T) {
	proof, req, partitionID, tb, sid0, _ := buildProofCommittedInShard(
		t, "1111111111111111111111111111111111111111111111111111111111111111", 0)

	// UC.ShardConfHash was never set (nil) yet verification passes when the
	// verifier context leaves ShardConfHash nil.
	err := proof.Verify(req, &VerifierContext{
		TrustBase:       tb,
		PartitionID:     partitionID,
		ExpectedShardID: sid0,
		ShardConfHash:   nil,
	})
	t.Logf("Verify() with nil ShardConfHash returned: %v", err)
	require.NoError(t, err)
}
