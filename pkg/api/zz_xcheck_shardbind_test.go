package api

import (
	"crypto"
	"testing"

	"github.com/stretchr/testify/require"
	test "github.com/unicitynetwork/bft-go-base/testutils"
	testsig "github.com/unicitynetwork/bft-go-base/testutils/sig"
	"github.com/unicitynetwork/bft-go-base/types"
)

// xcheckBuild builds a fully signed v2 inclusion proof for a two-shard
// partition (SH = {"0","1"}) in which the leaf for `stateIDHex` is committed
// into `committingShard`'s SMT -- regardless of which shard f_SH(sid) actually
// names. netID is written into the seal; tbNet into the trust base.
//
// Independent of buildSignedSingleLeafProof in inclusion_proof_v2_verify_test.go:
// that helper always puts the leaf root in shard 0's IR, so it cannot express a
// foreign-shard commitment.
func xcheckBuild(t *testing.T, stateIDHex string, committingShard types.ShardID,
	netID types.NetworkID, tbNet types.NetworkID) (
	*InclusionProofV2, *CertificationRequest, types.PartitionID, types.RootTrustBase) {
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

	// Single-leaf RSMT root: H(0x00 || key || v).
	h := NewDataHasher(InclusionProofV2HashAlgorithm)
	h.Reset().
		AddData([]byte{0x00}).
		AddData(key).
		AddData(LeafValue(txHash.DataBytes(), referenceTime))
	leafRoot := append([]byte(nil), h.GetHash().RawHash...)

	cert := &InclusionCert{} // single leaf, no siblings
	certBytes, err := cert.MarshalBinary()
	require.NoError(t, err)

	sid0, sid1 := types.ShardID{}.Split()
	const partitionID types.PartitionID = 0x0f0f0f0f

	mkIR := func(hash []byte, salt byte) *types.InputRecord {
		return &types.InputRecord{
			Version: 1, PreviousHash: []byte{0, 0, salt}, Hash: hash,
			BlockHash: []byte{0, 0, salt + 1}, SummaryValue: []byte{0, 0, salt + 2},
			Timestamp: types.NewTimestamp(), RoundNumber: 1, Epoch: 0,
		}
	}
	// The committing shard's IR carries the leaf root; the other shard is filler.
	ir0, ir1 := mkIR(test.RandomBytes(32), 1), mkIR(test.RandomBytes(32), 5)
	if committingShard.Equal(sid0) {
		ir0 = mkIR(leafRoot, 1)
	} else {
		ir1 = mkIR(leafRoot, 5)
	}
	trHash0, trHash1 := test.RandomBytes(32), test.RandomBytes(32)

	sTree, err := types.CreateShardTree(
		types.ShardingScheme{sid0, sid1},
		[]types.ShardTreeInput{
			{Shard: sid0, IR: ir0, TRHash: trHash0},
			{Shard: sid1, IR: ir1, TRHash: trHash1},
		}, crypto.SHA256)
	require.NoError(t, err)

	ownerIR, ownerTR := ir0, trHash0
	if committingShard.Equal(sid1) {
		ownerIR, ownerTR = ir1, trHash1
	}
	stCert, err := sTree.Certificate(committingShard)
	require.NoError(t, err)

	ut, err := types.NewUnicityTree(crypto.SHA256, []*types.UnicityTreeData{
		{Partition: partitionID, ShardTreeRoot: sTree.RootHash()},
	})
	require.NoError(t, err)
	utCert, err := ut.Certificate(partitionID)
	require.NoError(t, err)

	signer, verifier := testsig.CreateSignerAndVerifier(t)
	sigKey, err := verifier.MarshalPublicKey()
	require.NoError(t, err)
	tb, err := types.NewTrustBase(tbNet, []*types.NodeInfo{{NodeID: "n1", SigKey: sigKey, Stake: 1}})
	require.NoError(t, err)

	seal := &types.UnicitySeal{
		Version: 1, NetworkID: netID, RootChainRoundNumber: 1,
		Timestamp: types.NewTimestamp(), PreviousHash: test.RandomBytes(32),
		Hash: ut.RootHash(),
	}
	require.NoError(t, seal.Sign("n1", signer))

	ucBytes, err := types.Cbor.Marshal(types.UnicityCertificate{
		Version: 1, InputRecord: ownerIR, TRHash: ownerTR,
		ShardTreeCertificate: stCert, UnicityTreeCertificate: utCert, UnicitySeal: seal,
	})
	require.NoError(t, err)

	rt := referenceTime
	return &InclusionProofV2{
		CertificationData:  &req.CertificationData,
		ReferenceTime:      &rt,
		CertificateBytes:   certBytes,
		UnicityCertificate: ucBytes,
	}, req, partitionID, tb
}

// Establish the routing fact first: sid 0x1111... routes to shard "0".
func TestXCheck_RoutingFact(t *testing.T) {
	sid0, sid1 := types.ShardID{}.Split()
	key, err := RequireNewImprintV2(
		"1111111111111111111111111111111111111111111111111111111111111111").GetTreeKey()
	require.NoError(t, err)
	require.True(t, sid0.Comparator()(key), "f_SH(sid) must be shard 0")
	require.False(t, sid1.Comparator()(key), "sid must NOT route to shard 1")
	t.Logf("key[0]=%#02x -> f_SH = shard %q (not %q)", key[0], sid0, sid1)
}

// THE CLAIM: a leaf whose f_SH(sid) = "0" but which shard "1" committed and
// certified under a fully valid UC is accepted by InclusionProofV2.Verify.
func TestXCheck_ForeignShardLeafAccepted(t *testing.T) {
	sid0, sid1 := types.ShardID{}.Split()
	proof, req, pid, tb := xcheckBuild(t,
		"1111111111111111111111111111111111111111111111111111111111111111", // f_SH = sid0
		sid1, // but committed + certified by shard 1
		types.NetworkMainNet, types.NetworkMainNet)

	// Caller derives ExpectedShardID from the serving endpoint / the proof's own
	// UC -- the in-repo pattern -- i.e. sid1.
	err := proof.Verify(req, &VerifierContext{
		TrustBase: tb, PartitionID: pid, ExpectedShardID: sid1,
	})
	t.Logf("Verify(ExpectedShardID=sid1) for a sid0-routed key => %v", err)
	require.NoError(t, err, "spec ensure(f_SH(sid)=sigma) would have rejected this")

	// Control: same construction with the key committed in its correct shard.
	p2, r2, pid2, tb2 := xcheckBuild(t,
		"1111111111111111111111111111111111111111111111111111111111111111",
		sid0, types.NetworkMainNet, types.NetworkMainNet)
	require.NoError(t, p2.Verify(r2, &VerifierContext{
		TrustBase: tb2, PartitionID: pid2, ExpectedShardID: sid0,
	}))
}

// Control the other way: the ONLY shard check is equality with the caller's
// value, so passing the true responsible shard rejects the honest proof too.
func TestXCheck_OnlyCheckIsCallerEquality(t *testing.T) {
	sid0, sid1 := types.ShardID{}.Split()
	proof, req, pid, tb := xcheckBuild(t,
		"1111111111111111111111111111111111111111111111111111111111111111",
		sid1, types.NetworkMainNet, types.NetworkMainNet)
	err := proof.Verify(req, &VerifierContext{
		TrustBase: tb, PartitionID: pid, ExpectedShardID: sid0,
	})
	require.ErrorContains(t, err, "invalid shard ID")
	t.Logf("ExpectedShardID=sid0 => %v (equality with caller value, not f_SH)", err)
}

// UC.C^r.alpha = T.alpha is unchecked: a testnet-sealed UC verifies against a
// mainnet trust base.
func TestXCheck_NetworkIDUnchecked(t *testing.T) {
	sid0, _ := types.ShardID{}.Split()
	proof, req, pid, tb := xcheckBuild(t,
		"1111111111111111111111111111111111111111111111111111111111111111",
		sid0, types.NetworkTestNet, types.NetworkMainNet)
	require.EqualValues(t, types.NetworkMainNet, tb.GetNetworkID())

	var uc types.UnicityCertificate
	require.NoError(t, types.Cbor.Unmarshal(proof.UnicityCertificate, &uc))
	require.EqualValues(t, types.NetworkTestNet, uc.UnicitySeal.NetworkID)

	err := proof.Verify(req, &VerifierContext{
		TrustBase: tb, PartitionID: pid, ExpectedShardID: sid0,
	})
	t.Logf("seal.NetworkID=%d vs trustBase.NetworkID=%d => Verify: %v",
		uc.UnicitySeal.NetworkID, tb.GetNetworkID(), err)
	require.NoError(t, err, "spec ensure(UC.C^r.alpha = T.alpha) would have rejected this")
}

// ShardConfHash: the UC carries one, the verifier context does not, and the
// mismatch is simply not looked at.
func TestXCheck_ShardConfHashSkippable(t *testing.T) {
	sid0, _ := types.ShardID{}.Split()
	proof, req, pid, tb := xcheckBuild(t,
		"1111111111111111111111111111111111111111111111111111111111111111",
		sid0, types.NetworkMainNet, types.NetworkMainNet)
	require.NoError(t, proof.Verify(req, &VerifierContext{
		TrustBase: tb, PartitionID: pid, ExpectedShardID: sid0, ShardConfHash: nil,
	}))
	// Supplying any non-nil value does get compared -- so the check exists but
	// is opt-in, and no production caller opts in.
	err := proof.Verify(req, &VerifierContext{
		TrustBase: tb, PartitionID: pid, ExpectedShardID: sid0,
		ShardConfHash: []byte{0xAA},
	})
	require.ErrorContains(t, err, "invalid shard configuration hash")
	t.Logf("nil ShardConfHash: accepted; non-nil: %v", err)
}
