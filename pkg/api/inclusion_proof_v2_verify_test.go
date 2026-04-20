package api

import (
	"crypto"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	test "github.com/unicitynetwork/bft-go-base/testutils"
	testsig "github.com/unicitynetwork/bft-go-base/testutils/sig"
	"github.com/unicitynetwork/bft-go-base/types"
	"github.com/unicitynetwork/bft-go-base/types/hex"
)

// Contract tests for InclusionProofV2.Verify(req, vctx): nil guards, exclusion
// short-circuit, request-integrity checks, UC decoding, the positive path with
// a fully signed UC, and shard-ID mismatch. Local SMT path behaviors live in
// inclusion_cert_test.go.

// stubTrustBase is a RootTrustBase that never returns a valid signature.
type stubTrustBase struct{}

func (stubTrustBase) GetVersion() types.Version     { return 1 }
func (stubTrustBase) GetNetworkID() types.NetworkID { return 0 }
func (stubTrustBase) GetEpoch() uint64              { return 0 }
func (stubTrustBase) GetEpochStart() uint64         { return 0 }
func (stubTrustBase) VerifyQuorumSignatures(data []byte, signatures map[string]hex.Bytes) error {
	return errors.New("stub trust base: quorum sig verification not implemented")
}
func (stubTrustBase) VerifySignature(data []byte, sig []byte, nodeID string) (uint64, error) {
	return 0, errors.New("stub trust base: sig verification not implemented")
}
func (stubTrustBase) GetQuorumThreshold() uint64     { return 0 }
func (stubTrustBase) GetMaxFaultyNodes() uint64      { return 0 }
func (stubTrustBase) GetRootNodes() []*types.NodeInfo { return nil }

func newStubVctx() *VerifierContext {
	return &VerifierContext{
		TrustBase:       stubTrustBase{},
		PartitionID:     0,
		ExpectedShardID: types.ShardID{},
	}
}

func TestInclusionProofV2Verify_NilProof(t *testing.T) {
	var p *InclusionProofV2
	err := p.Verify(&CertificationRequest{}, newStubVctx())
	require.Error(t, err)
	require.Equal(t, "nil inclusion proof", err.Error())
}

func TestInclusionProofV2Verify_NilRequest(t *testing.T) {
	p := &InclusionProofV2{}
	err := p.Verify(nil, newStubVctx())
	require.Error(t, err)
	require.Equal(t, "nil certification request", err.Error())
}

func TestInclusionProofV2Verify_NilVerifierContext(t *testing.T) {
	p := &InclusionProofV2{}
	err := p.Verify(&CertificationRequest{}, nil)
	require.Error(t, err)
	require.Equal(t, "nil verifier context", err.Error())
}

func TestInclusionProofV2Verify_NilTrustBase(t *testing.T) {
	p := &InclusionProofV2{}
	err := p.Verify(&CertificationRequest{}, &VerifierContext{})
	require.Error(t, err)
	require.Equal(t, "nil trust base", err.Error())
}

// TestInclusionProofV2Verify_NonInclusionShortCircuits confirms that when
// CertificationData is nil the Verify method short-circuits with
// ErrExclusionNotImpl, before attempting to decode the certificate or UC.
// The stub trust base is never consulted.
func TestInclusionProofV2Verify_NonInclusionShortCircuits(t *testing.T) {
	stateID := RequireNewImprintV2("1111111111111111111111111111111111111111111111111111111111111111")
	req := &CertificationRequest{StateID: stateID}

	proof := &InclusionProofV2{
		CertificationData:  nil,
		CertificateBytes:   nil, // intentionally invalid — must not be touched
		UnicityCertificate: nil, // intentionally invalid — must not be touched
	}

	err := proof.Verify(req, newStubVctx())
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrExclusionNotImpl))
}

// TestInclusionProofV2Verify_MissingRequestTxHash checks that malformed outer
// requests fail fast with a clear error instead of relying on deeper cert
// verification.
func TestInclusionProofV2Verify_MissingRequestTxHash(t *testing.T) {
	stateID := RequireNewImprintV2("1111111111111111111111111111111111111111111111111111111111111111")
	txHash := RequireNewImprintV2("2222222222222222222222222222222222222222222222222222222222222222")

	// A well-formed proof envelope (cert bytes + UC.IR.h placeholder). Values
	// here do not need to form a real tree — the test asserts that the outer
	// request validation fires before cert decoding.
	cert := &InclusionCert{}
	certBytes, err := cert.MarshalBinary()
	require.NoError(t, err)
	ucBytes, err := types.Cbor.Marshal(types.UnicityCertificate{
		InputRecord: &types.InputRecord{Hash: make([]byte, StateTreeKeyLengthBytes)},
	})
	require.NoError(t, err)

	proof := &InclusionProofV2{
		CertificationData: &CertificationData{
			TransactionHash: txHash,
		},
		CertificateBytes:   certBytes,
		UnicityCertificate: ucBytes,
	}

	req := &CertificationRequest{
		StateID: stateID,
		CertificationData: CertificationData{
			TransactionHash: nil,
		},
	}

	err = proof.Verify(req, newStubVctx())
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing certification request transaction hash")
}

// TestInclusionProofV2Verify_MismatchedProofTxHashFails ensures the proof
// payload cannot carry a different tx hash than the outer request while still
// verifying against the request's leaf value.
func TestInclusionProofV2Verify_MismatchedProofTxHashFails(t *testing.T) {
	stateID := RequireNewImprintV2("1111111111111111111111111111111111111111111111111111111111111111")
	reqTxHash := RequireNewImprintV2("2222222222222222222222222222222222222222222222222222222222222222")
	proofTxHash := RequireNewImprintV2("3333333333333333333333333333333333333333333333333333333333333333")

	req := &CertificationRequest{
		StateID: stateID,
		CertificationData: CertificationData{
			TransactionHash: reqTxHash,
		},
	}

	cert := &InclusionCert{}
	certBytes, err := cert.MarshalBinary()
	require.NoError(t, err)
	ucBytes, err := types.Cbor.Marshal(types.UnicityCertificate{
		InputRecord: &types.InputRecord{Hash: make([]byte, StateTreeKeyLengthBytes)},
	})
	require.NoError(t, err)

	proof := &InclusionProofV2{
		CertificationData: &CertificationData{
			TransactionHash: proofTxHash,
		},
		CertificateBytes:   certBytes,
		UnicityCertificate: ucBytes,
	}

	err = proof.Verify(req, newStubVctx())
	require.Error(t, err)
	require.Contains(t, err.Error(), "proof certification data transaction hash does not match")
}

// TestInclusionProofV2Verify_RejectsInvalidUCInputRecordHash confirms that v2
// requires UC.IR.h to be exactly 32 bytes.
func TestInclusionProofV2Verify_RejectsInvalidUCInputRecordHash(t *testing.T) {
	stateID := RequireNewImprintV2("1111111111111111111111111111111111111111111111111111111111111111")
	txHash := RequireNewImprintV2("2222222222222222222222222222222222222222222222222222222222222222")
	req := &CertificationRequest{
		StateID: stateID,
		CertificationData: CertificationData{
			TransactionHash: txHash,
		},
	}

	legacyRoot := make([]byte, SiblingSize+2)

	cert := &InclusionCert{}
	certBytes, err := cert.MarshalBinary()
	require.NoError(t, err)

	ucBytes, err := types.Cbor.Marshal(types.UnicityCertificate{
		InputRecord: &types.InputRecord{Hash: legacyRoot},
	})
	require.NoError(t, err)

	proof := &InclusionProofV2{
		CertificationData:  &req.CertificationData,
		CertificateBytes:   certBytes,
		UnicityCertificate: ucBytes,
	}

	err = proof.Verify(req, newStubVctx())
	require.Error(t, err)
	require.Contains(t, err.Error(), "UC.IR.h length")
}

// buildSignedSingleLeafProof constructs a fully signed v2 inclusion proof
// with a two-shard partition (1-bit split: sid0 and sid1) where the commitment
// lives in ownerShard. Returns the proof, the matching CertificationRequest,
// the partition ID, and the RootTrustBase to verify against.
//
// The SMT is a single-leaf tree whose root is H(0x00 || key || tx hash); that
// same root is placed into InputRecord.Hash and flows through the shard tree
// and unicity tree verbatim. No aggregator plumbing is involved — this builds
// the exact cryptographic objects that a real deployment would emit.
func buildSignedSingleLeafProof(t *testing.T, ownerShard types.ShardID) (
	*InclusionProofV2,
	*CertificationRequest,
	types.PartitionID,
	types.RootTrustBase,
) {
	t.Helper()

	// A deterministic 32-byte state ID and tx hash. Content is irrelevant
	// — Verify checks the cryptographic chain, not the values.
	stateID := RequireNewImprintV2("1111111111111111111111111111111111111111111111111111111111111111")
	txHash := RequireNewImprintV2("2222222222222222222222222222222222222222222222222222222222222222")

	req := &CertificationRequest{
		StateID: stateID,
		CertificationData: CertificationData{
			TransactionHash: txHash,
		},
	}

	// Single-leaf root: H(0x00 || key || value) under the v2 hash algorithm.
	key, err := stateID.GetTreeKey()
	require.NoError(t, err)
	hasher := NewDataHasher(InclusionProofV2HashAlgorithm)
	hasher.Reset().
		AddData([]byte{0x00}).
		AddData(key).
		AddData(txHash.DataBytes())
	leafRoot := append([]byte(nil), hasher.GetHash().RawHash...)

	// Empty InclusionCert — single-leaf edge case, no siblings.
	cert := &InclusionCert{}
	certBytes, err := cert.MarshalBinary()
	require.NoError(t, err)

	// Two-shard partition via a 1-bit split of the empty ShardID.
	sid0, sid1 := types.ShardID{}.Split()

	const partitionID types.PartitionID = 0x0f0f0f0f
	ir0 := &types.InputRecord{
		Version:         1,
		PreviousHash:    []byte{0, 0, 1},
		Hash:            leafRoot,
		BlockHash:       []byte{0, 0, 3},
		SummaryValue:    []byte{0, 0, 4},
		Timestamp:       types.NewTimestamp(),
		RoundNumber:     1,
		Epoch:           0,
		SumOfEarnedFees: 0,
	}
	// Shard 1 needs its own (non-degenerate) IR so the shard tree is
	// well-formed; its hash is irrelevant to this test.
	ir1Hash := test.RandomBytes(32)
	ir1 := &types.InputRecord{
		Version:         1,
		PreviousHash:    []byte{0, 0, 5},
		Hash:            ir1Hash,
		BlockHash:       []byte{0, 0, 6},
		SummaryValue:    []byte{0, 0, 7},
		Timestamp:       types.NewTimestamp(),
		RoundNumber:     1,
		Epoch:           0,
		SumOfEarnedFees: 0,
	}
	trHash0 := test.RandomBytes(32)
	trHash1 := test.RandomBytes(32)

	sTree, err := types.CreateShardTree(
		types.ShardingScheme{sid0, sid1},
		[]types.ShardTreeInput{
			{Shard: sid0, IR: ir0, TRHash: trHash0, ShardConfHash: nil},
			{Shard: sid1, IR: ir1, TRHash: trHash1, ShardConfHash: nil},
		},
		crypto.SHA256)
	require.NoError(t, err)

	ownerIR := ir0
	ownerTR := trHash0
	if ownerShard.Equal(sid1) {
		ownerIR = ir1
		ownerTR = trHash1
	}

	stCert, err := sTree.Certificate(ownerShard)
	require.NoError(t, err)

	ut, err := types.NewUnicityTree(crypto.SHA256, []*types.UnicityTreeData{{
		Partition:     partitionID,
		ShardTreeRoot: sTree.RootHash(),
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
		Version:              1,
		RootChainRoundNumber: 1,
		Timestamp:            types.NewTimestamp(),
		PreviousHash:         test.RandomBytes(32),
		Hash:                 ut.RootHash(),
	}
	require.NoError(t, seal.Sign("test", signer))

	uc := types.UnicityCertificate{
		Version:                1,
		InputRecord:            ownerIR,
		TRHash:                 ownerTR,
		ShardConfHash:          nil,
		ShardTreeCertificate:   stCert,
		UnicityTreeCertificate: utCert,
		UnicitySeal:            seal,
	}
	ucBytes, err := types.Cbor.Marshal(uc)
	require.NoError(t, err)

	proof := &InclusionProofV2{
		CertificationData:  &req.CertificationData,
		CertificateBytes:   certBytes,
		UnicityCertificate: ucBytes,
	}
	return proof, req, partitionID, tb
}

// A fully signed UC over a two-shard partition verifies end-to-end.
func TestInclusionProofV2Verify_HappyPath_FullySignedUC(t *testing.T) {
	sid0, _ := types.ShardID{}.Split()

	proof, req, partitionID, tb := buildSignedSingleLeafProof(t, sid0)

	vctx := &VerifierContext{
		TrustBase:       tb,
		PartitionID:     partitionID,
		ExpectedShardID: sid0,
		ShardConfHash:   nil,
	}
	require.NoError(t, proof.Verify(req, vctx))
}

// A valid UC for one shard must be rejected when the verifier expects another.
func TestInclusionProofV2Verify_ShardMismatch_Rejected(t *testing.T) {
	sid0, sid1 := types.ShardID{}.Split()

	proof, req, partitionID, tb := buildSignedSingleLeafProof(t, sid0)

	vctx := &VerifierContext{
		TrustBase:       tb,
		PartitionID:     partitionID,
		ExpectedShardID: sid1,
		ShardConfHash:   nil,
	}
	err := proof.Verify(req, vctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match expected shard")
}
