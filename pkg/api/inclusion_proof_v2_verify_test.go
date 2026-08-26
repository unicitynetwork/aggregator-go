package api

import (
	"bytes"
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
func (stubTrustBase) GetQuorumThreshold() uint64      { return 0 }
func (stubTrustBase) GetMaxFaultyNodes() uint64       { return 0 }
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

	oversizedRoot := make([]byte, SiblingSize+2)

	cert := &InclusionCert{}
	certBytes, err := cert.MarshalBinary()
	require.NoError(t, err)

	ucBytes, err := types.Cbor.Marshal(types.UnicityCertificate{
		InputRecord: &types.InputRecord{Hash: oversizedRoot},
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
func buildSignedSingleLeafProof(t *testing.T, ownerShard types.ShardID, sealNetwork types.NetworkID) (
	*InclusionProofV2,
	*CertificationRequest,
	types.PartitionID,
	types.RootTrustBase,
) {
	t.Helper()

	txHash := RequireNewImprintV2("2222222222222222222222222222222222222222222222222222222222222222")
	certData := CertificationData{
		Version: CertificationDataVersion,
		OwnerPredicate: Predicate{
			Engine: 1,
			Code:   []byte{1},
			Params: bytes.Repeat([]byte{0x02}, 33),
		},
		SourceStateHash: bytes.Repeat([]byte{0x33}, StateTreeKeyLengthBytes),
		TransactionHash: txHash,
		Witness:         bytes.Repeat([]byte{0x44}, 65),
	}
	var (
		stateID StateID
		err     error
	)
	for candidate := 0; candidate < 256; candidate++ {
		certData.SourceStateHash[len(certData.SourceStateHash)-1] = byte(candidate)
		stateID, err = certData.CreateStateID()
		require.NoError(t, err)
		if stateID.DataBytes()[0]&0x80 == 0 {
			break
		}
	}
	// This fixture intentionally routes to shard 0. Passing ownerShard=shard 1
	// creates a fully signed wrong-shard proof for the regression test below.
	require.Zero(t, stateID.DataBytes()[0]&0x80)

	req := &CertificationRequest{
		StateID:           stateID,
		CertificationData: certData,
	}

	// Single-leaf root: H(0x00 || key || value) under the v2 hash algorithm,
	// where the leaf value binds the round's reference time.
	key, err := stateID.GetTreeKey()
	require.NoError(t, err)
	const referenceTime uint64 = 1755000000
	hasher := NewDataHasher(InclusionProofV2HashAlgorithm)
	hasher.Reset().
		AddData([]byte{0x00}).
		AddData(key).
		AddData(LeafValue(txHash.DataBytes(), referenceTime))
	leafRoot := append([]byte(nil), hasher.GetHash().RawHash...)

	// Empty InclusionCert — single-leaf edge case, no siblings.
	cert := &InclusionCert{}
	certBytes, err := cert.MarshalBinary()
	require.NoError(t, err)

	// Two-shard partition via a 1-bit split of the empty ShardID.
	sid0, sid1 := types.ShardID{}.Split()

	const partitionID types.PartitionID = 0x0f0f0f0f
	ir0Hash := test.RandomBytes(32)
	ir1Hash := test.RandomBytes(32)
	if ownerShard.Equal(sid0) {
		ir0Hash = leafRoot
	} else if ownerShard.Equal(sid1) {
		ir1Hash = leafRoot
	} else {
		t.Fatalf("owner shard %s is outside the fixture's two-shard scheme", ownerShard)
	}
	ir0 := &types.InputRecord{
		Version:         1,
		PreviousHash:    []byte{0, 0, 1},
		Hash:            ir0Hash,
		BlockHash:       []byte{0, 0, 3},
		SummaryValue:    []byte{0, 0, 4},
		Timestamp:       types.NewTimestamp(),
		RoundNumber:     1,
		Epoch:           0,
		SumOfEarnedFees: 0,
	}
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
		NetworkID:            sealNetwork,
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

	certifiedAt := referenceTime
	proof := &InclusionProofV2{
		CertificationData:  &req.CertificationData,
		ReferenceTime:      &certifiedAt,
		CertificateBytes:   certBytes,
		UnicityCertificate: ucBytes,
	}
	return proof, req, partitionID, tb
}

// A fully signed UC over a two-shard partition verifies end-to-end.
func TestInclusionProofV2Verify_HappyPath_FullySignedUC(t *testing.T) {
	sid0, _ := types.ShardID{}.Split()

	proof, req, partitionID, tb := buildSignedSingleLeafProof(t, sid0, types.NetworkMainNet)

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

	proof, req, partitionID, tb := buildSignedSingleLeafProof(t, sid0, types.NetworkMainNet)

	vctx := &VerifierContext{
		TrustBase:       tb,
		PartitionID:     partitionID,
		ExpectedShardID: sid1,
		ShardConfHash:   nil,
	}
	err := proof.Verify(req, vctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid shard ID")
}

// A validly signed shard-1 UC does not certify a state ID whose first bit
// routes it to shard 0, even when the caller also says it expects shard 1.
func TestInclusionProofV2Verify_StateIDMustBelongToCertifiedShard(t *testing.T) {
	_, sid1 := types.ShardID{}.Split()
	proof, req, partitionID, tb := buildSignedSingleLeafProof(t, sid1, types.NetworkMainNet)

	err := proof.Verify(req, &VerifierContext{
		TrustBase:       tb,
		PartitionID:     partitionID,
		ExpectedShardID: sid1,
	})
	require.EqualError(t, err, "stateId does not belong to certified shard")
}

// The seal is signed by a key accepted by the trust base, but its network is
// different. Signature validity must not substitute for network binding.
func TestInclusionProofV2Verify_SealNetworkMustMatchTrustBase(t *testing.T) {
	sid0, _ := types.ShardID{}.Split()
	proof, req, partitionID, tb := buildSignedSingleLeafProof(t, sid0, types.NetworkLocal)

	err := proof.Verify(req, &VerifierContext{
		TrustBase:       tb,
		PartitionID:     partitionID,
		ExpectedShardID: sid0,
	})
	require.EqualError(t, err, "unicity seal network does not match trust base")
}

func TestInclusionProofV2Verify_StateIDMustMatchCertificationData(t *testing.T) {
	sid0, _ := types.ShardID{}.Split()
	proof, req, partitionID, tb := buildSignedSingleLeafProof(t, sid0, types.NetworkMainNet)
	req.StateID = RequireNewImprintV2("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")

	err := proof.Verify(req, &VerifierContext{
		TrustBase:       tb,
		PartitionID:     partitionID,
		ExpectedShardID: sid0,
	})
	require.EqualError(t, err, "stateId does not match certification data")
}

func TestInclusionProofV2Verify_CertificationDataMustMatchRequest(t *testing.T) {
	sid0, _ := types.ShardID{}.Split()
	tests := []struct {
		name    string
		mutate  func(*CertificationData)
		errText string
	}{
		{
			name: "owner predicate",
			mutate: func(cd *CertificationData) {
				cd.OwnerPredicate.Params = append([]byte(nil), cd.OwnerPredicate.Params...)
				cd.OwnerPredicate.Params[0] ^= 0xff
			},
			errText: "proof certification data owner predicate does not match certification request owner predicate",
		},
		{
			name: "source state hash",
			mutate: func(cd *CertificationData) {
				cd.SourceStateHash = append([]byte(nil), cd.SourceStateHash...)
				cd.SourceStateHash[0] ^= 0xff
			},
			errText: "proof certification data source state hash does not match certification request source state hash",
		},
		{
			name: "witness",
			mutate: func(cd *CertificationData) {
				cd.Witness = append([]byte(nil), cd.Witness...)
				cd.Witness[0] ^= 0xff
			},
			errText: "proof certification data witness does not match certification request witness",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proof, req, partitionID, tb := buildSignedSingleLeafProof(t, sid0, types.NetworkMainNet)
			detached := *proof.CertificationData
			proof.CertificationData = &detached
			tt.mutate(proof.CertificationData)

			err := proof.Verify(req, &VerifierContext{
				TrustBase:       tb,
				PartitionID:     partitionID,
				ExpectedShardID: sid0,
			})
			require.EqualError(t, err, tt.errText)
		})
	}
}

// The request deadline is exclusive: a leaf created at exactly the deadline is
// expired, one created a second earlier is not. The deadline does not enter the
// leaf value, so the cryptographic chain is unaffected either way and this
// isolates the boundary itself.
func TestInclusionProofV2Verify_ExpiryBoundaryIsExclusive(t *testing.T) {
	sid0, _ := types.ShardID{}.Split()

	tests := []struct {
		name      string
		expiresAt func(referenceTime uint64) uint64
		accept    bool
	}{
		{"deadline one past the reference time", func(rt uint64) uint64 { return rt + 1 }, true},
		{"deadline equal to the reference time", func(rt uint64) uint64 { return rt }, false},
		{"deadline before the reference time", func(rt uint64) uint64 { return rt - 1 }, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proof, req, partitionID, tb := buildSignedSingleLeafProof(t, sid0, types.NetworkMainNet)
			require.NotNil(t, proof.ReferenceTime)

			// Both copies must agree or Verify rejects on the equality check
			// before it ever reaches the deadline comparison.
			deadline := tt.expiresAt(*proof.ReferenceTime)
			proof.CertificationData.ExpiresAt = &deadline
			req.CertificationData.ExpiresAt = &deadline

			err := proof.Verify(req, &VerifierContext{
				TrustBase:       tb,
				PartitionID:     partitionID,
				ExpectedShardID: sid0,
			})
			if tt.accept {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "expired")
		})
	}
}

// A deadline present on one side and absent on the other is a mismatch, not a
// silent pass: absence is a value in its own right, and zero is a legal instant.
//
// buildSignedSingleLeafProof aliases the proof's certification data to the
// request's, so the two must be separated before they can disagree at all.
func TestInclusionProofV2Verify_ExpiryPresenceMustMatch(t *testing.T) {
	sid0, _ := types.ShardID{}.Split()

	for _, tt := range []struct {
		name               string
		onProof, onRequest *uint64
	}{
		{"absent on the proof, present on the request", nil, Uint64Ptr(1755003600)},
		{"present on the proof, absent on the request", Uint64Ptr(1755003600), nil},
		{"present on both but different", Uint64Ptr(1755003600), Uint64Ptr(1755003601)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			proof, req, partitionID, tb := buildSignedSingleLeafProof(t, sid0, types.NetworkMainNet)

			detached := *proof.CertificationData
			proof.CertificationData = &detached
			proof.CertificationData.ExpiresAt = tt.onProof
			req.CertificationData.ExpiresAt = tt.onRequest

			err := proof.Verify(req, &VerifierContext{
				TrustBase:       tb,
				PartitionID:     partitionID,
				ExpectedShardID: sid0,
			})
			require.ErrorContains(t, err, "expiry")
		})
	}
}
