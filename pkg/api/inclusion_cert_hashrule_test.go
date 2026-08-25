package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The hash rules in docs/inclusion-proof-wire.md and README.md are what an
// independent client implements. This builds a root from the DOCUMENTED formula
// -- spelled out here rather than reusing the production helpers -- and requires
// InclusionCert.Verify to accept it.
//
// Both documents previously omitted region(key, depth) from the inner-node
// preimage, which reproduces the correct root only for a proof with no siblings.
// TestDocumentedInnerNodeRuleRequiresRegion below pins that the region is
// load-bearing, so dropping it again fails here rather than in a foreign client.
func TestDocumentedHashRulesReproduceTheRoot(t *testing.T) {
	const algo = InclusionProofV2HashAlgorithm

	key := make([]byte, StateTreeKeyLengthBytes)
	key[0] = 0x80 // bit 0 set under MSB-first addressing => descent goes right at depth 0
	value := LeafValue(RequireNewImprintV2(
		"2222222222222222222222222222222222222222222222222222222222222222").DataBytes(), 1755000000)

	// Leaf: H(0x00 || key || value)
	leaf := NewDataHasher(algo).Reset().
		AddData([]byte{0x00}).
		AddData(key).
		AddData(value).
		GetHash().RawHash

	sibling := make([]byte, SiblingSize)
	for i := range sibling {
		sibling[i] = 0xAB
	}

	// Inner node at depth 0, two children:
	//   H(0x01 || depth_byte || region(key, depth) || left || right)
	// bit(key, 0) == 1, so descent went right and the sibling is the LEFT child.
	region := make([]byte, StateTreeKeyLengthBytes) // depth 0 => 32 zero bytes
	root := NewDataHasher(algo).Reset().
		AddData([]byte{0x01, byte(0)}).
		AddData(region).
		AddData(sibling).
		AddData(leaf).
		GetHash().RawHash

	cert := &InclusionCert{Siblings: [][SiblingSize]byte{{}}}
	copy(cert.Siblings[0][:], sibling)
	SetBitBE(cert.Bitmap[:], 0) // one binary node, at depth 0

	require.NoError(t, cert.Verify(key, value, root, algo),
		"the documented hash rules must reproduce the root the verifier computes")
}

// region(key, depth) is load-bearing: a root built without it is rejected.
// A doc that omits it would mislead an independent implementer into computing
// a different root for every proof that carries a sibling.
func TestDocumentedInnerNodeRuleRequiresRegion(t *testing.T) {
	const algo = InclusionProofV2HashAlgorithm
	const depth = 8 // a depth where the region is non-zero

	key := make([]byte, StateTreeKeyLengthBytes)
	key[0] = 0xFF
	key[1] = 0x80 // bit 8 set => descent goes right at depth 8
	value := make([]byte, SiblingSize)

	leaf := NewDataHasher(algo).Reset().
		AddData([]byte{0x00}).AddData(key).AddData(value).GetHash().RawHash

	sibling := make([]byte, SiblingSize)
	for i := range sibling {
		sibling[i] = 0xCD
	}

	region := RegionFromKeyBytes(key, depth)
	require.NotEqual(t, make([]byte, StateTreeKeyLengthBytes), region,
		"pick a depth where the region is non-zero or this test proves nothing")

	withRegion := NewDataHasher(algo).Reset().
		AddData([]byte{0x01, byte(depth)}).AddData(region).
		AddData(sibling).AddData(leaf).GetHash().RawHash

	withoutRegion := NewDataHasher(algo).Reset().
		AddData([]byte{0x01, byte(depth)}).
		AddData(sibling).AddData(leaf).GetHash().RawHash

	require.NotEqual(t, withRegion, withoutRegion)

	cert := &InclusionCert{Siblings: [][SiblingSize]byte{{}}}
	copy(cert.Siblings[0][:], sibling)
	SetBitBE(cert.Bitmap[:], depth)

	require.NoError(t, cert.Verify(key, value, withRegion, algo))
	require.ErrorIs(t, cert.Verify(key, value, withoutRegion, algo), ErrCertRootMismatch,
		"a root computed without the region must be rejected")
}
