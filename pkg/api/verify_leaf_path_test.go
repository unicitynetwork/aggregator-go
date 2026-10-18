package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// VerifyLeafPath is exported for reference verifiers and is fed untrusted, partially decoded proofs:
// malformed input is an error, never a panic.
func TestVerifyLeafPathRejectsMalformedInputs(t *testing.T) {
	root := make([]byte, StateTreeKeyLengthBytes)
	req := &CertificationRequest{}

	var nilProof *InclusionProofV2
	require.NotPanics(t, func() { require.Error(t, nilProof.VerifyLeafPath(req, root)) })

	proof := &InclusionProofV2{CertificationData: &CertificationData{}}
	require.NotPanics(t, func() { require.Error(t, proof.VerifyLeafPath(nil, root)) })

	exclusion := &InclusionProofV2{}
	require.NotPanics(t, func() { require.ErrorIs(t, exclusion.VerifyLeafPath(req, root), ErrExclusionNotImpl) })
}
