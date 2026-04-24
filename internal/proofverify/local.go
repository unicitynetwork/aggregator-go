package proofverify

import (
	"bytes"
	"fmt"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// VerifyInclusionProofLocal checks the local, non-certified portion of v2
// proof verification: transaction-hash consistency plus SMT-path verification
// against UC.IR.h.
func VerifyInclusionProofLocal(p *api.InclusionProofV2, req *api.CertificationRequest) error {
	if p == nil {
		return fmt.Errorf("nil inclusion proof")
	}
	if req == nil {
		return fmt.Errorf("nil certification request")
	}
	if p.CertificationData == nil {
		return api.ErrExclusionNotImpl
	}
	if !bytes.Equal(
		p.CertificationData.TransactionHash.DataBytes(),
		req.CertificationData.TransactionHash.DataBytes(),
	) {
		return fmt.Errorf("proof certification data transaction hash does not match request")
	}
	rootRaw, err := p.UCInputRecordHashRaw()
	if err != nil {
		return err
	}
	var cert api.InclusionCert
	if err := cert.UnmarshalBinary(p.CertificateBytes); err != nil {
		return fmt.Errorf("failed to decode inclusion cert: %w", err)
	}
	key, err := req.StateID.GetTreeKey()
	if err != nil {
		return fmt.Errorf("failed to derive SMT key: %w", err)
	}
	return cert.Verify(key, req.CertificationData.TransactionHash.DataBytes(), rootRaw, api.InclusionProofV2HashAlgorithm)
}
