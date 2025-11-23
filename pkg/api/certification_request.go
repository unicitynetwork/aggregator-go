package api

import (
	"crypto/sha256"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"
)

// CertificationRequest represents the certification_request JSON-RPC request,
// sometimes also referred to as StateTransitionCertificationRequest, Commitment or UnicityServiceRequest.
type CertificationRequest struct {
	// StateID is the unique identifier of the certification request, used as a key in the state tree.
	// Calculated as hash of CBOR array [CertificationData.SourceStateHashImprint, CertificationData.PublicKey],
	// prefixed by two bytes that define the hashing algorithm (two zero bytes in case of SHA2_256).
	StateID StateID `json:"stateId"`

	// CertificationData contains the necessary cryptographic data needed for the CertificationRequest.
	CertificationData CertificationData `json:"certificationData"`

	// Receipt optional flag that if set to true includes the receipt data in the CertificationResponse.
	Receipt *bool `json:"receipt,omitempty"`

	AggregateRequestCount uint64 `json:"aggregateRequestCount,omitempty,string"`
}

// CertificationResponse represents the certification_request JSON-RPC response.
type CertificationResponse struct {
	Status  string   `json:"status"`
	Receipt *Receipt `json:"receipt,omitempty"`
}

// CertificationData represents the necessary cryptographic data needed for a state transition CertificationRequest.
type CertificationData struct {
	// The public key of the keypair used to generate the Signature.
	// Must be in compressed secp256k1 format (33 bytes, parsed using btcec.ParsePubKey function).
	PublicKey HexBytes `json:"publicKey"`

	// SourceStateHash is the source data (token) hash,
	// prefixed by two bytes that define the hashing algorithm (two zero bytes in case of SHA2_256).
	SourceStateHash SourceStateHash `json:"sourceStateHash"`

	// TransactionHash is the entire transaction data hash (including the source data),
	// prefixed by two bytes that define the hashing algorithm (two zero bytes in case of SHA2_256).
	TransactionHash TransactionHash `json:"transactionHash"`

	// Signature is the signature created on the hash of CBOR array[SourceStateHashImprint, TransactionHash].
	// Must be in Unicity's [R || S || V] format (65 bytes).
	Signature HexBytes `json:"signature"`
}

// SigBytes returns the signature data bytes for signature generation.
func (c CertificationData) SigBytes() ([]byte, error) {
	sourceStateHashImprint, err := c.SourceStateHash.Imprint()
	if err != nil {
		return nil, fmt.Errorf("failed to convert source state hash to bytes: %w", err)
	}
	transactionHashImprint, err := c.TransactionHash.Imprint()
	if err != nil {
		return nil, fmt.Errorf("failed to convert transaction hash to bytes: %w", err)
	}
	sigData := SigHashData{
		SourceStateHashImprint: sourceStateHashImprint,
		TransactionHashImprint: transactionHashImprint,
	}
	sigDataCBOR, err := types.Cbor.Marshal(sigData)
	if err != nil {
		return nil, fmt.Errorf("error serializing signature data to cbor: %w", err)
	}
	sigBytes := sha256.Sum256(sigDataCBOR)
	return sigBytes[:], nil
}
