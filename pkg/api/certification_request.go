package api

import (
	"fmt"
)

// CertificationRequest represents the certification_request JSON-RPC request,
// sometimes also referred to as StateTransitionCertificationRequest, Commitment or UnicityServiceRequest.
type CertificationRequest struct {
	// StateID is the unique identifier of the certification request, used as a key in the state tree.
	// Calculated as hash of CBOR array [CertificationData.PublicKey, CertificationData.SourceStateHashImprint],
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
	// OwnerPredicate is the owner predicate in format: CBOR[engine: uint, code: byte[], params: byte[]]
	OwnerPredicate HexBytes `json:"ownerPredicate"`

	// SourceStateHash is the source data (token) hash,
	// prefixed by two bytes that define the hashing algorithm (two zero bytes in case of SHA2_256).
	SourceStateHash SourceStateHash `json:"sourceStateHash"`

	// TransactionHash is the entire transaction data hash (including the source data),
	// prefixed by two bytes that define the hashing algorithm (two zero bytes in case of SHA2_256).
	TransactionHash TransactionHash `json:"transactionHash"`

	// Witness is the signature created on the hash of CBOR array[SourceStateHashImprint, TransactionHash].
	// Must be in Unicity's [R || S || V] format (65 bytes).
	Witness HexBytes `json:"witness"`
}

// SigDataHash returns the data hash used for signature generation.
// The hash is calculated as CBOR array of [sourceStateHashImprint, transactionHashImprint].
func (c CertificationData) SigDataHash() (*DataHash, error) {
	sourceStateHashImprint, err := c.SourceStateHash.Imprint()
	if err != nil {
		return nil, fmt.Errorf("failed to convert source state hash to bytes: %w", err)
	}
	transactionHashImprint, err := c.TransactionHash.Imprint()
	if err != nil {
		return nil, fmt.Errorf("failed to convert transaction hash to bytes: %w", err)
	}
	return SigDataHash(sourceStateHashImprint, transactionHashImprint), nil
}

// SigDataHash returns the data hash used for signature generation.
// The hash is calculated as CBOR array of [sourceStateHashImprint, transactionHashImprint].
func SigDataHash(sourceStateHashImprint []byte, transactionHashImprint []byte) *DataHash {
	return NewDataHasher(SHA256).AddData(
		CborArray(2)).
		AddCborBytes(sourceStateHashImprint).
		AddCborBytes(transactionHashImprint).
		GetHash()
}

// Hash returns the data hash of certification data, used as a key in the state tree.
// The hash is calculated as CBOR array of [OwnerPredicate, SourceStateHashImprint, TransactionHashImprint, Witness] and
// the value returned is in DataHash imprint format (2-byte algorithm prefix + hash of cbor array).
func (c CertificationData) Hash() ([]byte, error) {
	sourceStateHashImprint, err := c.SourceStateHash.Imprint()
	if err != nil {
		return nil, fmt.Errorf("failed to convert source state hash to bytes: %w", err)
	}
	transactionHashImprint, err := c.TransactionHash.Imprint()
	if err != nil {
		return nil, fmt.Errorf("failed to convert transaction hash to bytes: %w", err)
	}
	return CertDataHash(c.OwnerPredicate, sourceStateHashImprint, transactionHashImprint, c.Witness).GetImprint(), nil
}

// CertDataHash returns the data hash of certification data, used as a key in the state tree.
// The hash is calculated as CBOR array of [PublicKey, SourceStateHashImprint, TransactionHashImprint, Witness].
func CertDataHash(publicKey, sourceStateHashImprint, transactionHashImprint, signature []byte) *DataHash {
	return NewDataHasher(SHA256).AddData(
		CborArray(4)).
		AddCborBytes(publicKey).
		AddCborBytes(sourceStateHashImprint).
		AddCborBytes(transactionHashImprint).
		AddCborBytes(signature).
		GetHash()
}
