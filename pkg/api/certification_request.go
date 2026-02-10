package api

import (
	"encoding/json"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"
)

// CertificationRequest represents the certification_request JSON-RPC request,
// sometimes also referred to as StateTransitionCertificationRequest, Commitment or UnicityServiceRequest.
type CertificationRequest struct {
	_ struct{} `cbor:",toarray"`

	// StateID is the unique identifier of the certification request, used as a key in the state tree.
	// Calculated as hash of CBOR array [CertificationData.OwnerPredicate, CertificationData.SourceStateHashImprint],
	// prefixed by two bytes that define the hashing algorithm (two zero bytes in case of SHA2_256).
	StateID StateID

	// CertificationData contains the necessary cryptographic data needed for the CertificationRequest.
	CertificationData CertificationData

	// Receipt optional flag that if set to true includes the receipt data in the CertificationResponse.
	Receipt bool

	AggregateRequestCount uint64
}

// MarshalJSON marshals the request to CBOR and then hex encodes it, returning the result as a JSON string.
func (c *CertificationRequest) MarshalJSON() ([]byte, error) {
	cborBytes, err := types.Cbor.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal to CBOR: %w", err)
	}
	return HexBytes(cborBytes).MarshalJSON()
}

// UnmarshalJSON expects a hex-encoded CBOR string, decodes it, and then unmarshals the CBOR data.
func (c *CertificationRequest) UnmarshalJSON(data []byte) error {
	var hb HexBytes
	if err := json.Unmarshal(data, &hb); err != nil {
		return fmt.Errorf("failed to unmarshal JSON to HexBytes: %w", err)
	}
	return types.Cbor.Unmarshal(hb, c)
}

// CertificationResponse represents the certification_request JSON-RPC response.
type CertificationResponse struct {
	Status  string     `json:"status"`
	Receipt *ReceiptV2 `json:"receipt,omitempty"`
}

// CertificationData represents the necessary cryptographic data needed for a state transition CertificationRequest.
type CertificationData struct {
	_ struct{} `cbor:",toarray"`

	// OwnerPredicate is the owner predicate in format: CBOR[engine: uint, code: byte[], params: byte[]].
	//
	// In case of standard PayToPublicKey predicate the values must be:
	//  - engine = 01 (plain CBOR uint value of 1)
	//  - code = 4101 (byte array of length 1 containing the CBOR encoding of uint value 1)
	//  - params = 5821 000102..20 (byte array of length 33 containing the raw bytes of the public key value)
	OwnerPredicate Predicate `json:"ownerPredicate"`

	// SourceStateHash is the source data (token) hash,
	// prefixed by two bytes that define the hashing algorithm (two zero bytes in case of SHA2_256).
	SourceStateHash SourceStateHash `json:"sourceStateHash"`

	// TransactionHash is the entire transaction data hash (including the source data),
	// prefixed by two bytes that define the hashing algorithm (two zero bytes in case of SHA2_256).
	TransactionHash TransactionHash `json:"transactionHash"`

	// Witness is the "unlocking part" of owner predicate. In case of PayToPublicKey owner predicate the witness must be
	// a signature created on the hash of CBOR array[SourceStateHashImprint, TransactionHash],
	// in Unicity's [R || S || V] format (65 bytes).
	Witness HexBytes `json:"witness"`
}

// SigDataHash returns the data hash used for signature generation.
// The hash is calculated as CBOR array of [sourceStateHashImprint, transactionHashImprint].
func (c CertificationData) SigDataHash() (*DataHash, error) {
	return SigDataHash(c.SourceStateHash, c.TransactionHash), nil
}

// SigDataHash returns the data hash used for signature generation.
// The hash is calculated as CBOR array of [sourceStateHashImprint, transactionHashImprint].
func SigDataHash(sourceStateHash []byte, transactionHash []byte) *DataHash {
	return NewDataHasher(SHA256).AddData(
		CborArray(2)).
		AddCborBytes(sourceStateHash).
		AddCborBytes(transactionHash).
		GetHash()
}

// Hash returns the data hash of certification data, used as a key in the state tree.
// The hash is calculated as CBOR array of [OwnerPredicate, SourceStateHashImprint, TransactionHashImprint, Witness] and
// the value returned is in DataHash imprint format (2-byte algorithm prefix + hash of cbor array).
func (c CertificationData) Hash() ([]byte, error) {
	dataHash, err := CertDataHash(c.OwnerPredicate, c.SourceStateHash, c.TransactionHash, c.Witness)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate certification data hash: %w", err)
	}
	return dataHash.GetImprint(), nil
}

func (c CertificationData) CreateStateID() (StateID, error) {
	return CreateStateID(c.OwnerPredicate, c.SourceStateHash)
}

// CertDataHash returns the data hash of certification data, used as a key in the state tree.
// The hash is calculated as CBOR array of [OwnerPredicate, SourceStateHashImprint, TransactionHashImprint, Witness].
func CertDataHash(ownerPredicate Predicate, sourceStateHash, transactionHash, signature []byte) (*DataHash, error) {
	predicateBytes, err := types.Cbor.Marshal(ownerPredicate)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal owner predicate: %w", err)
	}

	return NewDataHasher(SHA256).AddData(
		CborArray(4)).
		AddData(predicateBytes).
		AddCborBytes(sourceStateHash).
		AddCborBytes(transactionHash).
		AddCborBytes(signature).
		GetHash(), nil
}
