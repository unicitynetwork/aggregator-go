package api

import (
	"encoding/json"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"
)

// CertificationRequest represents the certification_request JSON-RPC request,
// sometimes also referred to as StateTransitionCertificationRequest, Commitment or UnicityServiceRequest.
type CertificationRequest struct {
	_       struct{} `cbor:",toarray"`
	Version types.Version

	// StateID is the unique identifier of the certification request, used as a
	// key in the state tree. In v2 it is the raw 32-byte hash of the CBOR array
	// [CertificationData.OwnerPredicate, CertificationData.SourceStateHash].
	StateID StateID

	// CertificationData contains the necessary cryptographic data needed for the CertificationRequest.
	CertificationData CertificationData

	AggregateRequestCount uint64
}

func (c *CertificationRequest) GetVersion() types.Version {
	if c != nil && c.Version > 0 {
		return c.Version
	}
	return 1
}

func (c *CertificationRequest) MarshalCBOR() ([]byte, error) {
	type alias CertificationRequest
	cp := *c
	if cp.Version == 0 {
		cp.Version = 1
	}
	return types.Cbor.MarshalTaggedValue(CertificationRequestTag, (*alias)(&cp))
}

func (c *CertificationRequest) UnmarshalCBOR(data []byte) error {
	type alias CertificationRequest
	return types.UnmarshalTaggedVersioned(CertificationRequestTag, 1, data, (*alias)(c), c)
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
	Status string `json:"status"`
}

// CertificationData represents the necessary cryptographic data needed for a state transition CertificationRequest.
type CertificationData struct {
	_       struct{}      `cbor:",toarray"`
	Version types.Version `json:"version"`

	// OwnerPredicate is the owner predicate in format: CBOR[engine: uint, code: byte[], params: byte[]].
	//
	// In case of standard PayToPublicKey predicate the values must be:
	//  - engine = 01 (plain CBOR uint value of 1)
	//  - code = 4101 (byte array of length 1 containing the CBOR encoding of uint value 1)
	//  - params = 5821 000102..20 (byte array of length 33 containing the raw bytes of the public key value)
	OwnerPredicate Predicate `json:"ownerPredicate"`

	// SourceStateHash is the raw 32-byte hash of the source data.
	SourceStateHash SourceStateHash `json:"sourceStateHash"`

	// TransactionHash is the raw 32-byte hash of the transaction data.
	TransactionHash TransactionHash `json:"transactionHash"`

	// Witness is the "unlocking part" of owner predicate. In case of PayToPublicKey owner predicate the witness must be
	// a signature created on the hash of CBOR array[SourceStateHashImprint, TransactionHash],
	// in Unicity's [R || S || V] format (65 bytes).
	Witness HexBytes `json:"witness"`
}

func (c *CertificationData) GetVersion() types.Version {
	if c != nil && c.Version > 0 {
		return c.Version
	}
	return 1
}

func (c *CertificationData) MarshalCBOR() ([]byte, error) {
	type alias CertificationData
	cp := *c
	if cp.Version == 0 {
		cp.Version = 1
	}
	return types.Cbor.MarshalTaggedValue(CertificationDataTag, (*alias)(&cp))
}

func (c *CertificationData) UnmarshalCBOR(data []byte) error {
	type alias CertificationData
	return types.UnmarshalTaggedVersioned(CertificationDataTag, 1, data, (*alias)(c), c)
}

// SigDataHash returns the data hash used for signature generation.
// The hash is calculated as the CBOR array [SourceStateHash, TransactionHash].
func (c CertificationData) SigDataHash() (*DataHash, error) {
	return SigDataHash(c.SourceStateHash, c.TransactionHash), nil
}

// SigDataHash returns the data hash used for signature generation.
// The hash is calculated as the CBOR array [sourceStateHash, transactionHash].
func SigDataHash(sourceStateHash []byte, transactionHash []byte) *DataHash {
	return NewDataHasher(SHA256).AddData(
		CborArray(2)).
		AddCborBytes(sourceStateHash).
		AddCborBytes(transactionHash).
		GetHash()
}

// Hash returns the data hash of certification data.
// The hash is calculated as the CBOR array
// [OwnerPredicate, SourceStateHash, TransactionHash, Witness].
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

// CertDataHash returns the data hash of certification data.
// The hash is calculated as the CBOR array
// [OwnerPredicate, SourceStateHash, TransactionHash, Witness].
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
