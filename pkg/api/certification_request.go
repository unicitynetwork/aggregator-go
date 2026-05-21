package api

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"

	corecbor "github.com/unicitynetwork/aggregator-go/pkg/cbor"
)

// ErrCertificationRequestNotCanonical is returned by
// UnmarshalCertificationRequestCBOR when the input is not encoded in canonical
// Core Deterministic CBOR form.
var ErrCertificationRequestNotCanonical = corecbor.ErrNotCanonical

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
	return UnmarshalCertificationRequestCBOR(hb, c)
}

// UnmarshalCertificationRequestCBOR decodes data into out. The CBOR payload
// must be canonical Core Deterministic CBOR; non-canonical inputs are rejected
// so the same logical request cannot be expressed by multiple distinct byte
// sequences (see issue #150).
func UnmarshalCertificationRequestCBOR(data []byte, out *CertificationRequest) error {
	if out == nil {
		return errors.New("nil CertificationRequest output")
	}
	if err := corecbor.ValidateCoreDeterministic(data); err != nil {
		return err
	}
	if err := types.Cbor.Unmarshal(data, out); err != nil {
		return err
	}
	if out.Version != 1 {
		return fmt.Errorf("unsupported CertificationRequest version: %d", out.Version)
	}
	if out.CertificationData.Version != 1 {
		return fmt.Errorf("unsupported CertificationData version: %d", out.CertificationData.Version)
	}
	return nil
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
	// a signature created on the hash of CBOR array[SourceStateHash, TransactionHash],
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
	if len(c.SourceStateHash) != StateTreeKeyLengthBytes {
		return nil, fmt.Errorf("invalid source state hash length: expected %d bytes, got %d", StateTreeKeyLengthBytes, len(c.SourceStateHash))
	}
	if len(c.TransactionHash) != StateTreeKeyLengthBytes {
		return nil, fmt.Errorf("invalid transaction hash length: expected %d bytes, got %d", StateTreeKeyLengthBytes, len(c.TransactionHash))
	}
	return SigDataHash(c.SourceStateHash, c.TransactionHash), nil
}

// SigDataHash returns the data hash used for signature generation.
// The hash is calculated as the CBOR array [sourceStateHash, transactionHash].
func SigDataHash(sourceStateHash []byte, transactionHash []byte) *DataHash {
	// Reverted to manual construction to avoid unnecessary marshal error path.
	// CborArray(2) and AddCborBytes produce shortest-form canonical headers.
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
	if len(sourceStateHash) != StateTreeKeyLengthBytes {
		return nil, fmt.Errorf("invalid source state hash length: expected %d bytes, got %d", StateTreeKeyLengthBytes, len(sourceStateHash))
	}
	if len(transactionHash) != StateTreeKeyLengthBytes {
		return nil, fmt.Errorf("invalid transaction hash length: expected %d bytes, got %d", StateTreeKeyLengthBytes, len(transactionHash))
	}

	// We use an explicit struct to ensure canonical Core Deterministic CBOR
	// encoding of the array and its elements. This also ensures no extra data
	// is included in the hash computation.
	type certDataInput struct {
		_               struct{} `cbor:",toarray"`
		OwnerPredicate  Predicate
		SourceStateHash []byte
		TransactionHash []byte
		Witness         []byte
	}

	input := certDataInput{
		OwnerPredicate:  ownerPredicate,
		SourceStateHash: sourceStateHash,
		TransactionHash: transactionHash,
		Witness:         signature,
	}

	inputBytes, err := types.Cbor.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal certification data inputs: %w", err)
	}

	return NewDataHasher(SHA256).AddData(inputBytes).GetHash(), nil
}
