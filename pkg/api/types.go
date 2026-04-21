// Package api provides public JSON-RPC request and response types for the Unicity Aggregator.
// These types can be imported and used by clients to interact with the aggregator service.
package api

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"
)

type ShardID = int

// Timestamp wraps time.Time for consistent JSON serialization
type Timestamp struct {
	time.Time
}

// NewTimestamp creates a new Timestamp
func NewTimestamp(t time.Time) *Timestamp {
	return &Timestamp{Time: t}
}

// Now creates a Timestamp for current time
func Now() *Timestamp {
	return &Timestamp{Time: time.Now()}
}

// MarshalJSON implements json.Marshaler
func (t *Timestamp) MarshalJSON() ([]byte, error) {
	// Use Unix timestamp in milliseconds as string
	millis := t.Time.UnixMilli()
	return json.Marshal(strconv.FormatInt(millis, 10))
}

// UnmarshalJSON implements json.Unmarshaler
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	millis, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp: %w", err)
	}

	t.Time = time.UnixMilli(millis)
	return nil
}

// AggregatorRecord represents a finalized certification request with proof data
type AggregatorRecord struct {
	StateID               StateID           `json:"stateId"`
	CertificationData     CertificationData `json:"certificationData"`
	AggregateRequestCount uint64            `json:"aggregateRequestCount,omitempty,string"`
	BlockNumber           *BigInt           `json:"blockNumber"`
	LeafIndex             *BigInt           `json:"leafIndex"`
	CreatedAt             *Timestamp        `json:"createdAt"`
	FinalizedAt           *Timestamp        `json:"finalizedAt"`
}

// Block represents a blockchain block
type Block struct {
	Index               *BigInt                  `json:"index"`
	ChainID             string                   `json:"chainId"`
	ShardID             ShardID                  `json:"shardId"`
	Version             string                   `json:"version"`
	ForkID              string                   `json:"forkId"`
	RootHash            HexBytes                 `json:"rootHash"`
	PreviousBlockHash   HexBytes                 `json:"previousBlockHash"`
	NoDeletionProofHash HexBytes                 `json:"noDeletionProofHash"`
	CreatedAt           *Timestamp               `json:"createdAt"`
	UnicityCertificate  HexBytes                 `json:"unicityCertificate"`
	ParentFragment      *ParentInclusionFragment `json:"parentFragment,omitempty"`    // child mode only
	ParentBlockNumber   uint64                   `json:"parentBlockNumber,omitempty"` // child mode only
}

// NoDeletionProof represents a no-deletion proof
type NoDeletionProof struct {
	Proof     HexBytes   `json:"proof"`
	CreatedAt *Timestamp `json:"createdAt"`
}

// NewNoDeletionProof creates a new no-deletion proof
func NewNoDeletionProof(proof HexBytes) *NoDeletionProof {
	return &NoDeletionProof{
		Proof:     proof,
		CreatedAt: Now(),
	}
}

// JSON-RPC Request and Response Types

// GetInclusionProofRequestV2 represents the get_inclusion_proof JSON-RPC request
type GetInclusionProofRequestV2 struct {
	StateID StateID `json:"stateId"`
}

// GetInclusionProofResponseV2 represents the get_inclusion_proof JSON-RPC response
type GetInclusionProofResponseV2 struct {
	_              struct{}          `cbor:",toarray"`
	BlockNumber    uint64            `json:"blockNumber"`
	InclusionProof *InclusionProofV2 `json:"inclusionProof"`
}

// InclusionProofV2 is the canonical v2 inclusion proof payload.
//
// Wire form: CBOR tag InclusionProofTag wrapping a 4-element toarray:
//
//	#InclusionProofTag ([
//	  version: uint,
//	  certificationDataOrNull,
//	  certificateBytes: bstr,   // InclusionCert or ExclusionCert raw wire form
//	  unicityCertificate: raw CBOR
//	])
//
// Discriminator:
//   - CertificationData != nil → inclusion. CertificateBytes is an
//     InclusionCert wire payload. The SMT key comes from the outer RPC
//     request (stateId); the leaf value is CertificationData.TransactionHash.
//   - CertificationData == nil → non-inclusion. CertificateBytes is an
//     ExclusionCert wire payload. Non-inclusion verification is not yet
//     implemented in Go.
//
// The expected SMT root is ALWAYS taken from UC.IR.h (input record hash
// of the embedded Unicity Certificate). No root field appears here.
//
// See docs/inclusion-proof-wire.md for the frozen specification.
type InclusionProofV2 struct {
	_                  struct{}           `cbor:",toarray"`
	Version            types.Version      `json:"version"`
	CertificationData  *CertificationData `json:"certificationData"`
	CertificateBytes   HexBytes           `json:"certificateBytes"`
	UnicityCertificate types.RawCBOR      `json:"unicityCertificate"`
}

func (p *InclusionProofV2) GetVersion() types.Version {
	if p != nil && p.Version > 0 {
		return p.Version
	}
	return 1
}

func (p *InclusionProofV2) MarshalCBOR() ([]byte, error) {
	type alias InclusionProofV2
	cp := *p
	if cp.Version == 0 {
		cp.Version = 1
	}
	return types.Cbor.MarshalTaggedValue(InclusionProofTag, (*alias)(&cp))
}

func (p *InclusionProofV2) UnmarshalCBOR(data []byte) error {
	type alias InclusionProofV2
	return types.UnmarshalTaggedVersioned(InclusionProofTag, 1, data, (*alias)(p), p)
}

// ParentInclusionFragment is the internal parent-tree proof fragment stored on
// finalized child blocks and returned by get_shard_proof. CertificateBytes
// uses the same bitmap+sibling wire shape as InclusionCert; ShardLeafValue is
// the parent leaf value proven by that fragment and must equal the child SMT
// root before later composition.
type ParentInclusionFragment struct {
	CertificateBytes HexBytes `json:"certificateBytes"`
	ShardLeafValue   HexBytes `json:"shardLeafValue"`
}

func (f *ParentInclusionFragment) Verify(shardID ShardID, keyLength int, expectedLeafValue, expectedRoot []byte, algo HashAlgorithm) error {
	if f == nil {
		return errors.New("missing parent fragment")
	}
	if len(f.ShardLeafValue) != SiblingSize {
		return fmt.Errorf("invalid parent fragment shard leaf value length: got %d, want %d", len(f.ShardLeafValue), SiblingSize)
	}
	if !bytes.Equal(f.ShardLeafValue, expectedLeafValue) {
		return errors.New("parent fragment shard leaf value does not match expected child root")
	}

	var cert InclusionCert
	if err := cert.UnmarshalBinary(f.CertificateBytes); err != nil {
		return fmt.Errorf("failed to decode parent fragment cert: %w", err)
	}

	path := big.NewInt(int64(shardID))
	key, err := PathToFixedBytes(path, keyLength)
	if err != nil {
		return fmt.Errorf("failed to derive parent fragment key: %w", err)
	}

	return verifyBitmapPath(&cert.Bitmap, cert.Siblings, key, f.ShardLeafValue, expectedRoot, algo)
}

type RootShardInclusionProof struct {
	ParentFragment     *ParentInclusionFragment `json:"parentFragment,omitempty"`
	UnicityCertificate HexBytes                 `json:"unicityCertificate"`
	BlockNumber        uint64                   `json:"blockNumber,omitempty"`
}

// GetNoDeletionProofResponse represents the get_no_deletion_proof JSON-RPC response
type GetNoDeletionProofResponse struct {
	NoDeletionProof *NoDeletionProof `json:"noDeletionProof"`
}

// GetBlockHeightResponse represents the get_block_height JSON-RPC response
type GetBlockHeightResponse struct {
	BlockNumber *BigInt `json:"blockNumber"`
}

// GetBlockRequest represents the get_block JSON-RPC request
type GetBlockRequest struct {
	BlockNumber interface{} `json:"blockNumber"` // Can be number, string, or "latest"
}

// GetBlockResponse represents the get_block JSON-RPC response
type GetBlockResponse struct {
	Block            *Block `json:"block"`
	TotalCommitments uint64 `json:"totalCommitments,string"`
}

// GetBlockRecords represents the get_block_records JSON-RPC request
type GetBlockRecords struct {
	BlockNumber *BigInt `json:"blockNumber"`
}

// GetBlockRecordsResponse represents the get_block_records JSON-RPC response
type GetBlockRecordsResponse struct {
	AggregatorRecords []*AggregatorRecord `json:"aggregatorRecords"`
}

// Status constants for SubmitShardRootResponse
const (
	ShardRootStatusSuccess         = "SUCCESS"
	ShardRootStatusInvalidShardID  = "INVALID_SHARD_ID"
	ShardRootStatusInvalidRootHash = "INVALID_ROOT_HASH"
	ShardRootStatusInternalError   = "INTERNAL_ERROR"
	ShardRootStatusNotLeader       = "NOT_LEADER"
	ShardRootStatusNotReady        = "NOT_READY"
)

// Health status values returned by the health endpoint.
const (
	HealthStatusOk        = "ok"
	HealthStatusUnhealthy = "unhealthy"
	HealthStatusDegraded  = "degraded"
)

// SubmitShardRootRequest represents the submit_shard_root JSON-RPC request
type SubmitShardRootRequest struct {
	ShardID  ShardID  `json:"shardId"`
	RootHash HexBytes `json:"rootHash"` // Raw root hash from child SMT
}

// SubmitShardRootResponse represents the submit_shard_root JSON-RPC response
type SubmitShardRootResponse struct {
	Status string `json:"status"` // "SUCCESS", "INVALID_SHARD_ID", "INVALID_ROOT_HASH", etc.
}

// GetShardProofRequest represents the get_shard_proof JSON-RPC request
type GetShardProofRequest struct {
	ShardID ShardID `json:"shardId"`
}

// GetShardProofResponse represents the get_shard_proof JSON-RPC response
type GetShardProofResponse struct {
	ParentFragment     *ParentInclusionFragment `json:"parentFragment,omitempty"` // native parent fragment for child v2 composition
	UnicityCertificate HexBytes                 `json:"unicityCertificate"`       // Unicity Certificate from the finalized block
	BlockNumber        uint64                   `json:"blockNumber,omitempty"`
}

// HealthStatus represents the health status of the service
type HealthStatus struct {
	Status   string            `json:"status"`
	Role     string            `json:"role"`
	ServerID string            `json:"serverId"`
	Sharding Sharding          `json:"sharding"`
	Details  map[string]string `json:"details,omitempty"`
}

// NewHealthStatus creates a new health status
func NewHealthStatus(role, serverID string) *HealthStatus {
	return &HealthStatus{
		Status:   HealthStatusOk,
		Role:     role,
		ServerID: serverID,
		Details:  make(map[string]string),
	}
}

// AddDetail adds a detail to the health status
func (h *HealthStatus) AddDetail(key, value string) {
	if h.Details == nil {
		h.Details = make(map[string]string)
	}
	h.Details[key] = value
}

func (r *RootShardInclusionProof) IsValid(shardID ShardID, keyLength int, shardRootHash HexBytes) bool {
	if r == nil || len(r.UnicityCertificate) == 0 || r.ParentFragment == nil {
		return false
	}
	rootRaw, err := ucInputRecordHashRaw(r.UnicityCertificate)
	if err != nil {
		return false
	}
	return r.ParentFragment.Verify(shardID, keyLength, shardRootHash, rootRaw, InclusionProofV2HashAlgorithm) == nil
}

type Sharding struct {
	Mode       string `json:"mode"`
	ShardIDLen int    `json:"shardIdLen"`
	ShardID    int    `json:"shardId"`
}

// MarshalJSON marshals the request to CBOR and then hex encodes it, returning the result as a JSON string.
func (c *GetInclusionProofResponseV2) MarshalJSON() ([]byte, error) {
	cborBytes, err := types.Cbor.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal to CBOR: %w", err)
	}
	return HexBytes(cborBytes).MarshalJSON()
}

// UnmarshalJSON expects a hex-encoded CBOR string, decodes it, and then unmarshals the CBOR data.
func (c *GetInclusionProofResponseV2) UnmarshalJSON(data []byte) error {
	var hb HexBytes
	if err := json.Unmarshal(data, &hb); err != nil {
		return fmt.Errorf("failed to unmarshal JSON to HexBytes: %w", err)
	}
	return types.Cbor.Unmarshal(hb, c)
}

// InclusionProofV2HashAlgorithm is the SMT hash algorithm locked in by the
// v2 inclusion proof wire contract. It is fixed to SHA-256; changing it
// requires a format version bump.
const InclusionProofV2HashAlgorithm = SHA256

// Verify checks a v2 inclusion proof against the outer
// CertificationRequest. The expected SMT root is sourced exclusively from
// UC.IR.h (strictly 32 raw bytes); CertificateBytes never carries a root.
// Non-inclusion proofs short-circuit with ErrExclusionNotImpl — v2
// exclusion verification is not yet implemented in Go.
func (p *InclusionProofV2) Verify(v2 *CertificationRequest) error {
	if p == nil {
		return errors.New("nil inclusion proof")
	}
	if v2 == nil {
		return errors.New("nil certification request")
	}
	if p.CertificationData == nil {
		return ErrExclusionNotImpl
	}
	if len(v2.CertificationData.TransactionHash) == 0 {
		return errors.New("missing certification request transaction hash")
	}
	if !bytes.Equal(
		p.CertificationData.TransactionHash.DataBytes(),
		v2.CertificationData.TransactionHash.DataBytes(),
	) {
		return errors.New("proof certification data transaction hash does not match certification request transaction hash")
	}

	rootRaw, err := p.UCInputRecordHashRaw()
	if err != nil {
		return err
	}

	var cert InclusionCert
	if err := cert.UnmarshalBinary(p.CertificateBytes); err != nil {
		return fmt.Errorf("failed to decode inclusion cert: %w", err)
	}
	key, err := v2.StateID.GetTreeKey()
	if err != nil {
		return fmt.Errorf("failed to derive SMT key from stateId: %w", err)
	}
	// v2 leaf value is the raw transaction hash.
	value := v2.CertificationData.TransactionHash.DataBytes()
	return cert.Verify(key, value, rootRaw, InclusionProofV2HashAlgorithm)
}

// UCInputRecordHashRaw decodes the embedded Unicity Certificate and
// returns UC.IR.h as a raw 32-byte hash. Any other length is rejected.
func (p *InclusionProofV2) UCInputRecordHashRaw() ([]byte, error) {
	return ucInputRecordHashRaw(p.UnicityCertificate)
}

func ucInputRecordHashRaw(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("missing unicity certificate")
	}
	var uc types.UnicityCertificate
	if err := types.Cbor.Unmarshal(raw, &uc); err != nil {
		return nil, fmt.Errorf("failed to decode unicity certificate: %w", err)
	}
	if uc.InputRecord == nil {
		return nil, errors.New("unicity certificate missing input record")
	}
	ir := uc.InputRecord.Hash
	if len(ir) != StateTreeKeyLengthBytes {
		return nil, fmt.Errorf("invalid UC.IR.h length: got %d, want %d",
			len(ir), StateTreeKeyLengthBytes)
	}
	return append([]byte(nil), ir...), nil
}

func verify(p *MerkleTreePath, path *big.Int, expectedLeafValue []byte) error {
	// Verify leaf matches the first merkle tree step
	if p == nil {
		return errors.New("missing merkle tree path")
	}
	if len(p.Steps) == 0 {
		return errors.New("empty merkle path")
	}
	if p.Steps[0].Data == nil {
		return errors.New("missing leaf data in proof")
	}
	leafValue, err := hex.DecodeString(*p.Steps[0].Data)
	if err != nil {
		return fmt.Errorf("invalid leaf data encoding: %w", err)
	}
	if !bytes.Equal(expectedLeafValue, leafValue) {
		return errors.New("leaf hash mismatch: proof does not include expected value")
	}

	// Verify merkle tree path hashes to root
	res, err := p.Verify(path)
	if err != nil {
		return fmt.Errorf("merkle path verification failed: %w", err)
	}
	if !res.Result {
		return errors.New("merkle path verification failed")
	}
	return nil
}
