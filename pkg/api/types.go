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
	Index                *BigInt         `json:"index"`
	ChainID              string          `json:"chainId"`
	ShardID              ShardID         `json:"shardId"`
	Version              string          `json:"version"`
	ForkID               string          `json:"forkId"`
	RootHash             HexBytes        `json:"rootHash"`
	PreviousBlockHash    HexBytes        `json:"previousBlockHash"`
	NoDeletionProofHash  HexBytes        `json:"noDeletionProofHash"`
	CreatedAt            *Timestamp      `json:"createdAt"`
	UnicityCertificate   HexBytes        `json:"unicityCertificate"`
	ParentMerkleTreePath *MerkleTreePath `json:"parentMerkleTreePath,omitempty"` // child mode only
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

// ReceiptV2 represents a signed receipt for a certification request submission
type ReceiptV2 struct {
	PublicKey HexBytes `json:"publicKey"`
	Signature HexBytes `json:"signature"`
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

type InclusionProofV2 struct {
	_                  struct{}           `cbor:",toarray"`
	CertificationData  *CertificationData `json:"certificationData"`
	MerkleTreePath     *MerkleTreePath    `json:"merkleTreePath"`
	UnicityCertificate types.RawCBOR      `json:"unicityCertificate"`
}

type RootShardInclusionProof struct {
	MerkleTreePath     *MerkleTreePath `json:"merkleTreePath"`
	UnicityCertificate HexBytes        `json:"unicityCertificate"`
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
	MerkleTreePath     *MerkleTreePath `json:"merkleTreePath"`     // Proof path for the shard
	UnicityCertificate HexBytes        `json:"unicityCertificate"` // Unicity Certificate from the finalized block
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

func (r *RootShardInclusionProof) IsValid(shardRootHash string) bool {
	return r.MerkleTreePath != nil && len(r.UnicityCertificate) > 0 &&
		len(r.MerkleTreePath.Steps) > 0 && r.MerkleTreePath.Steps[0].Data != nil && *r.MerkleTreePath.Steps[0].Data == shardRootHash
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

func (p *InclusionProofV2) Verify(v2 *CertificationRequest) error {
	path, err := v2.StateID.GetPath()
	if err != nil {
		return fmt.Errorf("failed to get path: %w", err)
	}
	expectedLeafValue, err := v2.CertificationData.Hash()
	if err != nil {
		return fmt.Errorf("failed to get leaf value: %w", err)
	}
	return verify(p.MerkleTreePath, path, expectedLeafValue)
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
