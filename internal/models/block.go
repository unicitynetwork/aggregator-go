package models

import (
	"fmt"
	"strconv"
	"time"
)

// Block represents a blockchain block
type Block struct {
	Index               *BigInt    `json:"index" bson:"index"`
	ChainID             string     `json:"chainId" bson:"chainId"`
	Version             string     `json:"version" bson:"version"`
	ForkID              string     `json:"forkId" bson:"forkId"`
	Timestamp           *Timestamp `json:"timestamp" bson:"timestamp"`
	RootHash            HexBytes   `json:"rootHash" bson:"rootHash"`
	PreviousBlockHash   HexBytes   `json:"previousBlockHash" bson:"previousBlockHash"`
	NoDeletionProofHash *HexBytes  `json:"noDeletionProofHash" bson:"noDeletionProofHash,omitempty"`
	CreatedAt           *Timestamp `json:"createdAt" bson:"createdAt"`
}

// BlockBSON represents the BSON version of Block for MongoDB storage
type BlockBSON struct {
	Index               string `bson:"index"`
	ChainID             string `bson:"chainId"`
	Version             string `bson:"version"`
	ForkID              string `bson:"forkId"`
	Timestamp           string `bson:"timestamp"`
	RootHash            string `bson:"rootHash"`
	PreviousBlockHash   string `bson:"previousBlockHash"`
	NoDeletionProofHash string `bson:"noDeletionProofHash,omitempty"`
	CreatedAt           string `bson:"createdAt"`
}

// ToBSON converts Block to BlockBSON for MongoDB storage
func (b *Block) ToBSON() *BlockBSON {
	blockBSON := &BlockBSON{
		Index:             b.Index.String(),
		ChainID:           b.ChainID,
		Version:           b.Version,
		ForkID:            b.ForkID,
		Timestamp:         strconv.FormatInt(b.Timestamp.UnixMilli(), 10),
		RootHash:          b.RootHash.String(),
		PreviousBlockHash: b.PreviousBlockHash.String(),
		CreatedAt:         strconv.FormatInt(b.CreatedAt.UnixMilli(), 10),
	}
	
	if b.NoDeletionProofHash != nil {
		blockBSON.NoDeletionProofHash = b.NoDeletionProofHash.String()
	}
	
	return blockBSON
}

// FromBSON converts BlockBSON back to Block
func (bb *BlockBSON) FromBSON() (*Block, error) {
	index, err := NewBigIntFromString(bb.Index)
	if err != nil {
		return nil, fmt.Errorf("failed to parse index: %w", err)
	}
	
	timestampMillis, err := strconv.ParseInt(bb.Timestamp, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}
	timestamp := &Timestamp{Time: time.UnixMilli(timestampMillis)}
	
	createdAtMillis, err := strconv.ParseInt(bb.CreatedAt, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse createdAt: %w", err)
	}
	createdAt := &Timestamp{Time: time.UnixMilli(createdAtMillis)}
	
	rootHash, err := NewHexBytesFromString(bb.RootHash)
	if err != nil {
		return nil, fmt.Errorf("failed to parse rootHash: %w", err)
	}
	
	previousBlockHash, err := NewHexBytesFromString(bb.PreviousBlockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to parse previousBlockHash: %w", err)
	}
	
	block := &Block{
		Index:             index,
		ChainID:           bb.ChainID,
		Version:           bb.Version,
		ForkID:            bb.ForkID,
		Timestamp:         timestamp,
		RootHash:          rootHash,
		PreviousBlockHash: previousBlockHash,
		CreatedAt:         createdAt,
	}
	
	if bb.NoDeletionProofHash != "" {
		noDeletionProofHash, err := NewHexBytesFromString(bb.NoDeletionProofHash)
		if err != nil {
			return nil, fmt.Errorf("failed to parse noDeletionProofHash: %w", err)
		}
		block.NoDeletionProofHash = &noDeletionProofHash
	}
	
	return block, nil
}

// NewBlock creates a new block
func NewBlock(index *BigInt, chainID, version, forkID string, rootHash, previousBlockHash HexBytes) *Block {
	return &Block{
		Index:             index,
		ChainID:           chainID,
		Version:           version,
		ForkID:            forkID,
		Timestamp:         Now(),
		RootHash:          rootHash,
		PreviousBlockHash: previousBlockHash,
		CreatedAt:         Now(),
	}
}

// BlockRecords represents the mapping of block numbers to request IDs
type BlockRecords struct {
	BlockNumber *BigInt     `json:"blockNumber" bson:"blockNumber"`
	RequestIDs  []RequestID `json:"requestIds" bson:"requestIds"`
	CreatedAt   *Timestamp  `json:"createdAt" bson:"createdAt"`
}

// NewBlockRecords creates a new block records entry
func NewBlockRecords(blockNumber *BigInt, requestIDs []RequestID) *BlockRecords {
	return &BlockRecords{
		BlockNumber: blockNumber,
		RequestIDs:  requestIDs,
		CreatedAt:   Now(),
	}
}

// SmtNode represents a Sparse Merkle Tree node
type SmtNode struct {
	Key       HexBytes   `json:"key" bson:"key"`
	Value     HexBytes   `json:"value" bson:"value"`
	Hash      HexBytes   `json:"hash" bson:"hash"`
	CreatedAt *Timestamp `json:"createdAt" bson:"createdAt"`
}

// NewSmtNode creates a new SMT node
func NewSmtNode(key, value, hash HexBytes) *SmtNode {
	return &SmtNode{
		Key:       key,
		Value:     value,
		Hash:      hash,
		CreatedAt: Now(),
	}
}

// InclusionProof represents a merkle inclusion proof
type InclusionProof struct {
	RequestID   RequestID    `json:"requestId"`
	BlockNumber *BigInt      `json:"blockNumber,omitempty"`
	LeafIndex   *BigInt      `json:"leafIndex,omitempty"`
	Proof       []ProofNode  `json:"proof"`
	RootHash    HexBytes     `json:"rootHash"`
	Included    bool         `json:"included"`
}

// ProofNode represents a single node in an inclusion proof
type ProofNode struct {
	Hash      HexBytes `json:"hash"`
	Direction string   `json:"direction"` // "left" or "right"
}

// NewInclusionProof creates a new inclusion proof
func NewInclusionProof(requestID RequestID, blockNumber, leafIndex *BigInt, proof []ProofNode, rootHash HexBytes, included bool) *InclusionProof {
	return &InclusionProof{
		RequestID:   requestID,
		BlockNumber: blockNumber,
		LeafIndex:   leafIndex,
		Proof:       proof,
		RootHash:    rootHash,
		Included:    included,
	}
}

// NoDeletionProof represents a no-deletion proof (placeholder for future implementation)
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