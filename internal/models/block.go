package models

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Block represents a blockchain block
type Block struct {
	Index                *api.BigInt         `json:"index"`
	ChainID              string              `json:"chainId"`
	ShardID              int                 `json:"shardId"`
	Version              string              `json:"version"`
	ForkID               string              `json:"forkId"`
	RootHash             api.HexBytes        `json:"rootHash"`
	PreviousBlockHash    api.HexBytes        `json:"previousBlockHash"`
	NoDeletionProofHash  api.HexBytes        `json:"noDeletionProofHash"`
	CreatedAt            *api.Timestamp      `json:"createdAt"`
	UnicityCertificate   api.HexBytes        `json:"unicityCertificate"`
	ParentMerkleTreePath *api.MerkleTreePath `json:"parentMerkleTreePath,omitempty"` // child mode only
}

// BlockBSON represents the BSON version of Block for MongoDB storage
type BlockBSON struct {
	Index               primitive.Decimal128 `bson:"index"`
	ChainID             string               `bson:"chainId"`
	ShardID             int                  `bson:"shardId"`
	Version             string               `bson:"version"`
	ForkID              string               `bson:"forkId"`
	RootHash            string               `bson:"rootHash"`
	PreviousBlockHash   string               `bson:"previousBlockHash"`
	NoDeletionProofHash string               `bson:"noDeletionProofHash,omitempty"`
	CreatedAt           string               `bson:"createdAt"`
	UnicityCertificate  string               `bson:"unicityCertificate"`
	MerkleTreePath      string               `bson:"merkleTreePath,omitempty"` // child mode only
}

// ToBSON converts Block to BlockBSON for MongoDB storage
func (b *Block) ToBSON() (*BlockBSON, error) {
	indexDecimal, err := primitive.ParseDecimal128(b.Index.String())
	if err != nil {
		return nil, fmt.Errorf("error converting block index to decimal-128: %w", err)
	}
	var merkleTreePath string
	if b.ParentMerkleTreePath != nil {
		merkleTreePathJson, err := json.Marshal(b.ParentMerkleTreePath)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal parent merkle tree path: %w", err)
		}
		merkleTreePath = api.NewHexBytes(merkleTreePathJson).String()
	}
	return &BlockBSON{
		Index:               indexDecimal,
		ChainID:             b.ChainID,
		ShardID:             b.ShardID,
		Version:             b.Version,
		ForkID:              b.ForkID,
		RootHash:            b.RootHash.String(),
		PreviousBlockHash:   b.PreviousBlockHash.String(),
		NoDeletionProofHash: b.NoDeletionProofHash.String(),
		CreatedAt:           strconv.FormatInt(b.CreatedAt.UnixMilli(), 10),
		UnicityCertificate:  b.UnicityCertificate.String(),
		MerkleTreePath:      merkleTreePath,
	}, nil
}

// FromBSON converts BlockBSON back to Block
func (bb *BlockBSON) FromBSON() (*Block, error) {
	index, err := api.NewBigIntFromString(bb.Index.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse index: %w", err)
	}

	createdAtMillis, err := strconv.ParseInt(bb.CreatedAt, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse createdAt: %w", err)
	}
	createdAt := &api.Timestamp{Time: time.UnixMilli(createdAtMillis)}

	rootHash, err := api.NewHexBytesFromString(bb.RootHash)
	if err != nil {
		return nil, fmt.Errorf("failed to parse rootHash: %w", err)
	}

	previousBlockHash, err := api.NewHexBytesFromString(bb.PreviousBlockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to parse previousBlockHash: %w", err)
	}

	unicityCertificate, err := api.NewHexBytesFromString(bb.UnicityCertificate)
	if err != nil {
		return nil, fmt.Errorf("failed to parse unicityCertificate: %w", err)
	}

	var parentMerkleTreePath *api.MerkleTreePath
	if bb.MerkleTreePath != "" {
		hexBytes, err := api.NewHexBytesFromString(bb.MerkleTreePath)
		if err != nil {
			return nil, fmt.Errorf("failed to parse parentMerkleTreePath: %w", err)
		}
		parentMerkleTreePath = &api.MerkleTreePath{}
		if err := json.Unmarshal(hexBytes, parentMerkleTreePath); err != nil {
			return nil, fmt.Errorf("failed to parse parentMerkleTreePath: %w", err)
		}
	}

	noDeletionProofHash, err := api.NewHexBytesFromString(bb.NoDeletionProofHash)
	if err != nil {
		return nil, fmt.Errorf("failed to parse noDeletionProofHash: %w", err)
	}

	return &Block{
		Index:                index,
		ChainID:              bb.ChainID,
		ShardID:              bb.ShardID,
		Version:              bb.Version,
		ForkID:               bb.ForkID,
		RootHash:             rootHash,
		PreviousBlockHash:    previousBlockHash,
		NoDeletionProofHash:  noDeletionProofHash,
		CreatedAt:            createdAt,
		UnicityCertificate:   unicityCertificate,
		ParentMerkleTreePath: parentMerkleTreePath,
	}, nil
}

// NewBlock creates a new block
func NewBlock(index *api.BigInt, chainID string, shardID int, version, forkID string, rootHash, previousBlockHash, uc api.HexBytes, parentMerkleTreePath *api.MerkleTreePath) *Block {
	return &Block{
		Index:                index,
		ChainID:              chainID,
		ShardID:              shardID,
		Version:              version,
		ForkID:               forkID,
		RootHash:             rootHash,
		PreviousBlockHash:    previousBlockHash,
		CreatedAt:            api.Now(),
		UnicityCertificate:   uc,
		ParentMerkleTreePath: parentMerkleTreePath,
	}
}

// SmtNode represents a Sparse Merkle Tree node
type SmtNode struct {
	Key       api.HexBytes   `json:"key" bson:"key"`
	Value     api.HexBytes   `json:"value" bson:"value"`
	CreatedAt *api.Timestamp `json:"createdAt" bson:"createdAt"`
}

// NewSmtNode creates a new SMT node
func NewSmtNode(key, value api.HexBytes) *SmtNode {
	return &SmtNode{
		Key:       key,
		Value:     value,
		CreatedAt: api.Now(),
	}
}
