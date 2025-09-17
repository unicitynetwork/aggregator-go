package models

import (
	"fmt"
	"strconv"
	"time"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Block represents a blockchain block
type Block struct {
	Index               *api.BigInt    `json:"index" bson:"index"`
	ChainID             string         `json:"chainId" bson:"chainId"`
	Version             string         `json:"version" bson:"version"`
	ForkID              string         `json:"forkId" bson:"forkId"`
	RootHash            api.HexBytes   `json:"rootHash" bson:"rootHash"`
	PreviousBlockHash   api.HexBytes   `json:"previousBlockHash" bson:"previousBlockHash"`
	NoDeletionProofHash api.HexBytes   `json:"noDeletionProofHash" bson:"noDeletionProofHash,omitempty"`
	CreatedAt           *api.Timestamp `json:"createdAt" bson:"createdAt"`
	UnicityCertificate  api.HexBytes   `json:"unicityCertificate" bson:"unicityCertificate"`
}

// BlockBSON represents the BSON version of Block for MongoDB storage
type BlockBSON struct {
	Index               primitive.Decimal128 `bson:"index"`
	ChainID             string               `bson:"chainId"`
	Version             string               `bson:"version"`
	ForkID              string               `bson:"forkId"`
	RootHash            string               `bson:"rootHash"`
	PreviousBlockHash   string               `bson:"previousBlockHash"`
	NoDeletionProofHash string               `bson:"noDeletionProofHash,omitempty"`
	CreatedAt           string               `bson:"createdAt"`
	UnicityCertificate  string               `bson:"unicityCertificate"`
}

// ToBSON converts Block to BlockBSON for MongoDB storage
func (b *Block) ToBSON() *BlockBSON {
	indexDecimal, err := primitive.ParseDecimal128(b.Index.String())
	if err != nil {
		// This should never happen with valid BigInt, but fallback to zero
		indexDecimal = primitive.NewDecimal128(0, 0)
	}

	blockBSON := &BlockBSON{
		Index:               indexDecimal,
		ChainID:             b.ChainID,
		Version:             b.Version,
		ForkID:              b.ForkID,
		RootHash:            b.RootHash.String(),
		PreviousBlockHash:   b.PreviousBlockHash.String(),
		NoDeletionProofHash: b.NoDeletionProofHash.String(),
		CreatedAt:           strconv.FormatInt(b.CreatedAt.UnixMilli(), 10),
		UnicityCertificate:  b.UnicityCertificate.String(),
	}

	return blockBSON
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

	noDeletionProofHash, err := api.NewHexBytesFromString(bb.NoDeletionProofHash)
	if err != nil {
		return nil, fmt.Errorf("failed to parse noDeletionProofHash: %w", err)
	}

	block := &Block{
		Index:               index,
		ChainID:             bb.ChainID,
		Version:             bb.Version,
		ForkID:              bb.ForkID,
		RootHash:            rootHash,
		PreviousBlockHash:   previousBlockHash,
		CreatedAt:           createdAt,
		UnicityCertificate:  unicityCertificate,
		NoDeletionProofHash: noDeletionProofHash,
	}

	return block, nil
}

// NewBlock creates a new block
func NewBlock(index *api.BigInt, chainID, version, forkID string, rootHash, previousBlockHash api.HexBytes) *Block {
	return &Block{
		Index:             index,
		ChainID:           chainID,
		Version:           version,
		ForkID:            forkID,
		RootHash:          rootHash,
		PreviousBlockHash: previousBlockHash,
		CreatedAt:         api.Now(),
	}
}

// BlockRecords represents the mapping of block numbers to request IDs
type BlockRecords struct {
	BlockNumber *api.BigInt     `json:"blockNumber" bson:"blockNumber"`
	RequestIDs  []api.RequestID `json:"requestIds" bson:"requestIds"`
	CreatedAt   *api.Timestamp  `json:"createdAt" bson:"createdAt"`
}

// NewBlockRecords creates a new block records entry
func NewBlockRecords(blockNumber *api.BigInt, requestIDs []api.RequestID) *BlockRecords {
	return &BlockRecords{
		BlockNumber: blockNumber,
		RequestIDs:  requestIDs,
		CreatedAt:   api.Now(),
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
