package models

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Block represents a blockchain block
type Block struct {
	Index               *api.BigInt                  `json:"index"`
	ChainID             string                       `json:"chainId"`
	ShardID             api.ShardID                  `json:"shardId"`
	Version             string                       `json:"version"`
	ForkID              string                       `json:"forkId"`
	RootHash            api.HexBytes                 `json:"rootHash"`
	PreviousBlockHash   api.HexBytes                 `json:"previousBlockHash"`
	NoDeletionProofHash api.HexBytes                 `json:"noDeletionProofHash"`
	CreatedAt           *api.Timestamp               `json:"createdAt"`
	UnicityCertificate  api.HexBytes                 `json:"unicityCertificate"`
	ParentFragment      *api.ParentInclusionFragment `json:"parentFragment,omitempty"`    // child mode only
	ParentBlockNumber   uint64                       `json:"parentBlockNumber,omitempty"` // child mode only
	Finalized           bool                         `json:"finalized"`                   // true when all data is persisted
	Status              string                       `json:"status"`
	ProposalID          string                       `json:"proposalId,omitempty"`
}

// BlockBSON represents the BSON version of Block for MongoDB storage
type BlockBSON struct {
	Index               primitive.Decimal128 `bson:"index"`
	ChainID             string               `bson:"chainId"`
	ShardID             api.ShardID          `bson:"shardId"`
	Version             string               `bson:"version"`
	ForkID              string               `bson:"forkId"`
	RootHash            string               `bson:"rootHash"`
	PreviousBlockHash   string               `bson:"previousBlockHash"`
	NoDeletionProofHash string               `bson:"noDeletionProofHash,omitempty"`
	CreatedAt           time.Time            `bson:"createdAt"`
	UnicityCertificate  string               `bson:"unicityCertificate"`
	ParentFragment      *ParentFragmentBSON  `bson:"parentFragment,omitempty"` // child mode only
	ParentBlockNumber   uint64               `bson:"parentBlockNumber,omitempty"`
	Finalized           bool                 `bson:"finalized"`
	Status              string               `bson:"status,omitempty"`
	ProposalID          string               `bson:"proposalId,omitempty"`
}

// ParentFragmentBSON is the BSON representation of ParentInclusionFragment.
type ParentFragmentBSON struct {
	CertificateBytes []byte `bson:"certificateBytes"`
	ShardLeafValue   []byte `bson:"shardLeafValue"`
}

// ToBSON converts Block to BlockBSON for MongoDB storage
func (b *Block) ToBSON() (*BlockBSON, error) {
	indexDecimal, err := primitive.ParseDecimal128(b.Index.String())
	if err != nil {
		return nil, fmt.Errorf("error converting block index to decimal-128: %w", err)
	}
	var parentFragment *ParentFragmentBSON
	if b.ParentFragment != nil {
		parentFragment = &ParentFragmentBSON{
			CertificateBytes: append([]byte(nil), b.ParentFragment.CertificateBytes...),
			ShardLeafValue:   append([]byte(nil), b.ParentFragment.ShardLeafValue...),
		}
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
		CreatedAt:           b.CreatedAt.Time,
		UnicityCertificate:  b.UnicityCertificate.String(),
		ParentFragment:      parentFragment,
		ParentBlockNumber:   b.ParentBlockNumber,
		Finalized:           b.Finalized,
		Status:              b.Status,
		ProposalID:          b.ProposalID,
	}, nil
}

// FromBSON converts BlockBSON back to Block
func (bb *BlockBSON) FromBSON() (*Block, error) {
	index, err := api.NewBigIntFromString(bb.Index.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse index: %w", err)
	}

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

	var parentFragment *api.ParentInclusionFragment
	if bb.ParentFragment != nil {
		parentFragment = &api.ParentInclusionFragment{
			CertificateBytes: append([]byte(nil), bb.ParentFragment.CertificateBytes...),
			ShardLeafValue:   append([]byte(nil), bb.ParentFragment.ShardLeafValue...),
		}
	}

	noDeletionProofHash, err := api.NewHexBytesFromString(bb.NoDeletionProofHash)
	if err != nil {
		return nil, fmt.Errorf("failed to parse noDeletionProofHash: %w", err)
	}

	return &Block{
		Index:               index,
		ChainID:             bb.ChainID,
		ShardID:             bb.ShardID,
		Version:             bb.Version,
		ForkID:              bb.ForkID,
		RootHash:            rootHash,
		PreviousBlockHash:   previousBlockHash,
		NoDeletionProofHash: noDeletionProofHash,
		CreatedAt:           api.NewTimestamp(bb.CreatedAt),
		UnicityCertificate:  unicityCertificate,
		ParentFragment:      parentFragment,
		ParentBlockNumber:   bb.ParentBlockNumber,
		Finalized:           bb.Finalized,
		Status:              bb.Status,
		ProposalID:          bb.ProposalID,
	}, nil
}

// NewBlock creates a new block
func NewBlock(index *api.BigInt, chainID string, shardID api.ShardID, version, forkID string, rootHash, previousBlockHash, uc api.HexBytes) *Block {
	return &Block{
		Index:              index,
		ChainID:            chainID,
		ShardID:            shardID,
		Version:            version,
		ForkID:             forkID,
		RootHash:           rootHash,
		PreviousBlockHash:  previousBlockHash,
		CreatedAt:          api.Now(),
		UnicityCertificate: uc,
	}
}

// NewChildBlock creates a block for child mode with required parent proof metadata.
func NewChildBlock(index *api.BigInt, chainID string, shardID api.ShardID, version, forkID string, rootHash, previousBlockHash, uc api.HexBytes, parentFragment *api.ParentInclusionFragment, parentBlockNumber uint64) *Block {
	block := NewBlock(index, chainID, shardID, version, forkID, rootHash, previousBlockHash, uc)
	block.ParentFragment = parentFragment
	block.ParentBlockNumber = parentBlockNumber
	return block
}
