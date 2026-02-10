package mongodb

import (
	"context"
	"errors"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"
	"github.com/unicitynetwork/bft-go-base/types/hex"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

const trustBaseCollection = "trust_bases"

type TrustBaseStorage struct {
	collection *mongo.Collection
}

func NewTrustBaseStorage(db *mongo.Database) *TrustBaseStorage {
	return &TrustBaseStorage{
		collection: db.Collection(trustBaseCollection),
	}
}

// Store verifies and stores a RootTrustBase.
func (s *TrustBaseStorage) Store(ctx context.Context, trustBase types.RootTrustBase) error {
	if trustBase.GetVersion() != 1 {
		return fmt.Errorf("invalid version, got %d expected %d", trustBase.GetVersion(), 1)
	}
	trustBaseV1, ok := trustBase.(*types.RootTrustBaseV1)
	if !ok {
		return fmt.Errorf("failed to cast trust base to version 1 for epoch %d", trustBase.GetEpoch())
	}
	trustBaseBSON, err := NewTrustBaseBSON(trustBaseV1)
	if err != nil {
		return fmt.Errorf("failed to convert trust base to BSON: %w", err)
	}
	if _, err := s.collection.InsertOne(ctx, trustBaseBSON); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("trust base already exist for epoch %d: %w", trustBase.GetEpoch(), interfaces.ErrTrustBaseAlreadyExists)
		}
		return fmt.Errorf("failed to store trust base: %w", err)
	}
	return nil
}

// GetByEpoch retrieves a RootTrustBase by its epoch number.
func (s *TrustBaseStorage) GetByEpoch(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	version := s.GetVersion(epoch)
	if s.GetVersion(epoch) != 1 {
		return nil, fmt.Errorf("invalid version: got %d expected %d", version, 1)
	}
	var trustBaseBSON TrustBaseBSON
	filter := bson.M{"epoch": epoch}
	if err := s.collection.FindOne(ctx, filter).Decode(&trustBaseBSON); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, interfaces.ErrTrustBaseNotFound
		}
		return nil, fmt.Errorf("failed to get trust base by epoch %d: %w", epoch, err)
	}
	trustBase, err := trustBaseBSON.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to parse trust base BSON: %w", err)
	}
	return trustBase, nil
}

// GetLatest retrieves the trust base with the highest epoch.
func (s *TrustBaseStorage) GetLatest(ctx context.Context) (*types.RootTrustBaseV1, error) {
	var trustBaseBSON TrustBaseBSON
	opts := options.FindOne().SetSort(bson.M{"epoch": -1})
	if err := s.collection.FindOne(ctx, bson.M{}, opts).Decode(&trustBaseBSON); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, interfaces.ErrTrustBaseNotFound
		}
		return nil, fmt.Errorf("failed to get latest trust base: %w", err)
	}
	trustBase, err := trustBaseBSON.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to parse trust base BSON: %w", err)
	}
	return trustBase, nil
}

// GetByRound retrieves the active RootTrustBase for a given root chain round number.
// It finds the trust base with the highest EpochStart that is less than or equal to the given round.
func (s *TrustBaseStorage) GetByRound(ctx context.Context, round uint64) (*types.RootTrustBaseV1, error) {
	var trustBaseBSON TrustBaseBSON
	filter := bson.M{"epochStartRound": bson.M{"$lte": round}}
	opts := options.FindOne().SetSort(bson.M{"epochStartRound": -1})

	if err := s.collection.FindOne(ctx, filter, opts).Decode(&trustBaseBSON); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, interfaces.ErrTrustBaseNotFound
		}
		return nil, fmt.Errorf("failed to get trust base by round %d: %w", round, err)
	}
	trustBase, err := trustBaseBSON.FromBSON()
	if err != nil {
		return nil, fmt.Errorf("failed to parse trust base BSON: %w", err)
	}
	return trustBase, nil
}

// GetAll retrieves all trust bases from storage.
func (s *TrustBaseStorage) GetAll(ctx context.Context) ([]*types.RootTrustBaseV1, error) {
	cursor, err := s.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("failed to get all trust bases: %w", err)
	}
	defer cursor.Close(ctx)

	var trustBaseBSONs []*TrustBaseBSON
	if err := cursor.All(ctx, &trustBaseBSONs); err != nil {
		return nil, fmt.Errorf("failed to decode trust bases: %w", err)
	}

	result := make([]*types.RootTrustBaseV1, len(trustBaseBSONs))
	for i, tb := range trustBaseBSONs {
		result[i], err = tb.FromBSON()
		if err != nil {
			return nil, fmt.Errorf("failed to parse trust base BSON: %w", err)
		}
	}
	return result, nil
}

// CreateIndexes creates necessary indexes for the trust base collection.
func (s *TrustBaseStorage) CreateIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "epoch", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "epochStart", Value: 1}},
		},
	}
	_, err := s.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("failed to create trust base indexes: %w", err)
	}
	return nil
}

// GetVersion returns trust base version based on epoch
func (s *TrustBaseStorage) GetVersion(epoch uint64) uint64 {
	return 1 // currently only valid version is 1
}

type (
	TrustBaseBSON struct {
		_                 struct{}             `cbor:",toarray"`
		Version           types.Version        `bson:"version"`
		NetworkID         types.NetworkID      `bson:"networkId"`
		Epoch             uint64               `bson:"epoch"`             // current epoch number
		EpochStart        uint64               `bson:"epochStartRound"`   // root chain round number when the epoch begins
		RootNodes         []*NodeInfoBSON      `bson:"rootNodes"`         // list of all root nodes for the current epoch
		QuorumThreshold   uint64               `bson:"quorumThreshold"`   // amount of coins required to reach consensus, currently each node gets equal amount of voting power i.e. +1 for each node
		StateHash         hex.Bytes            `bson:"stateHash"`         // unicity tree root hash
		ChangeRecordHash  hex.Bytes            `bson:"changeRecordHash"`  // epoch change request hash
		PreviousEntryHash hex.Bytes            `bson:"previousEntryHash"` // previous trust base entry hash
		Signatures        map[string]hex.Bytes `bson:"signatures"`        // signatures of previous epoch validators, over all fields except for the signatures fields itself
	}

	NodeInfoBSON struct {
		NodeID string    `bson:"nodeId"` // node identifier
		SigKey hex.Bytes `bson:"sigKey"` // signing key of the node
		Stake  uint64    `bson:"stake"`  // amount of staked coins for this node
	}
)

func NewTrustBaseBSON(tb *types.RootTrustBaseV1) (*TrustBaseBSON, error) {
	rootNodesBSON := make([]*NodeInfoBSON, len(tb.RootNodes))
	for i, rn := range tb.RootNodes {
		rootNodesBSON[i] = NewNodeInfoBSON(rn)
	}
	return &TrustBaseBSON{
		Version:           tb.Version,
		NetworkID:         tb.NetworkID,
		Epoch:             tb.Epoch,
		EpochStart:        tb.EpochStart,
		RootNodes:         rootNodesBSON,
		QuorumThreshold:   tb.QuorumThreshold,
		StateHash:         tb.StateHash,
		ChangeRecordHash:  tb.ChangeRecordHash,
		PreviousEntryHash: tb.PreviousEntryHash,
		Signatures:        tb.Signatures,
	}, nil
}

func NewNodeInfoBSON(n *types.NodeInfo) *NodeInfoBSON {
	return &NodeInfoBSON{
		NodeID: n.NodeID,
		SigKey: n.SigKey,
		Stake:  n.Stake,
	}
}

func (tb *TrustBaseBSON) FromBSON() (*types.RootTrustBaseV1, error) {
	rootNodes := make([]*types.NodeInfo, len(tb.RootNodes))
	for i, rn := range tb.RootNodes {
		rootNodes[i] = rn.FromBSON()
	}
	return &types.RootTrustBaseV1{
		Version:           tb.Version,
		NetworkID:         tb.NetworkID,
		Epoch:             tb.Epoch,
		EpochStart:        tb.EpochStart,
		RootNodes:         rootNodes,
		QuorumThreshold:   tb.QuorumThreshold,
		StateHash:         tb.StateHash,
		ChangeRecordHash:  tb.ChangeRecordHash,
		PreviousEntryHash: tb.PreviousEntryHash,
		Signatures:        tb.Signatures,
	}, nil
}

func (n *NodeInfoBSON) FromBSON() *types.NodeInfo {
	return &types.NodeInfo{
		NodeID: n.NodeID,
		SigKey: n.SigKey,
		Stake:  n.Stake,
	}
}
