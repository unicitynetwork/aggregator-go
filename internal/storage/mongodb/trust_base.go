package mongodb

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/unicitynetwork/bft-go-base/types"
)

const trustBaseCollection = "trust_bases"

var (
	ErrTrustBaseNotFound      = errors.New("trust base not found")
	ErrTrustBaseAlreadyExists = errors.New("trust base already exists")
)

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
	epoch := trustBase.GetEpoch()
	version := uint64(trustBase.GetVersion())
	mappedVersion := s.GetVersion(epoch)
	if version != mappedVersion {
		return fmt.Errorf("trust base version mismatch: got %d expected %d", version, mappedVersion)
	}

	// check if a trust base for this epoch already exists
	existing, err := s.GetByEpoch(ctx, epoch)
	if err != nil && !errors.Is(err, ErrTrustBaseNotFound) {
		return fmt.Errorf("failed to check for existing trust base: %w", err)
	}
	if existing != nil {
		return ErrTrustBaseAlreadyExists
	}

	// verify trust base extends previous trust base
	var previousTrustBaseV1 *types.RootTrustBaseV1
	if epoch > 0 {
		previousTrustBase, err := s.GetByEpoch(ctx, epoch-1)
		if err != nil {
			return fmt.Errorf("previous trust base not found for epoch %d: %w", epoch-1, err)
		}
		var ok bool
		previousTrustBaseV1, ok = previousTrustBase.(*types.RootTrustBaseV1)
		if !ok {
			return fmt.Errorf("failed to cast previous trust base to version 1 for epoch %d", epoch)
		}
	}
	trustBaseV1, ok := trustBase.(*types.RootTrustBaseV1)
	if !ok {
		return fmt.Errorf("failed to cast provided trust base to version 1 for epoch %d", epoch)
	}
	if err := trustBaseV1.Verify(previousTrustBaseV1); err != nil {
		return fmt.Errorf("failed to verify trust base: %w", err)
	}

	_, err = s.collection.InsertOne(ctx, trustBase)
	if err != nil {
		return fmt.Errorf("failed to store trust base for epoch %d: %w", epoch, err)
	}
	return nil
}

// GetByEpoch retrieves a RootTrustBase by its epoch number.
func (s *TrustBaseStorage) GetByEpoch(ctx context.Context, epoch uint64) (types.RootTrustBase, error) {
	version := s.GetVersion(epoch)
	if s.GetVersion(epoch) != 1 {
		return nil, fmt.Errorf("invalid version: got %d expected %d", version, 1)
	}
	var trustBase types.RootTrustBaseV1
	filter := bson.M{"epoch": epoch}
	if err := s.collection.FindOne(ctx, filter).Decode(&trustBase); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrTrustBaseNotFound
		}
		return nil, fmt.Errorf("failed to get trust base by epoch %d: %w", epoch, err)
	}
	return &trustBase, nil
}

// GetByRound retrieves the active RootTrustBase for a given root chain round number.
// It finds the trust base with the highest EpochStart that is less than or equal to the given round.
func (s *TrustBaseStorage) GetByRound(ctx context.Context, round uint64) (types.RootTrustBase, error) {
	var trustBase types.RootTrustBaseV1
	filter := bson.M{"epochStartRound": bson.M{"$lte": round}}
	opts := options.FindOne().SetSort(bson.M{"epochStartRound": -1})

	if err := s.collection.FindOne(ctx, filter, opts).Decode(&trustBase); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrTrustBaseNotFound
		}
		return nil, fmt.Errorf("failed to get trust base by round %d: %w", round, err)
	}
	return &trustBase, nil
}

// GetAll retrieves all trust bases from storage.
func (s *TrustBaseStorage) GetAll(ctx context.Context) ([]types.RootTrustBase, error) {
	cursor, err := s.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("failed to get all trust bases: %w", err)
	}
	defer cursor.Close(ctx)

	var trustBases []*types.RootTrustBaseV1
	if err := cursor.All(ctx, &trustBases); err != nil {
		return nil, fmt.Errorf("failed to decode trust bases: %w", err)
	}

	result := make([]types.RootTrustBase, len(trustBases))
	for i, tb := range trustBases {
		result[i] = tb
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
