package mongodb

import (
	"context"
	"errors"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/unicitynetwork/aggregator-go/internal/trustbase/utb"
)

const (
	trustBaseHistoryCollection = "trust_base_entries_v2"
	trustBaseHistoryMetaID     = "meta"
)

// TrustBaseHistoryStore persists the Unicity Trust Base history (record version 2) in one collection: a document per entry (_id = epoch, the canonical
// bytes) and one meta document (what the collection is bound to and the acknowledged tip). The collection is always used with a majority, journaled write
// concern and a majority read concern, whatever the rest of the storage is configured to: an entry the aggregator has served must survive a primary
// change. It implements utb.Store.
type TrustBaseHistoryStore struct {
	collection *mongo.Collection
}

// NewTrustBaseHistoryStore returns the store over the database.
func NewTrustBaseHistoryStore(db *mongo.Database) *TrustBaseHistoryStore {
	journal := true
	return &TrustBaseHistoryStore{collection: db.Collection(trustBaseHistoryCollection,
		options.Collection().
			SetWriteConcern(&writeconcern.WriteConcern{W: "majority", Journal: &journal}).
			SetReadConcern(readconcern.Majority()))}
}

type historyMeta struct {
	ID       string           `bson:"_id"`
	Network  int64            `bson:"network"`
	Pin      primitive.Binary `bson:"pin"`
	TipEpoch int64            `bson:"tipEpoch"`
	TipID    primitive.Binary `bson:"tipId"`
}

type historyEntry struct {
	Epoch int64            `bson:"_id"`
	Raw   primitive.Binary `bson:"raw"`
}

func epochKey(epoch uint64) (int64, error) {
	if epoch > 1<<62 {
		return 0, fmt.Errorf("epoch %d is out of range", epoch)
	}
	return int64(epoch), nil
}

// Bind implements utb.Store.
func (s *TrustBaseHistoryStore) Bind(ctx context.Context, network types.NetworkID, pin [types.TrustBaseV2IDLen]byte) error {
	_, err := s.collection.UpdateOne(ctx, bson.M{"_id": trustBaseHistoryMetaID},
		bson.M{"$setOnInsert": bson.M{"network": int64(network), "pin": primitive.Binary{Data: pin[:]}, "tipEpoch": int64(0),
			"tipId": primitive.Binary{Data: make([]byte, types.TrustBaseV2IDLen)}}},
		options.Update().SetUpsert(true))
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return fmt.Errorf("binding the trust-base history: %w", err)
	}
	meta, err := s.meta(ctx)
	if err != nil {
		return err
	}
	if meta.Network != network || meta.Pin != pin {
		return utb.ErrStoreMismatch
	}
	return nil
}

func (s *TrustBaseHistoryStore) meta(ctx context.Context) (utb.Meta, error) {
	var d historyMeta
	if err := s.collection.FindOne(ctx, bson.M{"_id": trustBaseHistoryMetaID}).Decode(&d); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return utb.Meta{}, nil
		}
		return utb.Meta{}, fmt.Errorf("reading the trust-base history metadata: %w", err)
	}
	m := utb.Meta{Network: types.NetworkID(d.Network), TipEpoch: uint64(d.TipEpoch)}
	if len(d.Pin.Data) != types.TrustBaseV2IDLen || len(d.TipID.Data) != types.TrustBaseV2IDLen || d.TipEpoch < 0 {
		return utb.Meta{}, errors.New("the trust-base history metadata is malformed")
	}
	copy(m.Pin[:], d.Pin.Data)
	copy(m.TipID[:], d.TipID.Data)
	return m, nil
}

// Load implements utb.Store: the metadata is read first, so every entry up to its tip is in the result.
func (s *TrustBaseHistoryStore) Load(ctx context.Context, from uint64) (utb.Meta, [][]byte, error) {
	meta, err := s.meta(ctx)
	if err != nil {
		return utb.Meta{}, nil, err
	}
	key, err := epochKey(from)
	if err != nil {
		return utb.Meta{}, nil, err
	}
	cur, err := s.collection.Find(ctx, bson.M{"_id": bson.M{"$type": "number", "$gte": key}}, options.Find().SetSort(bson.M{"_id": 1}))
	if err != nil {
		return utb.Meta{}, nil, fmt.Errorf("reading the trust-base history: %w", err)
	}
	defer cur.Close(ctx)
	var out [][]byte
	for cur.Next(ctx) {
		var d historyEntry
		if err := cur.Decode(&d); err != nil {
			return utb.Meta{}, nil, fmt.Errorf("decoding a stored trust-base entry: %w", err)
		}
		out = append(out, d.Raw.Data)
	}
	if err := cur.Err(); err != nil {
		return utb.Meta{}, nil, fmt.Errorf("reading the trust-base history: %w", err)
	}
	return meta, out, nil
}

// Get implements utb.Store.
func (s *TrustBaseHistoryStore) Get(ctx context.Context, epoch uint64) ([]byte, error) {
	key, err := epochKey(epoch)
	if err != nil {
		return nil, err
	}
	var d historyEntry
	if err := s.collection.FindOne(ctx, bson.M{"_id": key}).Decode(&d); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, utb.ErrNotStored
		}
		return nil, fmt.Errorf("reading stored epoch %d: %w", epoch, err)
	}
	return d.Raw.Data, nil
}

// Put implements utb.Store: create-only.
func (s *TrustBaseHistoryStore) Put(ctx context.Context, epoch uint64, raw []byte) error {
	key, err := epochKey(epoch)
	if err != nil {
		return err
	}
	if _, err := s.collection.InsertOne(ctx, historyEntry{Epoch: key, Raw: primitive.Binary{Data: raw}}); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return utb.ErrExists
		}
		return fmt.Errorf("storing epoch %d: %w", epoch, err)
	}
	return nil
}

// Advance implements utb.Store: monotonic and idempotent. Without a bound store there is nothing to advance and nothing happens.
func (s *TrustBaseHistoryStore) Advance(ctx context.Context, epoch uint64, id [types.TrustBaseV2IDLen]byte) error {
	key, err := epochKey(epoch)
	if err != nil {
		return err
	}
	_, err = s.collection.UpdateOne(ctx, bson.M{"_id": trustBaseHistoryMetaID, "tipEpoch": bson.M{"$lt": key}},
		bson.M{"$set": bson.M{"tipEpoch": key, "tipId": primitive.Binary{Data: id[:]}}})
	if err != nil {
		return fmt.Errorf("advancing the trust-base history tip to epoch %d: %w", epoch, err)
	}
	return nil
}

var _ utb.Store = (*TrustBaseHistoryStore)(nil)
