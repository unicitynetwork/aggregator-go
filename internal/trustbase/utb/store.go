package utb

import (
	"context"
	"errors"

	"github.com/unicitynetwork/bft-go-base/types"
)

// ErrExists is returned by Store.Put when the epoch is already stored; the caller reads the stored bytes with Get and compares them.
var ErrExists = errors.New("trust-base entry already stored")

// ErrStoreMismatch is returned by Store.Bind when the store belongs to another network or genesis.
var ErrStoreMismatch = errors.New("trust-base store belongs to another network or genesis")

// ErrNotStored is returned by Store.Get for an epoch that is not stored.
var ErrNotStored = errors.New("trust-base entry not stored")

// Meta is the durable description of a store: what it is bound to and the tip it last acknowledged.
type Meta struct {
	Network types.NetworkID
	Pin     [types.TrustBaseV2IDLen]byte
	// TipEpoch is 0 for a store that holds nothing yet. Every entry from the genesis epoch up to TipEpoch has been written before the tip advanced to
	// it, so a stored history shorter than the tip is a missing or corrupt tail, never a shorter history.
	TipEpoch uint64
	TipID    [types.TrustBaseV2IDLen]byte
}

// Store persists the complete verified history, bundle entries included, so a restart never falls back to a shorter one. It is bound to one network
// and genesis. Writes must be acknowledged as durable before they return.
//
// Put and Advance are separate so that two aggregators sharing one database cannot lose each other's entries: Put is create-only, Advance is monotonic
// and idempotent.
type Store interface {
	// Bind creates the store's metadata for (network, pin) or checks that the existing metadata is the same (ErrStoreMismatch otherwise).
	Bind(ctx context.Context, network types.NetworkID, pin [types.TrustBaseV2IDLen]byte) error
	// Load returns the metadata and the stored entries with epoch >= from, ascending, as their canonical bytes. The metadata is read first, so every
	// entry up to Meta.TipEpoch is in the result.
	Load(ctx context.Context, from uint64) (Meta, [][]byte, error)
	// Get returns the stored bytes of an epoch (ErrNotStored if there are none).
	Get(ctx context.Context, epoch uint64) ([]byte, error)
	// Put stores the bytes of an epoch; ErrExists if the epoch is already stored (nothing is overwritten).
	Put(ctx context.Context, epoch uint64, raw []byte) error
	// Advance moves the durable tip forward to (epoch, id); a tip already at or beyond epoch is left alone.
	Advance(ctx context.Context, epoch uint64, id [types.TrustBaseV2IDLen]byte) error
}
