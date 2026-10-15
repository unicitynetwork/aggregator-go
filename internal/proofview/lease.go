package proofview

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ErrLeaseReleased is returned by a Lease used after Release.
var ErrLeaseReleased = errors.New("proofview: lease released")

// BlockIdentity identifies the local finalized block a view belongs to. It is
// the real block identity, never derived from Technical.Round or the root.
type BlockIdentity struct {
	Number *api.BigInt
	// ConfigLineage identifies the admitted shard configuration the state was
	// finalized under (for example its configuration hash).
	ConfigLineage []byte
}

func (b BlockIdentity) clone() BlockIdentity {
	var n *api.BigInt
	if b.Number != nil {
		n = api.NewBigInt(nil)
		n.Set(b.Number.Int)
	}
	return BlockIdentity{Number: n, ConfigLineage: append([]byte(nil), b.ConfigLineage...)}
}

func (b BlockIdentity) equal(o BlockIdentity) bool {
	if (b.Number == nil) != (o.Number == nil) {
		return false
	}
	if b.Number != nil && b.Number.Cmp(o.Number.Int) != 0 {
		return false
	}
	return string(b.ConfigLineage) == string(o.ConfigLineage)
}

// Lease pins one finalized view and one certification attachment for the
// duration of a request. Everything it exposes is immutable and stays valid
// until Release, even if the catalog meanwhile evicts the view or replaces the
// attachment. Release must be called on every exit path; it is idempotent.
type Lease struct {
	c   *Catalog
	v   *view
	att *attachment

	ctx      context.Context
	cancel   context.CancelFunc
	released atomic.Bool
}

// Context is the lease's deadline context: it carries the caller's context,
// the maximum lease duration, and cancellation under retention pressure. Proof
// work should run under it. Cancellation never closes the reader; only
// Release does, after the work has stopped.
func (l *Lease) Context() context.Context { return l.ctx }

// Root returns the raw root of the leased state. The slice is immutable.
func (l *Lease) Root() []byte { return l.v.root }

// Block returns a copy of the finalized local block identity.
func (l *Lease) Block() BlockIdentity { return l.v.block.clone() }

// UC returns the exact certified UC bytes of the leased attachment. The slice
// is immutable.
func (l *Lease) UC() []byte { return l.att.uc }

// PartitionView returns the authenticated-transport partition view bytes
// accepted with this UC, or nil when the attachment has none. The slice is
// immutable.
func (l *Lease) PartitionView() []byte { return l.att.view }

// GetInclusionCert reads a proof from the leased state under the lease's
// context.
func (l *Lease) GetInclusionCert(key []byte) (*api.InclusionCert, error) {
	if l.released.Load() {
		return nil, ErrLeaseReleased
	}
	if err := l.ctx.Err(); err != nil {
		return nil, err
	}
	return l.v.reader.GetInclusionCert(l.ctx, key)
}

// Release returns the pin. When it was the last pin on an already evicted view
// the backend reader is closed.
func (l *Lease) Release() {
	if !l.released.CompareAndSwap(false, true) {
		return
	}
	l.cancel()
	l.c.release(l)
}
