package nifixture

import (
	"errors"

	"github.com/unicitynetwork/bft-core/rootchain/testutils/ucfixture"
	"github.com/unicitynetwork/bft-go-base/types"
)

// Authority is a deterministic test root chain for one unsharded partition (public test keys): it
// certifies the state roots a test commits and hands out the trust base which authenticates them.
// Test use only.
type Authority struct {
	prod  *ucfixture.Producer
	round uint64
	ts    uint64
}

// NewAuthority creates the authority of the unsharded partition at its genesis (initial empty state).
func NewAuthority() (*Authority, error) { return NewShardedAuthority(0) }

// NewShardedAuthority creates the authority of a partition of 2^bits shards (bits 0..3).
func NewShardedAuthority(bits int) (*Authority, error) {
	p, err := ucfixture.New(ucfixture.Config{Partition: Partition, ShardBits: bits})
	if err != nil {
		return nil, err
	}
	return &Authority{prod: p, round: 1, ts: ucfixture.GenesisTimestamp}, nil
}

// ShardID is the ID of the i-th shard in canonical order.
func (a *Authority) ShardID(i int) types.ShardID { return a.prod.Shards()[i].ID }

// CertifyShardRoot certifies the 32 byte state root of shard i and returns that shard's UC.
func (a *Authority) CertifyShardRoot(i int, root []byte) (*types.UnicityCertificate, []byte, error) {
	if len(root) != 32 {
		return nil, nil, errors.New("state root must be 32 bytes")
	}
	return a.nextShard(i, ucfixture.Change{Shard: i, Hash: root})
}

// TrustBase is the authority's trust base.
func (a *Authority) TrustBase() *types.RootTrustBaseV1 { return a.prod.TrustBase() }

// ShardConfHash is the configuration hash committed in the UCs.
func (a *Authority) ShardConfHash() []byte { return a.prod.Shards()[0].ConfigHash }

// CertifyRoot certifies the 32 byte state root in the next root chain round and returns the
// ordinary UC of the partition and its native encoding.
func (a *Authority) CertifyRoot(root []byte) (*types.UnicityCertificate, []byte, error) {
	if len(root) != 32 {
		return nil, nil, errors.New("state root must be 32 bytes")
	}
	return a.next(ucfixture.Change{Shard: 0, Hash: root})
}

// CertifyRepeat certifies a timeout repeat of the previous state (the initial empty state when
// nothing was certified before).
func (a *Authority) CertifyRepeat() (*types.UnicityCertificate, []byte, error) {
	return a.next(ucfixture.Change{Shard: 0, Timeout: true})
}

func (a *Authority) next(c ucfixture.Change) (*types.UnicityCertificate, []byte, error) {
	return a.nextShard(0, c)
}

func (a *Authority) nextShard(i int, c ucfixture.Change) (*types.UnicityCertificate, []byte, error) {
	a.round++
	a.ts += 10
	blk, err := a.prod.Next(ucfixture.BlockSpec{Round: a.round, Timestamp: a.ts, Changes: []ucfixture.Change{c}})
	if err != nil {
		return nil, nil, err
	}
	return blk.Shards[i].UC, blk.Shards[i].UCBytes, nil
}
