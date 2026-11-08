// Package utbtest builds signed trust-base chains and bundle files for tests.
package utbtest

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"
)

// Network is the network id of every chain built here.
const Network types.NetworkID = 5

// Chain is a signed chain: Entries[i] is epoch i+1, started at round 1+100*i.
type Chain struct {
	Entries []*types.RootTrustBaseV2
	Raw     [][]byte
	Signers []abcrypto.Signer
	// Salt is mixed into the state hash, so two chains over the same members and epochs have different entries (a conflicting history).
	Salt byte
}

func signer(t testing.TB, i int) abcrypto.Signer {
	s, err := abcrypto.NewInMemorySecp256K1SignerFromKey(append(make([]byte, 30), byte((i+1)>>8), byte(i+1)))
	require.NoError(t, err)
	return s
}

// NewChain builds epochs 1..n of a committee of `members` weight-1 nodes, each entry signed by a bare quorum of its predecessor's members. The salt
// changes every entry after the genesis: two chains with different salts share their genesis and fork at epoch 2.
func NewChain(t testing.TB, members, n int, salt byte) *Chain {
	return NewChainOn(t, Network, members, n, salt)
}

// NewChainOn is NewChain for another network.
func NewChainOn(t testing.TB, network types.NetworkID, members, n int, salt byte) *Chain {
	c := &Chain{Salt: salt}
	nodes := make([]*types.NodeInfo, members)
	for i := range nodes {
		c.Signers = append(c.Signers, signer(t, i))
		v, err := c.Signers[i].Verifier()
		require.NoError(t, err)
		pk, err := v.MarshalPublicKey()
		require.NoError(t, err)
		nodes[i] = &types.NodeInfo{NodeID: fmt.Sprintf("n%03d", i), SigKey: pk, Stake: 1}
	}
	quorum := uint64(members)*2/3 + 1
	for k := 0; k < n; k++ {
		e := &types.RootTrustBaseV2{Version: 2, NetworkID: network, Epoch: uint64(k + 1), EpochStart: uint64(1 + 100*k), RootNodes: nodes,
			QuorumThreshold: quorum, StateHash: stateHash(salt, k), PreviousEntryID: make([]byte, types.TrustBaseV2IDLen)}
		if k > 0 {
			prev, err := c.Entries[k-1].EntryID()
			require.NoError(t, err)
			e.PreviousEntryID = prev[:]
			for i := 0; i < int(quorum); i++ {
				require.NoError(t, e.Sign(nodes[i].NodeID, c.Signers[i]))
			}
		}
		raw, err := e.MarshalCBOR()
		require.NoError(t, err)
		c.Entries, c.Raw = append(c.Entries, e), append(c.Raw, raw)
	}
	return c
}

func stateHash(salt byte, k int) []byte {
	h := make([]byte, types.TrustBaseV2IDLen)
	h[1] = byte(k)
	if k > 0 {
		h[0] = salt
	}
	return h
}

// Pin is the genesis EntryID.
func (c *Chain) Pin(t testing.TB) [types.TrustBaseV2IDLen]byte {
	id, err := c.Entries[0].EntryID()
	require.NoError(t, err)
	return id
}

// ID is the EntryID of epoch e.
func (c *Chain) ID(t testing.TB, epoch uint64) [types.TrustBaseV2IDLen]byte {
	id, err := c.Entries[epoch-1].EntryID()
	require.NoError(t, err)
	return id
}

// Bundle encodes epochs 1..upTo as a release bundle.
func (c *Chain) Bundle(t testing.TB, upTo int) []byte {
	b, err := types.EncodeTrustBaseBundle(c.Raw[:upTo])
	require.NoError(t, err)
	return b
}

// WriteFile writes bytes into a fresh temp directory and returns the path.
func WriteFile(t testing.TB, name string, data []byte) string {
	p := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(p, data, 0o600))
	return p
}

// V1 returns the version 1 trust base carrying the same authority as epoch e of the chain (what the aggregator holds for it).
func (c *Chain) V1(epoch uint64) *types.RootTrustBaseV1 {
	e := c.Entries[epoch-1]
	return &types.RootTrustBaseV1{Version: 1, NetworkID: e.NetworkID, Epoch: e.Epoch, EpochStart: e.EpochStart, RootNodes: e.RootNodes,
		QuorumThreshold: e.QuorumThreshold}
}
