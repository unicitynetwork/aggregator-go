package client

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
)

// balancedScheme is the complete scheme of 2^depth shards.
func balancedScheme(depth int) []types.ShardID {
	ids := []types.ShardID{{}}
	for range depth {
		var next []types.ShardID
		for _, id := range ids {
			a, b := id.Split()
			next = append(next, a, b)
		}
		ids = next
	}
	return ids
}

// chainScheme is the maximally unbalanced complete scheme: 0, 10, 110, ..., 1^(n-1)0, 1^n.
func chainScheme(n int) []types.ShardID {
	var ids []types.ShardID
	cur := types.ShardID{}
	for range n {
		a, b := cur.Split()
		ids = append(ids, a)
		cur = b
	}
	return append(ids, cur)
}

func endpoints(ids []types.ShardID) []Endpoint {
	eps := make([]Endpoint, len(ids))
	for i, id := range ids {
		eps[i] = Endpoint{ShardID: id, URL: "http://x"}
	}
	return eps
}

// the slow reference model: scan every endpoint with the shard comparator
func scanLookup(eps []Endpoint, key []byte) int {
	for i, ep := range eps {
		if ep.ShardID.Length() <= uint(len(key)*8) && ep.ShardID.Comparator()(key) {
			return i
		}
	}
	return -1
}

func TestRouter_AgreesWithLinearScan(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for name, ids := range map[string][]types.ShardID{
		"singleton": {{}}, "2": balancedScheme(1), "8": balancedScheme(3), "256": balancedScheme(8),
		"chain 16": chainScheme(16), "chain 64": chainScheme(64),
	} {
		eps := endpoints(ids)
		// shuffle: routing must not depend on endpoint order
		rng.Shuffle(len(eps), func(i, j int) { eps[i], eps[j] = eps[j], eps[i] })
		r, err := newRouter(eps)
		require.NoError(t, err, name)
		for range 5000 {
			key := make([]byte, 32)
			rng.Read(key)
			if rng.Intn(3) == 0 { // bias towards long shared prefixes (deep chain shards)
				for i := 0; i < rng.Intn(9); i++ {
					key[i] = 0xff
				}
			}
			got, _ := r.lookup(key)
			require.Equal(t, scanLookup(eps, key), got, name)
		}
	}
}

// Routing work is O(shard depth) per input: it grows with log S for balanced schemes (not with S),
// is exactly linear in the number of inputs, and never exceeds the key width.
func TestRouter_WorkIsDepthNotShardCount(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	keys := func(n int) [][]byte {
		out := make([][]byte, n)
		for i := range out {
			out[i] = make([]byte, 32)
			rng.Read(out[i])
		}
		return out
	}
	for depth := 0; depth <= 10; depth++ { // S = 1 .. 1024
		r, err := newRouter(endpoints(balancedScheme(depth)))
		require.NoError(t, err)
		for _, k := range keys(50) {
			_, v := r.lookup(k)
			require.Equal(t, depth+1, v, "balanced scheme of %d shards: visits = depth+1", 1<<depth)
		}
	}
	// linear in L at a fixed scheme
	r, err := newRouter(endpoints(balancedScheme(6)))
	require.NoError(t, err)
	total := func(l int) int {
		sum := 0
		for _, k := range keys(l) {
			_, v := r.lookup(k)
			sum += v
		}
		return sum
	}
	for _, l := range []int{1, 10, 100, 1000} {
		require.Equal(t, l*7, total(l))
	}
	// the unbalanced chain is bounded by its depth, still no scan over all endpoints per input
	for _, n := range []int{8, 64, 200} {
		r, err := newRouter(endpoints(chainScheme(n)))
		require.NoError(t, err)
		worst := 0
		for _, k := range keys(2000) {
			_, v := r.lookup(k)
			worst = max(worst, v)
		}
		require.LessOrEqual(t, worst, n+1)
	}
}

func TestRouter_RejectsOverlap(t *testing.T) {
	a, _ := types.ShardID{}.Split()
	_, err := newRouter(endpoints([]types.ShardID{{}, a}))
	require.Error(t, err)
	_, err = newRouter(endpoints([]types.ShardID{a, a}))
	require.Error(t, err)
}
