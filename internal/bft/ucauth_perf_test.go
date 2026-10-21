package bft

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
)

type authCounters struct{ authority, history, encodes, quorum int64 }

func counts(a *ucAuthenticator) authCounters {
	return authCounters{a.authorityLoads.Load(), a.historyLookups.Load(), a.sealEncodes.Load(), a.quorumChecks.Load()}
}

func (c authCounters) sub(o authCounters) authCounters {
	return authCounters{c.authority - o.authority, c.history - o.history, c.encodes - o.encodes, c.quorum - o.quorum}
}

// fixturesAt builds certificates of distinct seals (one per root round) of one shared authority.
func fixturesAt(t *testing.T, rounds ...uint64) []*prooftest.Fixture {
	var out []*prooftest.Fixture
	for _, r := range rounds {
		out = append(out, prooftest.New(t, 2, 7, prooftest.Hash("root"), r))
	}
	return out
}

// One intake operation (the UC and the view of one response) resolves the authority and the
// history once and judges the shared seal once. Across operations the authority is read once
// and a certified seal costs no further quorum check; the unknown history is looked up once per
// operation, never more. Counted, swept over the number of responses.
func TestUCAuthenticatorWorkPerResponseIsConstant(t *testing.T) {
	for _, n := range []int{1, 4, 16, 64} {
		rounds := make([]uint64, n)
		for i := range rounds {
			rounds[i] = uint64(10 + i)
		}
		fx := fixturesAt(t, rounds...)
		hs := &historyStore{bases: map[uint64]*types.RootTrustBaseV1{1: fx[0].TrustBase}}
		a := newUCAuthenticator(prooftest.NetworkID, hs)
		for _, f := range fx {
			op := a.begin(context.Background())
			require.NoError(t, op.verifyUC(f.UC, prooftest.Partition, f.Shard))
			// the partition view of the same response: judged within the same operation
			require.NoError(t, op.sealVerifier().VerifySeal(f.UC.UnicitySeal))
		}
		got := counts(a)
		require.Equal(t, authCounters{authority: 1, history: int64(n), encodes: int64(n), quorum: int64(n)}, got, "n=%d: distinct seals", n)
		require.Equal(t, 1, len(hs.getCalls)-n, "the epoch's trust base is read once; the n history lookups are the other reads (n=%d)", n)
	}

	// the same certificate delivered by several root nodes: one quorum check in total
	f := fixturesAt(t, 10)[0]
	hs := &historyStore{bases: map[uint64]*types.RootTrustBaseV1{1: f.TrustBase}}
	a := newUCAuthenticator(prooftest.NetworkID, hs)
	const deliveries = 50
	for i := 0; i < deliveries; i++ {
		op := a.begin(context.Background())
		require.NoError(t, op.verifyUC(f.UC, prooftest.Partition, f.Shard))
		require.NoError(t, op.sealVerifier().VerifySeal(f.UC.UnicitySeal))
	}
	require.Equal(t, authCounters{authority: 1, history: deliveries, encodes: deliveries, quorum: 1}, counts(a))
}

// Known history is cached for good (it can never change): further operations do not look it up,
// yet every one still enforces it. Unknown history is never carried across operations.
func TestUCAuthenticatorRetirementIsNeverBypassedByReuse(t *testing.T) {
	ctx := context.Background()
	at := func(round uint64) *prooftest.Fixture { return prooftest.New(t, 2, 7, prooftest.Hash("root"), round) }
	f79, f80, f81 := at(79), at(80), at(81)
	hs := &historyStore{bases: map[uint64]*types.RootTrustBaseV1{1: f80.TrustBase}}
	a := newUCAuthenticator(prooftest.NetworkID, hs)
	verify := func(f *prooftest.Fixture) error {
		op := a.begin(ctx)
		err := op.verifyUC(f.UC, prooftest.Partition, f.Shard)
		if err == nil {
			err = op.sealVerifier().VerifySeal(f.UC.UnicitySeal)
		}
		return err
	}

	// unknown boundary: accepted, and looked up again by the next operation
	require.NoError(t, verify(f80))
	h1 := a.historyLookups.Load()
	require.NoError(t, verify(f80))
	require.Equal(t, h1+1, a.historyLookups.Load(), "an unknown boundary is not remembered")

	// admitted between operations: the very next operation retires the cached seal
	hs.put(nextEpochBase(t, f80, 2, 80))
	require.ErrorContains(t, verify(f80), "retired")
	require.ErrorContains(t, verify(f81), "retired")
	require.NoError(t, verify(f79))

	// now known: no more history lookups, still enforced every time
	known := a.historyLookups.Load()
	for i := 0; i < 20; i++ {
		require.NoError(t, verify(f79))
		require.Error(t, verify(f80))
		require.Error(t, verify(f81))
	}
	require.Equal(t, known, a.historyLookups.Load(), "a known boundary is cached")
	// the boundary is not just remembered by the seal memo: a cold authenticator agrees
	cold := newUCAuthenticator(prooftest.NetworkID, hs)
	require.NoError(t, cold.verifyUC(ctx, f79.UC, prooftest.Partition, f79.Shard))
	require.Error(t, cold.verifyUC(ctx, f80.UC, prooftest.Partition, f80.Shard))
}

// Through random interleavings of certificates, history admissions between operations and
// repeated deliveries, the reusing authenticator accepts exactly what a naive authenticator
// which resolves everything afresh for every call accepts.
func TestUCAuthenticatorMatchesNaiveResolution(t *testing.T) {
	ctx := context.Background()
	rounds := []uint64{5, 30, 59, 60, 61, 90, 120}
	pool := fixturesAt(t, rounds...)
	for seed := int64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		hs := &historyStore{bases: map[uint64]*types.RootTrustBaseV1{1: pool[0].TrustBase}}
		a := newUCAuthenticator(prooftest.NetworkID, hs)
		admitted := false
		for step := 0; step < 120; step++ {
			if !admitted && rng.Intn(10) == 0 {
				hs.put(nextEpochBase(t, pool[0], 2, 60))
				admitted = true
			}
			f := pool[rng.Intn(len(pool))]
			op := a.begin(ctx)
			got := op.verifyUC(f.UC, prooftest.Partition, f.Shard)
			if got == nil {
				got = op.sealVerifier().VerifySeal(f.UC.UnicitySeal)
			}
			naive := newUCAuthenticator(prooftest.NetworkID, hs) // nothing cached
			want := naive.verifyUC(ctx, f.UC, prooftest.Partition, f.Shard)
			if want == nil {
				want = naive.verifySeal(ctx, f.UC.UnicitySeal)
			}
			require.Equal(t, want == nil, got == nil, fmt.Sprintf("seed %d step %d round %d admitted=%v: got %v want %v",
				seed, step, f.UC.UnicitySeal.RootChainRoundNumber, admitted, got, want))
			if admitted && f.UC.UnicitySeal.RootChainRoundNumber >= 60 {
				require.Error(t, got, "retired authority must not certify")
			}
		}
	}
}

// Through the real response entry point: the UC and its sidecar cost one history lookup, one
// seal encoding and (after the first) no authority read and no quorum check per response.
func TestResponseEntryPointWorkIsConstantPerResponse(t *testing.T) {
	root := prooftest.Hash("root")
	f := prooftest.New(t, 2, 7, root, 80)
	hs := &historyStore{bases: map[uint64]*types.RootTrustBaseV1{1: f.TrustBase}}
	rm := &stubRoundManager{}
	c, _ := newProofClient(t, rm, f)
	c.authn = newUCAuthenticator(prooftest.NetworkID, hs)
	c.shardID = f.Shard
	proposeBlock(c, 7, root)
	require.NoError(t, c.handleCertificationResponse(t.Context(), response(f, f.Raw)))
	base := counts(c.authn)
	require.Equal(t, authCounters{authority: 1, history: 1, encodes: 1, quorum: 1}, base, "first response: the UC and the view share one operation")

	for _, n := range []int{1, 8, 32} {
		before := counts(c.authn)
		for i := 0; i < n; i++ {
			require.NoError(t, c.handleCertificationResponse(t.Context(), response(f, f.Raw)))
		}
		require.Equal(t, authCounters{history: int64(n), encodes: int64(n)}, counts(c.authn).sub(before), "n=%d duplicate deliveries", n)
	}
}
