package proofview

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// compatUC is a UC for root with the accepted input record round/epoch/block hash and configuration given.
func compatUC(t testing.TB, root []byte, irRound, epoch uint64, blockHash, prevHash, conf []byte, sealRound uint64) []byte {
	t.Helper()
	uc := types.UnicityCertificate{
		Version: 1, ShardConfHash: conf,
		InputRecord: &types.InputRecord{Version: 1, RoundNumber: irRound, Epoch: epoch, Hash: root, PreviousHash: prevHash, BlockHash: blockHash, SummaryValue: []byte{}},
		UnicitySeal: &types.UnicitySeal{Version: 1, RootChainRoundNumber: sealRound},
	}
	b, err := types.Cbor.Marshal(&uc)
	require.NoError(t, err)
	return b
}

func decodeTestUC(t testing.TB, b []byte) *types.UnicityCertificate {
	var uc types.UnicityCertificate
	require.NoError(t, types.Cbor.Unmarshal(b, &uc))
	return &uc
}

// oldScan is the previous implementation: walk the same-root chain newest first, decode each candidate's latest
// certification and compare its accepted input record and configuration.
func oldScan(t testing.TB, c *Catalog, root []byte, want *types.UnicityCertificate) (BlockIdentity, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for v := c.byRoot[string(root)]; v != nil; v = v.rootOlder {
		if v.attTail == nil {
			continue
		}
		ref := decodeTestUC(t, v.attTail.uc)
		a, b := ref.InputRecord, want.InputRecord
		if a.RoundNumber == b.RoundNumber && a.Epoch == b.Epoch && bytes.Equal(a.Hash, b.Hash) &&
			bytes.Equal(a.PreviousHash, b.PreviousHash) && bytes.Equal(a.BlockHash, b.BlockHash) &&
			bytes.Equal(ref.ShardConfHash, want.ShardConfHash) {
			c.compatProbes++ // every decoded candidate is a probe of the scan
			return v.block.clone(), true
		}
		c.compatProbes++
	}
	return BlockIdentity{}, false
}

func publishSameRoot(t testing.TB, c *Catalog, w *countingWorld, n int, root []byte, uc []byte) {
	t.Helper()
	require.NoError(t, c.Publish(context.Background(), PublishInput{
		Block: BlockIdentity{Number: api.NewBigIntFromUint64(uint64(n)), ConfigLineage: []byte("cfg")},
		Root:  root, Reader: w.newReader(root, 100), Cert: Certification{UC: uc},
	}))
}

// With K retained states sharing one root (each with its own accepted input record), the lookup of the newest
// compatible one is a single probe whichever it is; the previous scan examined up to K candidates.
func TestSameRootCompatibleLookupIsConstantInK(t *testing.T) {
	root := rootN(7)
	var idx, scan []float64
	for _, k := range []int{8, 16, 32, 64, 128} {
		w := &countingWorld{}
		c, err := NewCatalog(scalingLimits(k), WithAccountant(w))
		require.NoError(t, err)
		ucs := make([]*types.UnicityCertificate, k+1)
		for i := 1; i <= k; i++ {
			b := compatUC(t, root, uint64(i), 1, []byte{byte(i)}, []byte{9}, []byte("conf"), uint64(i))
			publishSameRoot(t, c, w, i, root, b)
			ucs[i] = decodeTestUC(t, b)
		}
		checkInvariants(t, c)
		require.Equal(t, k, c.Stats().Views)
		// the OLDEST state is the worst case of the scan
		before := c.compatProbes
		for rep := 0; rep < 20; rep++ {
			id, ok := c.ViewForRootCompatible(root, CompatDigest(ucs[1]))
			require.True(t, ok)
			require.EqualValues(t, 1, id.Number.Uint64())
		}
		idx = append(idx, float64(c.compatProbes-before)/20)
		// an incompatible repeat: a miss, then the "is the root known" question: both single probes
		before = c.compatProbes
		other := decodeTestUC(t, compatUC(t, root, 1, 2, []byte{1}, []byte{9}, []byte("conf"), 1)) // other epoch
		_, ok := c.ViewForRootCompatible(root, CompatDigest(other))
		require.False(t, ok)
		prev := decodeTestUC(t, compatUC(t, root, 1, 1, []byte{1}, []byte{8}, []byte("conf"), 1)) // other previous hash
		_, ok = c.ViewForRootCompatible(root, CompatDigest(prev))
		require.False(t, ok, "the previous hash is part of the binding")
		require.True(t, c.HasCertifiedRoot(root))
		require.Equal(t, uint64(3), c.compatProbes-before)

		before = c.compatProbes
		for rep := 0; rep < 20; rep++ {
			id, ok := oldScan(t, c, root, ucs[1])
			require.True(t, ok)
			require.EqualValues(t, 1, id.Number.Uint64())
		}
		scan = append(scan, float64(c.compatProbes-before)/20)
		// The answer does not come from the same-root chain: sever it (and the by-root head) and the oldest state
		// is still found, by the index alone.
		c.mu.Lock()
		for v := c.head; v != nil; v = v.next {
			v.rootOlder, v.rootNewer = nil, nil
		}
		c.byRoot = map[string]*view{}
		c.mu.Unlock()
		for _, i := range []int{1, k / 2, k} {
			id, ok := c.ViewForRootCompatible(root, CompatDigest(ucs[i]))
			require.True(t, ok)
			require.EqualValues(t, i, id.Number.Uint64())
		}
		require.True(t, c.HasCertifiedRoot(root))
		require.NoError(t, c.Close())
	}
	for _, p := range idx {
		require.Equal(t, 1.0, p, "probes per lookup: %v", idx)
	}
	require.Equal(t, 128.0, scan[len(scan)-1], "the reference scan examines all K candidates for the oldest: %v", scan)
	require.Equal(t, 8.0, scan[0])
}

// On random sequences of publications, repeats, evictions and certification updates, the index returns exactly
// what the previous scan returned (newest compatible state; none when none is compatible), and "root known"
// equals the old "any certified view of the root".
func TestCompatibleLookupEqualsTheScanOnRandomSequences(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		w := &countingWorld{}
		c, err := NewCatalog(scalingLimits(6), WithAccountant(w))
		require.NoError(t, err)
		roots := [][]byte{rootN(1), rootN(2), rootN(3)}
		confs := [][]byte{[]byte("c1"), []byte("c2")}
		var probes []*types.UnicityCertificate
		n := 0
		mk := func() (root, uc []byte) {
			root = roots[rng.Intn(len(roots))]
			uc = compatUC(t, root, uint64(rng.Intn(3)), uint64(rng.Intn(2)), []byte{byte(rng.Intn(2))}, []byte{byte(rng.Intn(2))}, confs[rng.Intn(2)], uint64(1+rng.Intn(6)))
			return
		}
		for step := 0; step < 120; step++ {
			switch rng.Intn(3) {
			case 0: // publish a new state (evicts when over the limit)
				n++
				root, uc := mk()
				publishSameRoot(t, c, w, n, root, uc)
				probes = append(probes, decodeTestUC(t, uc))
			case 1: // a later certification for a random retained block (repeat/timeout attach)
				if n == 0 {
					continue
				}
				root, uc := mk()
				d := decodeTestUC(t, uc)
				if id, ok := c.ViewForRootCompatible(root, CompatDigest(d)); ok {
					_ = c.AttachCertification(context.Background(), id, root, Certification{UC: uc})
				} else if rng.Intn(2) == 0 {
					// a higher-round certification attached to the newest view of the root, changing its latest
					c.mu.Lock()
					v := c.byRoot[string(root)]
					var id BlockIdentity
					if v != nil {
						id = v.block.clone()
					}
					c.mu.Unlock()
					if v != nil {
						_ = c.AttachCertification(context.Background(), id, root, Certification{UC: uc})
					}
				}
				probes = append(probes, d)
			default:
			}
			checkInvariants(t, c)
			for _, p := range probes[max(0, len(probes)-8):] {
				root := p.InputRecord.Hash
				got, gok := c.ViewForRootCompatible(root, CompatDigest(p))
				want, wok := oldScan(t, c, root, p)
				require.Equal(t, wok, gok, "seed %d step %d", seed, step)
				if wok {
					require.Equal(t, want.Number.Uint64(), got.Number.Uint64(), "seed %d step %d", seed, step)
				}
				any := false
				c.mu.Lock()
				for v := c.byRoot[string(root)]; v != nil; v = v.rootOlder {
					any = any || v.attTail != nil
				}
				c.mu.Unlock()
				require.Equal(t, any, c.HasCertifiedRoot(root))
			}
		}
		require.NoError(t, c.Close())
	}
}
