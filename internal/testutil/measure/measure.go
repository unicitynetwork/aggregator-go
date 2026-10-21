// Package measure supports the opt-in scaling measurements (UC_MEASURE=1; see
// docs/uc-consolidation-measurements.md): hardware independent cost counters,
// log-log slope fitting, and natively verified realistic certification material.
package measure

import (
	"crypto"
	"crypto/sha256"
	"fmt"
	"math"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	testsig "github.com/unicitynetwork/bft-go-base/testutils/sig"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Require skips the calling test unless UC_MEASURE is set.
func Require(t testing.TB) {
	t.Helper()
	if os.Getenv("UC_MEASURE") == "" {
		t.Skip("set UC_MEASURE=1 to run the scaling measurements")
	}
}

// Cost is the per-call cost of an operation. Allocs and Bytes are deterministic
// for deterministic code; Nanos is wall time and only meaningful relatively.
type Cost struct {
	Allocs, Bytes, Nanos float64
}

// CostOf runs fn repeatedly (until about minTime has elapsed) and returns the
// average per-call allocation count, allocated bytes and wall time.
func CostOf(minTime time.Duration, fn func()) Cost {
	fn() // warm up
	var before, after runtime.MemStats
	n := 1
	for {
		runtime.GC()
		runtime.ReadMemStats(&before)
		start := time.Now()
		for i := 0; i < n; i++ {
			fn()
		}
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		if elapsed >= minTime || n >= 1<<20 {
			return Cost{
				Allocs: float64(after.Mallocs-before.Mallocs) / float64(n),
				Bytes:  float64(after.TotalAlloc-before.TotalAlloc) / float64(n),
				Nanos:  float64(elapsed.Nanoseconds()) / float64(n),
			}
		}
		n *= 2
	}
}

// CostOfN runs fn exactly n times (no warm-up) and returns the per-call cost; for
// operations that consume a finite pre-built input.
func CostOfN(n int, fn func()) Cost {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()
	for i := 0; i < n; i++ {
		fn()
	}
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	return Cost{
		Allocs: float64(after.Mallocs-before.Mallocs) / float64(n),
		Bytes:  float64(after.TotalAlloc-before.TotalAlloc) / float64(n),
		Nanos:  float64(elapsed.Nanoseconds()) / float64(n),
	}
}

// Slope is the least-squares slope of log y against log x: a power law x^a has
// slope a. Non-positive points are skipped; a constant-zero series has slope 0.
func Slope(xs, ys []float64) float64 {
	var sx, sy, sxx, sxy, n float64
	for i := range xs {
		if xs[i] <= 0 || ys[i] <= 0 {
			continue
		}
		lx, ly := math.Log(xs[i]), math.Log(ys[i])
		sx, sy, sxx, sxy, n = sx+lx, sy+ly, sxx+lx*lx, sxy+lx*ly, n+1
	}
	if n < 2 || n*sxx-sx*sx == 0 {
		return 0
	}
	return (n*sxy - sx*sy) / (n*sxx - sx*sx)
}

// FmtSlope formats Slope with two decimals.
func FmtSlope(xs, ys []float64) string { return fmt.Sprintf("%.2f", Slope(xs, ys)) }

func h32(label string) []byte {
	h := sha256.Sum256([]byte(label))
	return h[:]
}

// PartitionID of the measured partition.
const PartitionID types.PartitionID = api.AggregationPartitionID

// Certification is complete ordinary UC material for one shard of an S-shard
// partition, bound to a given state root, plus the encoded partition view.
type Certification struct {
	Shards  int
	Signers int
	UC      []byte // the shard's ordinary unicity certificate (CBOR), seal with Signers signatures
	View    []byte // PartitionViewV1 (CBOR); nil when Shards exceeds the production cap
}

// NewCertification builds native certification material for shard 0 of an
// S-shard partition whose input record certifies root, with a seal signed by
// `signers` validators of a trust base of that size. The material is verified
// natively (UC.Verify, including the quorum of seal signatures) before it is
// returned, so measured sizes are those of a UC a real verifier accepts.
func NewCertification(t *testing.T, shards, signers int, root []byte) Certification {
	t.Helper()
	return NewCertificationRound(t, shards, signers, root, 50)
}

// NewCertificationRound is NewCertification with the seal at the given round
// after the trust base's epoch start (later rounds supersede earlier ones).
func NewCertificationRound(t *testing.T, shards, signers int, root []byte, round uint64) Certification {
	t.Helper()
	scheme := types.ShardingScheme{}
	ids := []types.ShardID{{}}
	for n := shards; n > 1; n /= 2 {
		var next []types.ShardID
		for _, id := range ids {
			a, b := id.Split()
			next = append(next, a, b)
		}
		ids = next
	}
	if shards > 1 {
		scheme = types.ShardingScheme(ids)
	}
	order := ids
	nodes := make([]*types.NodeInfo, signers)
	signersList := make([]signerPair, signers)
	for i := range nodes {
		s, v := testsig.CreateSignerAndVerifier(t)
		key, err := v.MarshalPublicKey()
		require.NoError(t, err)
		nodes[i] = &types.NodeInfo{NodeID: fmt.Sprintf("validator-%d", i), SigKey: key, Stake: 1}
		signersList[i] = signerPair{id: nodes[i].NodeID, s: s}
	}
	tb, err := types.NewTrustBase(types.NetworkMainNet, nodes)
	require.NoError(t, err)

	inputs := make([]types.ShardTreeInput, len(order))
	view := &types.PartitionViewV1{Version: 1, PartitionID: PartitionID}
	for i, sid := range order {
		hash := h32(fmt.Sprint("state", i))
		if i == 0 {
			hash = root
		}
		ir := &types.InputRecord{Version: 1, RoundNumber: 1000 + uint64(i), PreviousHash: h32(fmt.Sprint("prev", i)), Hash: hash,
			BlockHash: h32(fmt.Sprint("block", i)), SummaryValue: []byte{}, Timestamp: 1755000000}
		inputs[i] = types.ShardTreeInput{Shard: sid, IR: ir, TRHash: h32(fmt.Sprint("tr", i)), ShardConfHash: h32(fmt.Sprint("conf", i))}
		view.Leaves = append(view.Leaves, types.PartitionViewLeafV1{ShardID: sid, InputRecord: ir, TRHash: inputs[i].TRHash, ShardConfHash: inputs[i].ShardConfHash})
	}
	tree, err := types.CreateShardTree(scheme, inputs, crypto.SHA256)
	require.NoError(t, err)
	ut, err := types.NewUnicityTree(crypto.SHA256, []*types.UnicityTreeData{
		{Partition: PartitionID, ShardTreeRoot: tree.RootHash()},
		{Partition: 0x01010101, ShardTreeRoot: h32("other partition")},
		{Partition: 0x02020202, ShardTreeRoot: h32("another partition")},
	})
	require.NoError(t, err)
	utc, err := ut.Certificate(PartitionID)
	require.NoError(t, err)
	stc, err := tree.Certificate(order[0])
	require.NoError(t, err)

	seal := &types.UnicitySeal{Version: 1, NetworkID: tb.GetNetworkID(), Epoch: tb.GetEpoch(), RootChainRoundNumber: tb.GetEpochStart() + round,
		Timestamp: 1755000000, PreviousHash: h32("previous root"), Hash: ut.RootHash()}
	for _, sp := range signersList {
		require.NoError(t, seal.Sign(sp.id, sp.s))
	}
	uc := &types.UnicityCertificate{Version: 1, InputRecord: inputs[0].IR, TRHash: inputs[0].TRHash, ShardConfHash: inputs[0].ShardConfHash,
		ShardTreeCertificate: stc, UnicityTreeCertificate: utc, UnicitySeal: seal}
	// full native verification, including the seal quorum
	require.NoError(t, uc.Verify(tb, crypto.SHA256, PartitionID, order[0], inputs[0].ShardConfHash))
	require.Len(t, seal.Signatures, signers)
	ucBytes, err := uc.MarshalCBOR()
	require.NoError(t, err)

	c := Certification{Shards: shards, Signers: signers, UC: ucBytes}
	if shards <= types.MaxPartitionViewLeaves {
		c.View, err = view.MarshalCBOR()
		require.NoError(t, err)
		decoded, err := types.DecodePartitionViewV1(c.View)
		require.NoError(t, err)
		require.Len(t, decoded.Leaves, shards)
	}
	return c
}

type signerPair struct {
	id string
	s  abcrypto.Signer
}
