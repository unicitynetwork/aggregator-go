package integration

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// phaseCount is what one measurement phase observed, per shard.
type phaseCount struct {
	Responses   []float64 // certification responses received from the root
	Blocks      []float64 // finalized blocks (new certified states)
	NonProgress []float64 // responses certifying no new shard block (re-deliveries, timeout repeats, extra traffic)
	Handshakes  []float64 // handshakes sent to the root (each may be answered by a re-delivery)
	Submitted   []float64 // accepted submissions
	Rounds      float64   // root rounds elapsed
}

func (p phaseCount) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "rounds=%.0f", p.Rounds)
	for i := range p.Responses {
		fmt.Fprintf(&sb, " | shard %d: submitted=%.0f blocks=%.0f responses=%.0f nonprogress=%.0f handshakes=%.0f", i, p.Submitted[i], p.Blocks[i], p.Responses[i], p.NonProgress[i], p.Handshakes[i])
	}
	return sb.String()
}

// checkWriteProgress requires every shard to have accepted writes and finalized
// new states during the phase.
func checkWriteProgress(p phaseCount) error {
	for i := range p.Responses {
		if p.Submitted[i] < 3 {
			return fmt.Errorf("shard %d accepted only %.0f writes: continuous writes are not running", i, p.Submitted[i])
		}
		if p.Blocks[i] < 1 {
			return fmt.Errorf("shard %d finalized no new block during the phase", i)
		}
	}
	return nil
}

// checkMessageRate bounds the root->shard traffic by the certification workload, per shard:
//
//   - every response either certifies a new shard block or is non-progress (re-delivery, timeout
//     repeat, or any extra traffic whatever its root round): progress responses may not exceed the
//     finalized blocks (+1 for a block finalized at the phase boundary), so extra traffic cannot hide
//     behind advancing root rounds;
//   - non-progress responses may not exceed the equal-rounds baseline plus the re-deliveries a
//     handshake legitimately triggers (one each);
//   - consolidation may not cause extra handshakes beyond the baseline (+1 jitter), so they cannot
//     excuse extra traffic;
//   - every finalized block must have been delivered.
func checkMessageRate(base, with phaseCount) error {
	scale := 1.0
	if base.Rounds > 0 && with.Rounds > base.Rounds {
		scale = with.Rounds / base.Rounds // phases may span different numbers of root rounds
	}
	for i := range with.Responses {
		if with.Responses[i] < with.Blocks[i]-1 {
			return fmt.Errorf("shard %d: %.0f responses for %.0f finalized blocks (missing deliveries)", i, with.Responses[i], with.Blocks[i])
		}
		if progress := with.Responses[i] - with.NonProgress[i]; progress > with.Blocks[i]+1 {
			return fmt.Errorf("shard %d: %.0f progress responses for %.0f finalized blocks: additional root-to-shard traffic", i, progress, with.Blocks[i])
		}
		if with.Handshakes[i] > base.Handshakes[i]*scale+1 {
			return fmt.Errorf("shard %d: %.0f handshakes (baseline %.0f): consolidation caused extra handshakes", i, with.Handshakes[i], base.Handshakes[i])
		}
		if allowed := base.NonProgress[i]*scale + with.Handshakes[i]; with.NonProgress[i] > allowed {
			return fmt.Errorf("shard %d: %.0f non-progress responses exceed %.0f (baseline %.0f scaled + %.0f handshakes) out of %.0f: additional root-to-shard traffic",
				i, with.NonProgress[i], allowed, base.NonProgress[i], with.Handshakes[i], with.Responses[i])
		}
	}
	return nil
}

func pc(responses, blocks, nonprogress, handshakes, submitted []float64, rounds float64) phaseCount {
	return phaseCount{Responses: responses, Blocks: blocks, NonProgress: nonprogress, Handshakes: handshakes, Submitted: submitted, Rounds: rounds}
}

func vec(v ...float64) []float64 { return v }

func TestMessageRateCheck(t *testing.T) {
	base := pc(vec(8, 9, 8, 7), vec(7, 8, 7, 7), vec(1, 1, 1, 0), vec(0, 0, 0, 0), vec(40, 40, 40, 40), 20)
	with := pc(vec(8, 8, 9, 7), vec(8, 7, 8, 7), vec(0, 1, 1, 0), vec(0, 0, 0, 0), vec(40, 40, 40, 40), 20)
	require.NoError(t, checkMessageRate(base, with))

	// every response sent twice
	doubled := with
	doubled.Responses = vec(16, 16, 18, 14)
	doubled.NonProgress = vec(8, 8, 9, 7)
	require.Error(t, checkMessageRate(base, doubled))

	// the review's repro: extra traffic carrying ADVANCING root rounds (no new shard block): baseline
	// 2 blocks / 2 responses, consolidation phase 2 blocks / 16 responses. The old Repeats-only gate
	// accepted it; as non-progress responses it fails at the minimum live workload.
	minBase := pc(vec(2, 2), vec(2, 2), vec(0, 0), vec(0, 0), vec(50, 50), 21)
	extra := pc(vec(16, 2), vec(2, 2), vec(14, 0), vec(0, 0), vec(50, 50), 21)
	require.NoError(t, checkWriteProgress(minBase))
	require.NoError(t, checkWriteProgress(extra))
	require.ErrorContains(t, checkMessageRate(minBase, extra), "shard 0")
	// even if the counter missed it (all responses counted as progress) the progress bound catches it
	hidden := extra
	hidden.NonProgress = vec(0, 0)
	require.ErrorContains(t, checkMessageRate(minBase, hidden), "progress responses")

	// one finalized block with one extra response is detected too
	one := pc(vec(2, 1), vec(1, 1), vec(1, 0), vec(0, 0), vec(50, 50), 21)
	require.ErrorContains(t, checkMessageRate(pc(vec(1, 1), vec(1, 1), vec(0, 0), vec(0, 0), vec(50, 50), 21), one), "shard 0")

	// the baseline's own timeout repeats are scaled to the longer phase, not blamed on consolidation
	noisy := pc(vec(5, 5), vec(3, 3), vec(2, 2), vec(0, 0), vec(40, 40), 20)
	longer := pc(vec(7, 7), vec(3, 3), vec(3, 3), vec(0, 0), vec(40, 40), 30)
	require.NoError(t, checkMessageRate(noisy, longer))

	// missing deliveries are detected too
	missing := with
	missing.Responses = vec(2, 8, 9, 7)
	require.ErrorContains(t, checkMessageRate(base, missing), "missing deliveries")
}

func TestMessageRateCheck_HandshakesCannotExcuseTraffic(t *testing.T) {
	base := pc(vec(3, 3), vec(3, 3), vec(0, 0), vec(0, 0), vec(50, 50), 21)
	// one handshake answered by one re-delivery: fine
	ok := pc(vec(4, 3), vec(3, 3), vec(1, 0), vec(1, 0), vec(50, 50), 21)
	require.NoError(t, checkMessageRate(base, ok))
	// two re-deliveries for one handshake are traffic
	two := pc(vec(5, 3), vec(3, 3), vec(2, 0), vec(1, 0), vec(50, 50), 21)
	require.ErrorContains(t, checkMessageRate(base, two), "shard 0")
	// consolidation-caused handshakes beyond the baseline cannot excuse extra re-deliveries
	many := pc(vec(7, 3), vec(3, 3), vec(4, 0), vec(4, 0), vec(50, 50), 21)
	require.ErrorContains(t, checkMessageRate(base, many), "extra handshakes")
}

func TestWriteProgressCheck(t *testing.T) {
	ok := phaseCount{Responses: []float64{5, 5}, Blocks: []float64{4, 4}, Submitted: []float64{30, 30}}
	require.NoError(t, checkWriteProgress(ok))
	// stopped writers: no submissions
	stopped := phaseCount{Responses: []float64{1, 1}, Blocks: []float64{0, 0}, Submitted: []float64{0, 0}}
	require.ErrorContains(t, checkWriteProgress(stopped), "continuous writes are not running")
	// submissions but no finalized block on one shard
	stalled := phaseCount{Responses: []float64{5, 1}, Blocks: []float64{4, 0}, Submitted: []float64{30, 30}}
	require.ErrorContains(t, checkWriteProgress(stalled), "shard 1 finalized no new block")
}
