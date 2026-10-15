package integration

import (
	"fmt"
	"math"
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
	MaxGap      []float64 // longest time (s) the shard went without a new finalized block during the phase
	// Stalls is the number of stalls (a gap of at least T2 without a finalized block) that ended
	// with finalization resuming inside the phase; StallBlocks the blocks finalized in the
	// catch-up burst right after such stalls, which the root may deliver coalesced.
	Stalls      []float64
	StallBlocks []float64
	Rounds      float64 // root rounds elapsed
}

func (p phaseCount) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "rounds=%.0f", p.Rounds)
	for i := range p.Responses {
		fmt.Fprintf(&sb, " | shard %d: submitted=%.0f blocks=%.0f responses=%.0f nonprogress=%.0f handshakes=%.0f maxgap=%.1fs", i, p.Submitted[i], p.Blocks[i], p.Responses[i], p.NonProgress[i], p.Handshakes[i], p.MaxGap[i])
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

// t2Seconds is the partition's T2 timeout (see buildShardConf): only after that long without a
// certified block may the root answer a shard with a timeout repeat.
const t2Seconds = 5.0

// checkMessageRate bounds the root->shard traffic by the certification workload, per shard:
//
//   - every response either certifies a new shard block or is non-progress (re-delivery, timeout
//     repeat, or any extra traffic whatever its root round): progress responses may not exceed the
//     finalized blocks (+1 for a block finalized at the phase boundary), so extra traffic cannot hide
//     behind advancing root rounds;
//   - non-progress responses are only legitimate as timeout repeats, which need a T2 gap without a
//     finalized block (so floor(maxGap/T2) at most), and as the one re-delivery each handshake
//     triggers; with no such gap a single duplicate per block is detected;
//   - consolidation may not cause extra handshakes beyond the baseline (+1 jitter), so they cannot
//     excuse extra traffic;
//   - every finalized block must have been delivered.
func checkMessageRate(base, with phaseCount) error {
	scale := 1.0
	if base.Rounds > 0 && with.Rounds > base.Rounds {
		scale = with.Rounds / base.Rounds // phases may span different numbers of root rounds
	}
	for i := range with.Responses {
		// delivery lower bound: blocks outside a stall's catch-up burst are delivered one response each
		// (-1 for a block at the phase boundary); the burst after each stall may be coalesced by the root
		// but yields at least one response per stall. A stalled phase therefore still needs responses
		// (0 responses for 9 blocks never passes).
		required := math.Max(0, with.Blocks[i]-with.StallBlocks[i]-1) + with.Stalls[i]
		if with.Responses[i] < required {
			return fmt.Errorf("shard %d: %.0f responses for %.0f finalized blocks, at least %.0f required (%.0f blocks outside %.0f stall(s) + 1 per stall): missing deliveries",
				i, with.Responses[i], with.Blocks[i], required, math.Max(0, with.Blocks[i]-with.StallBlocks[i]-1), with.Stalls[i])
		}
		// a stall only in the consolidation phase is a regression to explain, not noise: it is compared with
		// the baseline phase's own worst gap (+1 s sampling slack)
		if with.MaxGap[i] >= t2Seconds && with.MaxGap[i] > base.MaxGap[i]+1 {
			return fmt.Errorf("shard %d: finalization stalled for %.1fs during consolidation (baseline worst gap %.1fs)", i, with.MaxGap[i], base.MaxGap[i])
		}
		if progress := with.Responses[i] - with.NonProgress[i]; progress > with.Blocks[i]+1 {
			return fmt.Errorf("shard %d: %.0f progress responses for %.0f finalized blocks: additional root-to-shard traffic", i, progress, with.Blocks[i])
		}
		if with.Handshakes[i] > base.Handshakes[i]*scale+1 {
			return fmt.Errorf("shard %d: %.0f handshakes (baseline %.0f): consolidation caused extra handshakes", i, with.Handshakes[i], base.Handshakes[i])
		}
		timeoutRepeats := math.Floor((with.MaxGap[i] + 0.5) / t2Seconds)
		if allowed := timeoutRepeats + with.Handshakes[i]; with.NonProgress[i] > allowed {
			return fmt.Errorf("shard %d: %.0f non-progress responses exceed %.0f (%.0f timeout repeats for a %.1fs gap without a block + %.0f handshakes) out of %.0f: additional root-to-shard traffic",
				i, with.NonProgress[i], allowed, timeoutRepeats, with.MaxGap[i], with.Handshakes[i], with.Responses[i])
		}
	}
	return nil
}

func pc(responses, blocks, nonprogress, handshakes, submitted []float64, rounds float64) phaseCount {
	gaps := make([]float64, len(responses)) // no gap approaching T2: no timeout repeat is legitimate
	for i := range gaps {
		gaps[i] = 1
	}
	zero := make([]float64, len(responses))
	return phaseCount{Responses: responses, Blocks: blocks, NonProgress: nonprogress, Handshakes: handshakes, Submitted: submitted, MaxGap: gaps,
		Stalls: zero, StallBlocks: append([]float64(nil), zero...), Rounds: rounds}
}

func vec(v ...float64) []float64 { return v }

func TestMessageRateCheck(t *testing.T) {
	base := pc(vec(8, 9, 8, 7), vec(7, 8, 7, 7), vec(0, 0, 0, 0), vec(0, 0, 0, 0), vec(40, 40, 40, 40), 20)
	with := pc(vec(8, 8, 9, 7), vec(8, 7, 8, 7), vec(0, 0, 0, 0), vec(0, 0, 0, 0), vec(40, 40, 40, 40), 20)
	require.NoError(t, checkMessageRate(base, with))

	// every response sent twice
	doubled := with
	doubled.Responses = vec(16, 16, 18, 14)
	doubled.NonProgress = vec(8, 8, 9, 7)
	require.Error(t, checkMessageRate(base, doubled))

	// extra traffic carrying ADVANCING root rounds (no new shard block) at the minimum live workload:
	// baseline 2 blocks / 2 responses, consolidation phase 2 blocks / 16 responses
	minBase := pc(vec(2, 2), vec(2, 2), vec(0, 0), vec(0, 0), vec(50, 50), 21)
	extra := pc(vec(16, 2), vec(2, 2), vec(14, 0), vec(0, 0), vec(50, 50), 21)
	require.NoError(t, checkWriteProgress(minBase))
	require.NoError(t, checkWriteProgress(extra))
	require.ErrorContains(t, checkMessageRate(minBase, extra), "shard 0")
	// even if the counter missed it (all responses counted as progress) the progress bound catches it
	hidden := extra
	hidden.NonProgress = vec(0, 0)
	require.ErrorContains(t, checkMessageRate(minBase, hidden), "progress responses")

	// a single duplicate per block at the minimum workload (2 blocks, 2 extra responses)
	dup := pc(vec(4, 2), vec(2, 2), vec(2, 0), vec(0, 0), vec(50, 50), 21)
	require.ErrorContains(t, checkMessageRate(minBase, dup), "shard 0")
	// one finalized block with one extra response
	one := pc(vec(2, 1), vec(1, 1), vec(1, 0), vec(0, 0), vec(50, 50), 21)
	require.ErrorContains(t, checkMessageRate(pc(vec(1, 1), vec(1, 1), vec(0, 0), vec(0, 0), vec(50, 50), 21), one), "shard 0")

	// timeout repeats are legitimate exactly when the shard went T2 without a finalized block
	repeatBase := minBase // the baseline had the same gap: the stall is not a consolidation effect
	repeatBase.MaxGap = vec(6.2, 1)
	repeat := pc(vec(4, 3), vec(3, 3), vec(1, 0), vec(0, 0), vec(40, 40), 28)
	repeat.MaxGap = vec(6.2, 1)
	require.NoError(t, checkMessageRate(repeatBase, repeat))
	repeat.NonProgress = vec(2, 0) // a second one needs a second T2 gap
	repeat.Responses = vec(5, 3)
	require.ErrorContains(t, checkMessageRate(repeatBase, repeat), "shard 0")

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

// stalled returns a phase in which shard 0 stalled for gap seconds and then caught up with a burst.
func stalled(responses, blocks float64, gap, stalls, burst float64) phaseCount {
	p := pc(vec(responses, 5), vec(blocks, 5), vec(0, 0), vec(0, 0), vec(50, 50), 21)
	p.MaxGap = vec(gap, 1)
	p.Stalls = vec(stalls, 0)
	p.StallBlocks = vec(burst, 0)
	return p
}

func TestMessageRateCheck_StalledPhases(t *testing.T) {
	base := pc(vec(5, 5), vec(5, 5), vec(0, 0), vec(0, 0), vec(50, 50), 21)
	baseStalled := stalled(4, 9, 6.0, 1, 5) // the baseline has the same stall: not a consolidation effect

	// the review's case: 0 responses for 9 blocks in a stalled phase must fail (it used to pass)
	require.ErrorContains(t, checkMessageRate(baseStalled, stalled(0, 9, 6.0, 1, 9)), "missing deliveries")
	// a coalesced catch-up burst: 9 blocks, one response for the 9-block burst
	require.NoError(t, checkMessageRate(baseStalled, stalled(1, 9, 6.0, 1, 9)))
	// the blocks outside the burst keep the normal bound: 4 outside blocks + 1 burst response = at least 4
	require.NoError(t, checkMessageRate(baseStalled, stalled(4, 9, 6.0, 1, 5)))
	require.ErrorContains(t, checkMessageRate(baseStalled, stalled(3, 9, 6.0, 1, 5)), "missing deliveries")
	// two stalls need two responses
	baseLong := stalled(4, 9, 11.0, 2, 9)
	require.NoError(t, checkMessageRate(baseLong, stalled(2, 9, 11.0, 2, 9)))
	require.ErrorContains(t, checkMessageRate(baseLong, stalled(1, 9, 11.0, 2, 9)), "missing deliveries")
	// a stall still lasting at the end of the phase (no catch-up) expects no extra response, only the normal bound
	baseStill := stalled(3, 3, 12.0, 0, 0)
	require.NoError(t, checkMessageRate(baseStill, stalled(3, 3, 12.0, 0, 0)))
	require.ErrorContains(t, checkMessageRate(baseStill, stalled(0, 3, 12.0, 0, 0)), "missing deliveries")

	// a stall that exists only in the consolidation phase is compared with the baseline and rejected
	err := checkMessageRate(base, stalled(1, 9, 6.0, 1, 9))
	require.ErrorContains(t, err, "stalled")
	require.ErrorContains(t, err, "baseline worst gap 1.0s")
	// a short gap below T2 is not a stall; a baseline stall of the same length excuses it
	require.NoError(t, checkMessageRate(base, stalled(5, 5, 4.0, 0, 0)))
	require.NoError(t, checkMessageRate(baseStalled, stalled(1, 9, 6.5, 1, 9)))
	// but a much longer stall than the baseline's is flagged
	require.ErrorContains(t, checkMessageRate(baseStalled, stalled(1, 9, 20.0, 1, 9)), "stalled")
}

// The recorded live 4-shard run (final gate) is accepted: no stalls, responses = blocks ±1.
func TestMessageRateCheck_RecordedRunIsAccepted(t *testing.T) {
	gap := vec(2, 2, 2, 2)
	z := vec(0, 0, 0, 0)
	p1 := phaseCount{Responses: vec(8, 8, 8, 7), Blocks: vec(8, 7, 8, 7), NonProgress: z, Handshakes: z, Submitted: vec(61, 61, 60, 61), MaxGap: gap, Stalls: z, StallBlocks: z, Rounds: 23}
	p2 := phaseCount{Responses: vec(7, 7, 7, 8), Blocks: vec(7, 8, 7, 8), NonProgress: z, Handshakes: z, Submitted: vec(60, 60, 61, 60), MaxGap: gap, Stalls: z, StallBlocks: z, Rounds: 22}
	require.NoError(t, checkWriteProgress(p1))
	require.NoError(t, checkWriteProgress(p2))
	require.NoError(t, checkMessageRate(p1, p2))
}
