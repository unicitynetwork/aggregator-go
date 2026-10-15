package integration

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// phaseCount is what one measurement phase observed, per shard.
type phaseCount struct {
	Responses []float64 // certification responses received from the root
	Blocks    []float64 // finalized blocks (new certified states)
	Submitted []float64 // accepted submissions
	Rounds    float64   // root rounds elapsed
}

func (p phaseCount) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "rounds=%.0f", p.Rounds)
	for i := range p.Responses {
		fmt.Fprintf(&sb, " | shard %d: submitted=%.0f blocks=%.0f responses=%.0f", i, p.Submitted[i], p.Blocks[i], p.Responses[i])
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

// checkMessageRate compares the root->shard responses with the finalization
// workload per shard. Every finalized block is delivered by one response; the
// baseline phase (same writers, no consolidation traffic) shows how many extra
// responses (timeout repeats) occur without it. Consolidation traffic must not
// raise a shard's responses beyond its blocks plus that baseline extra.
func checkMessageRate(base, with phaseCount) error {
	for i := range with.Responses {
		extraBase := base.Responses[i] - base.Blocks[i]
		if extraBase < 0 {
			extraBase = 0
		}
		if with.Responses[i] < with.Blocks[i]-1 {
			return fmt.Errorf("shard %d: %.0f responses for %.0f finalized blocks (missing deliveries)", i, with.Responses[i], with.Blocks[i])
		}
		if allowed := with.Blocks[i] + extraBase + 2; with.Responses[i] > allowed {
			return fmt.Errorf("shard %d: %.0f responses exceed %.0f (blocks %.0f + baseline extra %.0f + 2): additional root-to-shard traffic",
				i, with.Responses[i], allowed, with.Blocks[i], extraBase)
		}
	}
	return nil
}

func TestMessageRateCheck(t *testing.T) {
	base := phaseCount{Responses: []float64{8, 9, 8, 7}, Blocks: []float64{7, 8, 7, 7}, Submitted: []float64{40, 40, 40, 40}, Rounds: 20}
	with := phaseCount{Responses: []float64{8, 8, 9, 7}, Blocks: []float64{8, 7, 8, 7}, Submitted: []float64{40, 40, 40, 40}, Rounds: 20}
	require.NoError(t, checkMessageRate(base, with))

	// the review's counter-example: doubled traffic must fail (aggregate 0.20 -> 0.40 passed the old check)
	doubled := with
	doubled.Responses = []float64{16, 16, 18, 14}
	require.Error(t, checkMessageRate(base, doubled))

	// a single duplicate response per block on one shard is detected
	dup := with
	dup.Responses = []float64{8, 8, 9 + 8, 7}
	require.ErrorContains(t, checkMessageRate(base, dup), "shard 2")

	// missing deliveries are detected too
	missing := with
	missing.Responses = []float64{2, 8, 9, 7}
	require.ErrorContains(t, checkMessageRate(base, missing), "missing deliveries")
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
