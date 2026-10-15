package integration

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// phaseCount is what one measurement phase observed, per shard.
type phaseCount struct {
	Responses  []float64 // certification responses received from the root
	Blocks     []float64 // finalized blocks (new certified states)
	Repeats    []float64 // responses repeating an already received root round
	Handshakes []float64 // handshakes sent to the root (each may be answered by a re-delivery)
	Submitted  []float64 // accepted submissions
	Rounds     float64   // root rounds elapsed
}

func (p phaseCount) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "rounds=%.0f", p.Rounds)
	for i := range p.Responses {
		fmt.Fprintf(&sb, " | shard %d: submitted=%.0f blocks=%.0f responses=%.0f repeats=%.0f handshakes=%.0f", i, p.Submitted[i], p.Blocks[i], p.Responses[i], p.Repeats[i], p.Handshakes[i])
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

// checkMessageRate checks the root->shard traffic against the certification workload without a
// workload-dependent tolerance: every response must be a new certification. The aggregator counts
// responses whose root round does not advance beyond the newest one already received (re-deliveries);
// consolidation traffic must not increase that count over the equal-work baseline phase (normally 0)
// except for re-deliveries answering handshakes, so a single duplicate per block is detected at any
// workload, including one finalized block. Every
// finalized block must also have been delivered.
func checkMessageRate(base, with phaseCount) error {
	for i := range with.Responses {
		if with.Responses[i] < with.Blocks[i]-1 {
			return fmt.Errorf("shard %d: %.0f responses for %.0f finalized blocks (missing deliveries)", i, with.Responses[i], with.Blocks[i])
		}
		// a handshake (reconnect/inactivity) may legitimately be answered by a re-delivery of the last response
		if allowed := base.Repeats[i] + with.Handshakes[i]; with.Repeats[i] > allowed {
			return fmt.Errorf("shard %d: %.0f repeated responses exceed %.0f (baseline %.0f + %.0f handshakes) out of %.0f: additional root-to-shard traffic",
				i, with.Repeats[i], allowed, base.Repeats[i], with.Handshakes[i], with.Responses[i])
		}
	}
	return nil
}

func TestMessageRateCheck(t *testing.T) {
	base := phaseCount{Responses: []float64{8, 9, 8, 7}, Blocks: []float64{7, 8, 7, 7}, Repeats: []float64{0, 0, 0, 0}, Handshakes: []float64{0, 0, 0, 0}, Submitted: []float64{40, 40, 40, 40}, Rounds: 20}
	with := phaseCount{Responses: []float64{8, 8, 9, 7}, Blocks: []float64{8, 7, 8, 7}, Repeats: []float64{0, 0, 0, 0}, Handshakes: []float64{0, 0, 0, 0}, Submitted: []float64{40, 40, 40, 40}, Rounds: 20}
	require.NoError(t, checkMessageRate(base, with))

	// every response sent twice
	doubled := with
	doubled.Responses = []float64{16, 16, 18, 14}
	doubled.Repeats = []float64{8, 8, 9, 7}
	require.Error(t, checkMessageRate(base, doubled))

	// the review's minimum-workload case: 2 blocks / 2 responses baseline, 2 blocks / 4 responses with
	// consolidation, 50 submissions per shard: the previous tolerance of +2 accepted it
	minBase := phaseCount{Responses: []float64{2, 2}, Blocks: []float64{2, 2}, Repeats: []float64{0, 0}, Handshakes: []float64{0, 0, 0, 0}, Submitted: []float64{50, 50}}
	minWith := phaseCount{Responses: []float64{4, 2}, Blocks: []float64{2, 2}, Repeats: []float64{2, 0}, Handshakes: []float64{0, 0, 0, 0}, Submitted: []float64{50, 50}}
	require.NoError(t, checkWriteProgress(minBase))
	require.NoError(t, checkWriteProgress(minWith))
	require.ErrorContains(t, checkMessageRate(minBase, minWith), "shard 0")
	// one finalized block with one duplicate delivery is detected too
	oneWith := phaseCount{Responses: []float64{2, 1}, Blocks: []float64{1, 1}, Repeats: []float64{1, 0}, Handshakes: []float64{0, 0, 0, 0}, Submitted: []float64{50, 50}}
	oneBase := phaseCount{Responses: []float64{1, 1}, Blocks: []float64{1, 1}, Repeats: []float64{0, 0}, Handshakes: []float64{0, 0, 0, 0}, Submitted: []float64{50, 50}}
	require.ErrorContains(t, checkMessageRate(oneBase, oneWith), "shard 0")

	// a baseline with its own re-deliveries is not blamed on consolidation
	noisy := base
	noisy.Repeats = []float64{1, 0, 0, 0}
	withNoisy := with
	withNoisy.Repeats = []float64{1, 0, 0, 0}
	require.NoError(t, checkMessageRate(noisy, withNoisy))

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

func TestMessageRateCheck_HandshakeRedeliveriesAreNotTraffic(t *testing.T) {
	base := phaseCount{Responses: []float64{3, 3}, Blocks: []float64{3, 3}, Repeats: []float64{0, 0}, Handshakes: []float64{0, 0}, Submitted: []float64{50, 50}}
	// one inactivity handshake answered by one re-delivery on shard 0
	with := phaseCount{Responses: []float64{4, 3}, Blocks: []float64{3, 3}, Repeats: []float64{1, 0}, Handshakes: []float64{1, 0}, Submitted: []float64{50, 50}}
	require.NoError(t, checkMessageRate(base, with))
	// but two re-deliveries for one handshake are traffic
	with.Repeats = []float64{2, 0}
	with.Responses = []float64{5, 3}
	require.ErrorContains(t, checkMessageRate(base, with), "shard 0")
}
