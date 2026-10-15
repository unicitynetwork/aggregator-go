package consolidation

import (
	"crypto"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
)

// Without an independently pinned topology the scheme and configuration hashes
// come from the seal-committed view.
func TestConsolidate_UnpinnedTopologyIsAuthenticatedByTheSeal(t *testing.T) {
	f := newFixture(t, ts())
	f.policy.Topology = types.PartitionViewTopology{}
	s := f.session()
	got, err := f.consolidate(s)
	require.NoError(t, err)
	require.Equal(t, Counters{QuorumChecks: 1, BodyChecks: 2, LeafChecks: 2, EvidenceQuorumChecks: 2}, s.Counters())
	require.Len(t, got.Shards, 2)
	for i, so := range got.Shards {
		l := f.view.Leaves[i]
		require.NoError(t, so.UC.Verify(f.tb, crypto.SHA256, testPartition, so.ShardID, l.ShardConfHash))
	}
}

func TestConsolidate_UnpinnedStillRejectsForgedViews(t *testing.T) {
	for name, mutate := range map[string]func(f *fixture){
		"configuration hash of the view changed": func(f *fixture) {
			v := f.view.Clone()
			v.Leaves[1].ShardConfHash = h32("forged conf")
			f.in.PartitionView, _ = v.MarshalCBOR()
		},
		"view of another scheme": func(f *fixture) {
			v := f.view.Clone()
			v.Leaves = v.Leaves[:1]
			v.Leaves[0].ShardID = types.ShardID{}
			f.in.PartitionView, _ = v.MarshalCBOR()
		},
		"leaf record changed": func(f *fixture) {
			v := f.view.Clone()
			v.Leaves[1].InputRecord.Timestamp++
			f.in.PartitionView, _ = v.MarshalCBOR()
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, ts())
			f.policy.Topology = types.PartitionViewTopology{}
			mutate(f)
			_, err := f.consolidate(f.session())
			requireOutcome(t, err, OutcomeInvalidInput)
		})
	}
}

func TestConsolidate_PinsAreStricterNotLooser(t *testing.T) {
	f := newFixture(t, ts())
	// pins which disagree with the (genuinely authenticated) view reject it
	for _, id := range f.scheme {
		f.policy.Topology.ShardConfHashes[id.Key()] = h32("another configuration")
	}
	_, err := f.consolidate(f.session())
	require.Error(t, err)
}

func TestConsolidate_EvidenceShardMustMatchConsolidatedShard(t *testing.T) {
	f := newFixture(t, ts())
	f.policy.Topology = types.PartitionViewTopology{}
	s := f.session()
	// the original of the peer's request was certified in the master's shard
	ev := f.defaultEvidence()
	ev[1] = f.evidenceAt(0, f.round-2)
	ev[1].Request = f.in.Peers[0].Requests[0]
	_, err := s.AuthenticateEvidence(ev)
	// authenticating this inconsistent original fails as its path is of another leaf, or the
	// shard binding refuses it at consolidation: never a silent success
	if err == nil {
		bound, _ := s.AuthenticateEvidence(ev)
		f.in.Bound = bound
		_, err = s.Consolidate(f.in)
	}
	require.Error(t, err)
}
