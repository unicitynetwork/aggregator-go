package consolidation

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"
)

// The session snapshots its policy: later changes by the caller change nothing.
func TestSession_PolicyIsSnapshotted(t *testing.T) {
	f := newFixture(t, ts())
	pol := f.policy
	s, err := NewSession(pol, f.resolver())
	require.NoError(t, err)
	idBefore := append([]byte(nil), s.policyID...)
	// mutate everything the caller still holds
	for k := range pol.Topology.ShardConfHashes {
		pol.Topology.ShardConfHashes[k] = []byte{1}
		delete(pol.Topology.ShardConfHashes, k)
	}
	pol.Topology.Scheme[0] = types.ShardID{}
	pol.ID = []byte("changed")
	require.Equal(t, idBefore, s.policyID)
	_, err = s.AuthenticateEvidence(f.defaultEvidence())
	require.NoError(t, err, "the session keeps working on its own snapshot")
	require.Equal(t, 1, s.Counters().TopologyValidations)
}

// The slow reference model for the shard index: a linear scan of the authenticated leaves.
func TestAuthenticatedMaster_IndexAgreesWithScan(t *testing.T) {
	f := newFixture(t, ts())
	s := f.session()
	b, err := s.AuthenticateEvidence(f.defaultEvidence())
	require.NoError(t, err)
	m, err := s.AuthenticateMaster(b, f.in.Master, f.in.PartitionView)
	require.NoError(t, err)
	for i, l := range m.view.Leaves {
		idx, ok := m.leaf(l.ShardID)
		require.True(t, ok)
		require.Equal(t, i, idx)
		root, err := m.PeerRoot(l.ShardID)
		require.NoError(t, err)
		require.Equal(t, []byte(l.InputRecord.Hash), root)
	}
	_, ok := m.leaf(types.ShardID{})
	require.False(t, ok)
	require.Equal(t, 2*len(m.view.Leaves)+1, s.Counters().ShardLeafProbes, "exactly one probe per lookup")
}
