package proofmaterial_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
	"github.com/unicitynetwork/bft-go-base/types"
)

type topo struct{ t types.PartitionViewTopology }

func (p topo) Topology(types.PartitionID) (types.PartitionViewTopology, bool) { return p.t, true }

type noTopo struct{}

func (noTopo) Topology(types.PartitionID) (types.PartitionViewTopology, bool) {
	return types.PartitionViewTopology{}, false
}

// accept stands in for seal authentication (the fixture seals are unsigned); the real
// verifier is exercised through types.AuthenticateView and the BFT client tests.
type accept struct{}

func (accept) VerifySeal(*types.UnicitySeal) error { return nil }

type reject struct{}

func (reject) VerifySeal(*types.UnicitySeal) error { return errors.New("seal not authenticated") }

func Test_ValidateView(t *testing.T) {
	for _, bits := range []int{0, 2, 3} {
		f := prooftest.New(t, bits, 5, prooftest.Hash("root"), 80)
		// authenticated by the seal alone, and additionally pinned
		for name, pins := range map[string]proofmaterial.TopologyProvider{"no pins": nil, "unresolved pins": noTopo{}, "pinned": f.Provider(t)} {
			res, got, err := proofmaterial.ValidateView(f.Raw, f.UC, prooftest.Partition, accept{}, pins)
			require.NoError(t, err, "%d %s", bits, name)
			require.Equal(t, proofmaterial.ResultOK, res)
			require.Equal(t, f.Raw, got)
			orig := f.Raw[0]
			got[0] ^= 0xff
			require.Equal(t, orig, f.Raw[0], "result must be a private copy")
		}
	}
}

// The topology is no prerequisite; an unusable or disagreeing pin only rejects.
func Test_ValidateView_PinsAreOptionalAndStricter(t *testing.T) {
	f := prooftest.New(t, 2, 5, prooftest.Hash("root"), 80)

	incomplete := map[string][]byte{}
	for k, v := range f.Topo.ShardConfHashes {
		incomplete[k] = v
	}
	delete(incomplete, f.Shard.Key())
	short := map[string][]byte{}
	for k, v := range f.Topo.ShardConfHashes {
		short[k] = v
	}
	short[f.Shard.Key()] = []byte{1, 2, 3}

	// without pins the view hashes into the seal and is retained
	for _, p := range []proofmaterial.TopologyProvider{nil, noTopo{}} {
		res, _, err := proofmaterial.ValidateView(f.Raw, f.UC, prooftest.Partition, accept{}, p)
		require.NoError(t, err)
		require.Equal(t, proofmaterial.ResultOK, res)
	}
	// unusable pins are refused, never silently ignored
	for name, p := range map[string]proofmaterial.TopologyProvider{
		"scheme only, no hashes":        topo{types.PartitionViewTopology{Scheme: f.Topo.Scheme}},
		"hash map missing a shard":      topo{types.PartitionViewTopology{Scheme: f.Topo.Scheme, ShardConfHashes: incomplete}},
		"hash of the wrong size":        topo{types.PartitionViewTopology{Scheme: f.Topo.Scheme, ShardConfHashes: short}},
		"incomplete scheme with hashes": topo{types.PartitionViewTopology{Scheme: f.Topo.Scheme[:3], ShardConfHashes: f.Topo.ShardConfHashes}},
	} {
		res, view, err := proofmaterial.ValidateView(f.Raw, f.UC, prooftest.Partition, accept{}, p)
		require.Error(t, err, name)
		require.Equal(t, proofmaterial.ResultNoTopology, res, name)
		require.Nil(t, view, name)
	}

	_, err := proofmaterial.NewStaticTopology(prooftest.Partition, f.Topo.Scheme, incomplete)
	require.Error(t, err)
	_, err = proofmaterial.NewStaticTopology(prooftest.Partition, f.Topo.Scheme[:3], f.Topo.ShardConfHashes)
	require.Error(t, err)
	_, err = proofmaterial.SingleShardTopology(prooftest.Partition, []byte{1})
	require.Error(t, err)
	st, err := proofmaterial.SingleShardTopology(prooftest.Partition, prooftest.Hash("c"))
	require.NoError(t, err)
	_, ok := st.Topology(prooftest.Partition + 1)
	require.False(t, ok)
}

func Test_ValidateView_Failures(t *testing.T) {
	f := prooftest.New(t, 2, 5, prooftest.Hash("root"), 80)
	other := prooftest.New(t, 2, 6, prooftest.Hash("other root"), 81)
	good := f.Provider(t)

	wrongConf := map[string][]byte{}
	for k, v := range f.Topo.ShardConfHashes {
		wrongConf[k] = v
	}
	wrongConf[f.Shard.Key()] = prooftest.Hash("bad")

	cases := map[string]struct {
		raw    []byte
		topos  proofmaterial.TopologyProvider
		result string
	}{
		"missing":                   {nil, good, proofmaterial.ResultMissing},
		"too large":                 {bytes.Repeat([]byte{1}, types.MaxPartitionViewBytes+1), good, proofmaterial.ResultTooLarge},
		"malformed":                 {[]byte{0x83, 0x01}, good, proofmaterial.ResultMalformed},
		"trailing bytes":            {append(bytes.Clone(f.Raw), 0), good, proofmaterial.ResultMalformed},
		"view of other UC":          {other.Raw, good, proofmaterial.ResultInvalid},
		"view of other UC, no pins": {other.Raw, nil, proofmaterial.ResultInvalid},
		"wrong scheme":              {f.Raw, topo{oneBitTopology()}, proofmaterial.ResultInvalid},
		"wrong config hash":         {f.Raw, topo{types.PartitionViewTopology{Scheme: f.Topo.Scheme, ShardConfHashes: wrongConf}}, proofmaterial.ResultInvalid},
	}
	for name, c := range cases {
		res, view, err := proofmaterial.ValidateView(c.raw, f.UC, prooftest.Partition, accept{}, c.topos)
		require.Equal(t, c.result, res, name)
		require.Nil(t, view, name)
		if c.result != proofmaterial.ResultMissing {
			require.Error(t, err, name)
		}
	}
	t.Run("seal hash mismatch", func(t *testing.T) {
		uc := *f.UC
		seal := *f.UC.UnicitySeal
		seal.Hash = prooftest.Hash("tampered")
		uc.UnicitySeal = &seal
		for _, pins := range []proofmaterial.TopologyProvider{nil, good} {
			res, _, err := proofmaterial.ValidateView(f.Raw, &uc, prooftest.Partition, accept{}, pins)
			require.Error(t, err)
			require.Equal(t, proofmaterial.ResultInvalid, res)
		}
	})
	t.Run("seal not authenticated", func(t *testing.T) {
		for name, v := range map[string]types.SealVerifier{"rejecting": reject{}, "nil": nil} {
			res, view, err := proofmaterial.ValidateView(f.Raw, f.UC, prooftest.Partition, v, nil)
			require.Error(t, err, name)
			require.Equal(t, proofmaterial.ResultInvalid, res, name)
			require.Nil(t, view, name)
		}
	})
	t.Run("nil path elements", func(t *testing.T) {
		uc := *f.UC
		utc := *f.UC.UnicityTreeCertificate
		utc.HashSteps = []*types.PathItem{nil}
		uc.UnicityTreeCertificate = &utc
		require.NotPanics(t, func() {
			res, _, err := proofmaterial.ValidateView(f.Raw, &uc, prooftest.Partition, accept{}, nil)
			require.Error(t, err)
			require.Equal(t, proofmaterial.ResultInvalid, res)
		})
	})
	t.Run("wrong partition", func(t *testing.T) {
		res, _, err := proofmaterial.ValidateView(f.Raw, f.UC, prooftest.Partition+1, accept{}, nil)
		require.Error(t, err)
		require.Equal(t, proofmaterial.ResultInvalid, res)
	})
}

func Test_Staging_BoundedAndKeyed(t *testing.T) {
	s := proofmaterial.NewStaging(2)
	d := func(b byte) proofmaterial.Digest { return proofmaterial.Digest{b} }
	s.Put(d(1), []byte{1})
	s.Put(d(2), []byte{2})
	s.Put(d(3), []byte{3})
	require.Equal(t, 2, s.Len())
	_, ok := s.Get(d(1))
	require.False(t, ok, "oldest evicted")
	v, ok := s.Get(d(3))
	require.True(t, ok)
	require.Equal(t, []byte{3}, v)

	in := []byte{9}
	s.Put(d(4), in)
	in[0] = 0
	v, _ = s.Get(d(4))
	require.Equal(t, []byte{9}, v, "stored copy")

	s.Put(d(4), []byte{7})
	v, _ = s.Get(d(4))
	require.Equal(t, []byte{9}, v, "existing entry is kept")
	s.Put(d(5), nil)
	_, ok = s.Get(d(5))
	require.False(t, ok)
}

func Test_DigestOfUC_DistinguishesSeals(t *testing.T) {
	f := prooftest.New(t, 0, 5, prooftest.Hash("root"), 80)
	d1, _, err := proofmaterial.DigestOfUC(f.UC)
	require.NoError(t, err)
	uc2 := *f.UC
	seal := *f.UC.UnicitySeal
	seal.RootChainRoundNumber++
	uc2.UnicitySeal = &seal
	d2, _, err := proofmaterial.DigestOfUC(&uc2)
	require.NoError(t, err)
	require.NotEqual(t, d1, d2)
	_, _, err = proofmaterial.DigestOfUC(nil)
	require.Error(t, err)
}

func oneBitTopology() types.PartitionViewTopology {
	scheme := prooftest.Scheme(1)
	hashes := map[string][]byte{}
	for _, id := range scheme {
		hashes[id.Key()] = prooftest.Hash("a" + id.String())
	}
	return types.PartitionViewTopology{Scheme: scheme, ShardConfHashes: hashes}
}
