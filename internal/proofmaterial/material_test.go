package proofmaterial_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial"
	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
	"github.com/unicitynetwork/bft-go-base/types"
)

type topo struct{ t types.PartitionViewTopology }

func (p topo) Topology(types.PartitionID) (types.PartitionViewTopology, bool) { return p.t, true }

func Test_ValidateView(t *testing.T) {
	for _, bits := range []int{0, 2, 3} {
		f := prooftest.New(t, bits, 5, prooftest.Hash("root"), 80)
		res, got, err := proofmaterial.ValidateView(f.Raw, f.UC, prooftest.Partition, nil)
		require.NoError(t, err, bits)
		require.Equal(t, proofmaterial.ResultDerivedTopology, res)
		require.Equal(t, f.Raw, got)
		orig := f.Raw[0]
		got[0] ^= 0xff
		require.Equal(t, orig, f.Raw[0], "result must be a private copy")

		res, _, err = proofmaterial.ValidateView(f.Raw, f.UC, prooftest.Partition, topo{f.Topo})
		require.NoError(t, err)
		require.Equal(t, proofmaterial.ResultOK, res)
	}
}

func Test_ValidateView_Failures(t *testing.T) {
	f := prooftest.New(t, 2, 5, prooftest.Hash("root"), 80)
	other := prooftest.New(t, 2, 6, prooftest.Hash("other root"), 81)

	cases := map[string]struct {
		raw    []byte
		topos  proofmaterial.TopologyProvider
		result string
	}{
		"missing":           {nil, nil, proofmaterial.ResultMissing},
		"too large":         {bytes.Repeat([]byte{1}, types.MaxPartitionViewBytes+1), nil, proofmaterial.ResultTooLarge},
		"malformed":         {[]byte{0x83, 0x01}, nil, proofmaterial.ResultMalformed},
		"trailing bytes":    {append(bytes.Clone(f.Raw), 0), nil, proofmaterial.ResultMalformed},
		"view of other UC":  {other.Raw, nil, proofmaterial.ResultInvalid},
		"wrong topology":    {f.Raw, topo{types.PartitionViewTopology{Scheme: prooftest.Scheme(1)}}, proofmaterial.ResultInvalid},
		"wrong config hash": {f.Raw, topo{types.PartitionViewTopology{Scheme: f.Topo.Scheme, ShardConfHashes: map[string][]byte{f.Shard.Key(): prooftest.Hash("bad")}}}, proofmaterial.ResultInvalid},
	}
	for name, c := range cases {
		res, view, err := proofmaterial.ValidateView(c.raw, f.UC, prooftest.Partition, c.topos)
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
		res, _, err := proofmaterial.ValidateView(f.Raw, &uc, prooftest.Partition, nil)
		require.Error(t, err)
		require.Equal(t, proofmaterial.ResultInvalid, res)
	})
	t.Run("wrong partition", func(t *testing.T) {
		res, _, err := proofmaterial.ValidateView(f.Raw, f.UC, prooftest.Partition+1, nil)
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
