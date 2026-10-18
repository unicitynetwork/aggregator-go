package mongodb

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/unicitynetwork/aggregator-go/internal/proofmaterial/prooftest"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/bft-go-base/types"
)

// Measures the BSON size of a block document of an 8 shard partition (the testnet2 size) with the
// stored partition view and without it. Before the retention change every block kept the view,
// afterwards only the latest finalized one does. Sizes are bytes, independent of the hardware.
func TestBlockDocumentSizeWithAndWithoutView(t *testing.T) {
	for _, bits := range []int{1, 2, 3} {
		shards := 1 << bits
		root := make([]byte, 32)
		f := prooftest.New(t, bits, 1, root, 80)
		uc, err := types.Cbor.Marshal(f.UC)
		require.NoError(t, err)

		block := viewBlock(1, "", "")
		block.Index = api.NewBigInt(big.NewInt(1))
		block.UnicityCertificate = api.NewHexBytes(uc)
		bb, err := block.ToBSON()
		require.NoError(t, err)
		without, err := bson.Marshal(bb)
		require.NoError(t, err)
		block.PartitionView = api.NewHexBytes(f.Raw)
		bb, err = block.ToBSON()
		require.NoError(t, err)
		with, err := bson.Marshal(bb)
		require.NoError(t, err)

		require.Greater(t, len(with), len(without))
		t.Logf("shards=%d: block document %d B with the view (every block before), %d B without (history after); view %d B raw, added per block %d B",
			shards, len(with), len(without), len(f.Raw), len(with)-len(without))
	}
}
