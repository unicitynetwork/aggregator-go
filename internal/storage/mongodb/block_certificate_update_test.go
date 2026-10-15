package mongodb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// The partition view is always written or cleared in the same update as the UC.
func TestCertificateUpdatePairsViewWithUC(t *testing.T) {
	block := &models.Block{UnicityCertificate: api.NewHexBytes([]byte{0xaa})}

	u := certificateUpdate(block, bson.M{"finalized": true})
	require.Equal(t, bson.M{"finalized": true, "unicityCertificate": "aa"}, u["$set"])
	require.Equal(t, bson.M{"partitionView": ""}, u["$unset"], "stale view must be cleared when the UC is replaced")

	block.PartitionView = api.NewHexBytes([]byte{0x01, 0x02})
	u = certificateUpdate(block, bson.M{"finalized": true})
	require.Equal(t, bson.M{"finalized": true, "unicityCertificate": "aa", "partitionView": "0102"}, u["$set"])
	require.NotContains(t, u, "$unset")
}
