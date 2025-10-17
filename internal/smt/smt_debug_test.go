package smt

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func TestAddLeaves_DebugInvalidPath(t *testing.T) {

	addCommitment := func(tree *SparseMerkleTree, commJson map[string]interface{}) string {
		commJsonBytes, err := json.Marshal(commJson)
		require.NoError(t, err, "Failed to marshal commitment JSON")
		commitment := &models.Commitment{}
		require.NoError(t, json.Unmarshal(commJsonBytes, commitment))

		path, err := commitment.RequestID.GetPath()
		require.NoError(t, err)

		// Create leaf value (hash of commitment data)
		leafValue, err := commitment.CreateLeafValue()
		require.NoError(t, err)

		leaf := &Leaf{
			Path:  path,
			Value: leafValue,
		}

		err = tree.AddLeaves([]*Leaf{leaf})
		require.NoError(t, err, "Expected error due to invalid path")

		// now validate the path of request
		merkleTreePath, err := tree.GetPath(path)
		require.NoError(t, err)
		require.NotNil(t, merkleTreePath, "Expected non-nil Merkle tree path for valid request ID")

		res, err := merkleTreePath.Verify(path)
		require.NoError(t, err, "Expected no error verifying Merkle tree path")

		require.True(t, res.PathIncluded)
		require.True(t, res.PathValid)

		rh := tree.GetRootHashHex()
		require.Equal(t, rh, merkleTreePath.Root)

		return rh
	}

	_smt := NewSparseMerkleTree(api.SHA256, 16+256)
	{ // mint commitment
		commJson := map[string]interface{}{
			"requestId":       "00007d535ade796772c5088b095e79a18e282437ee8d8238f5aa9d9c61694948ba9e",
			"transactionHash": "0000784d629eb9530b02c35d66eea760354bfdf55eddffc14823774137add2858ccd",
			"authenticator": map[string]interface{}{
				"algorithm": "secp256k1",
				"publicKey": "02b766b621e32ddf4350f4061fda2b3da2cbd1df3cc52d7b790fe4a6ce94fd56b4",
				"signature": "8e81f0628d183ddb997032aac169c331e1e8ce68041b028c5d85e1149bc444f1273b9800e5f32542abc8a8dec9d8fb84b1e9f5feb316fe9f822016f97b68f2d001",
				"stateHash": "0000e5a2521a3b309282d71359a02ee75c4ac378f12fffda070fa83cc6e1588c522d",
			},
		}
		fmt.Printf("Root hash after mint: %x\n", addCommitment(_smt, commJson))
	}

	{ // transfer commitment
		commJson := map[string]interface{}{
			"requestId":       "00006478ca42f6949cfbd4b9e4a41b9a384ea78261c1776808da70cf21e98c345700",
			"transactionHash": "0000cbbea2c8ce3ca54649210fa5f4febd4f2418d011a89800081f28f6c419f45ee9",
			"authenticator": map[string]interface{}{
				"algorithm": "secp256k1",
				"publicKey": "0231a5ab4f93da686b862762c86c661a2e642009f73412dc1d57df539e17b50300",
				"signature": "ec68788d7b58afba297ecaf1859a3204a98dc109a222814569ca84d50c63c9f5228b1431153be8513bf6acc636179efeb214eef533802d8d322f5ae53894b51400",
				"stateHash": "0000ec68788d7b58afba297ecaf1859a3204a98dc109a222814569ca84d50c63c9f5228b1431153be8513bf6acc636179efeb214eef533802d8d322f5ae53894b51400",
			},
		}
		fmt.Printf("Root hash after transfer: %x\n", addCommitment(_smt, commJson))
	}

	{
		req, err := api.NewImprintHexString("00006df936060e07cad29086335623b2a05afef0b05f77dcc27f6e5065abce6f061d")
		require.NoError(t, err, "Failed to create request ID")
		path, err := req.GetPath()
		require.NoError(t, err)
		merkleTreePath, err := _smt.GetPath(path)
		require.NoError(t, err)
		require.NotNil(t, merkleTreePath, "Expected non-nil Merkle tree path for valid request ID")

		res, err := merkleTreePath.Verify(path)
		require.NoError(t, err, "Expected no error verifying Merkle tree path")

		require.False(t, res.PathIncluded)
		require.True(t, res.PathValid)
	}
}
