package api

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/aggregator-go/internal/api"
	"github.com/unicitynetwork/aggregator-go/internal/models"
)

func TestInclusionProof_SerializeAndVerify(t *testing.T) {
	t.Run("should encode and decode JSON correctly", func(t *testing.T) {
		// Test data from InclusionProofTest.ts
		publicKeyHex := "0279BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798"
		signatureHex := "A0B37F8FBA683CC68F6574CD43B39F0343A50008BF6CCEA9D13231D9E7E2E1E411EDC8D307254296264AEBFC3DC76CD8B668373A072FD64665B50000E9FCCE5201"
		stateHashHex := "00000000000000000000000000000000000000000000000000000000000000000000"
		transactionHashHex := "00000000000000000000000000000000000000000000000000000000000000000000"
		
		publicKey, err := hex.DecodeString(publicKeyHex)
		require.NoError(t, err)
		
		signature, err := hex.DecodeString(signatureHex)
		require.NoError(t, err)
		
		stateHashBytes := make([]byte, 34)
		transactionHashBytes := make([]byte, 34)
		
		authenticator := &models.Authenticator{
			Algorithm: "secp256k1",
			PublicKey: models.NewHexBytes(publicKey),
			Signature: models.NewHexBytes(signature),
			StateHash: models.NewHexBytes(stateHashBytes),
		}
		
		// Create MerkleTreePath from TypeScript test data
		merkleTreePath := &models.MerkleTreePath{
			Root: "0000CEEA69FFE5399BAE643C9DC6E456B33F17488A5E1F6A497CC6692677C1DBC940",
			Steps: []models.MerkleTreeStep{
				{
					Branch:  []string{"0000635F7A05683E8BD119490DE02AE3CE67A44C73CED2A5F2DA33743269218AB8DF"},
					Path:    "7588594300971394838541568248286222591294169947711183361137673310094707450920243806",
					Sibling: nil,
				},
			},
		}
		
		transactionHashHexBytes := models.NewHexBytes(transactionHashBytes)
		inclusionProof := &models.APIInclusionProof{
			MerkleTreePath:  merkleTreePath,
			Authenticator:   authenticator,
			TransactionHash: &transactionHashHexBytes,
		}

		// Test JSON serialization
		expectedJSON := map[string]interface{}{
			"authenticator": map[string]interface{}{
				"algorithm": "secp256k1",
				"publicKey": strings.ToLower(publicKeyHex),
				"signature": strings.ToLower(signatureHex),
				"stateHash": stateHashHex,
			},
			"merkleTreePath": map[string]interface{}{
				"root": "0000CEEA69FFE5399BAE643C9DC6E456B33F17488A5E1F6A497CC6692677C1DBC940",
				"steps": []interface{}{
					map[string]interface{}{
						"branch":  []interface{}{"0000635F7A05683E8BD119490DE02AE3CE67A44C73CED2A5F2DA33743269218AB8DF"},
						"path":    "7588594300971394838541568248286222591294169947711183361137673310094707450920243806",
						"sibling": nil,
					},
				},
			},
			"transactionHash": transactionHashHex,
		}

		jsonBytes, err := json.Marshal(inclusionProof)
		require.NoError(t, err)
		
		var actualJSON map[string]interface{}
		err = json.Unmarshal(jsonBytes, &actualJSON)
		require.NoError(t, err)
		
		assert.Equal(t, expectedJSON, actualJSON)

		// Test deserialization
		var deserializedProof models.APIInclusionProof
		err = json.Unmarshal(jsonBytes, &deserializedProof)
		require.NoError(t, err)
		
		assert.Equal(t, inclusionProof.MerkleTreePath.Root, deserializedProof.MerkleTreePath.Root)
		assert.Len(t, deserializedProof.MerkleTreePath.Steps, 1)
		assert.Equal(t, inclusionProof.Authenticator.Algorithm, deserializedProof.Authenticator.Algorithm)
	})

	t.Run("should handle null authenticator and transaction hash", func(t *testing.T) {
		// Create MerkleTreePath
		merkleTreePath := &models.MerkleTreePath{
			Root: "0000CEEA69FFE5399BAE643C9DC6E456B33F17488A5E1F6A497CC6692677C1DBC940",
			Steps: []models.MerkleTreeStep{
				{
					Branch:  []string{"0000635F7A05683E8BD119490DE02AE3CE67A44C73CED2A5F2DA33743269218AB8DF"},
					Path:    "7588594300971394838541568248286222591294169947711183361137673310094707450920243806",
					Sibling: nil,
				},
			},
		}
		
		inclusionProof := &models.APIInclusionProof{
			MerkleTreePath:  merkleTreePath,
			Authenticator:   nil,
			TransactionHash: nil,
		}

		// Test JSON serialization with nulls
		jsonBytes, err := json.Marshal(inclusionProof)
		require.NoError(t, err)
		
		var actualJSON map[string]interface{}
		err = json.Unmarshal(jsonBytes, &actualJSON)
		require.NoError(t, err)
		
		assert.Nil(t, actualJSON["authenticator"])
		assert.Nil(t, actualJSON["transactionHash"])
		assert.NotNil(t, actualJSON["merkleTreePath"])

		// Test deserialization
		var deserializedProof models.APIInclusionProof
		err = json.Unmarshal(jsonBytes, &deserializedProof)
		require.NoError(t, err)
		
		assert.Nil(t, deserializedProof.Authenticator)
		assert.Nil(t, deserializedProof.TransactionHash)
		assert.NotNil(t, deserializedProof.MerkleTreePath)
	})

	t.Run("should validate authenticator and transaction hash consistency", func(t *testing.T) {
		// Test validation logic: both must be set or both must be null
		merkleTreePath := &models.MerkleTreePath{
			Root:  "0000CEEA69FFE5399BAE643C9DC6E456B33F17488A5E1F6A497CC6692677C1DBC940",
			Steps: []models.MerkleTreeStep{},
		}
		
		authenticator := &models.Authenticator{
			Algorithm: "secp256k1",
			PublicKey: models.NewHexBytes(make([]byte, 33)),
			Signature: models.NewHexBytes(make([]byte, 64)),
			StateHash: models.NewHexBytes(make([]byte, 34)),
		}
		
		// Valid: both set
		transactionHashBytes := models.NewHexBytes(make([]byte, 34))
		validProof := &models.APIInclusionProof{
			MerkleTreePath:  merkleTreePath,
			Authenticator:   authenticator,
			TransactionHash: &transactionHashBytes,
		}
		
		err := api.ValidateInclusionProof(validProof)
		assert.NoError(t, err)
		
		// Valid: both null
		validNullProof := &models.APIInclusionProof{
			MerkleTreePath:  merkleTreePath,
			Authenticator:   nil,
			TransactionHash: nil,
		}
		
		err = api.ValidateInclusionProof(validNullProof)
		assert.NoError(t, err)
		
		// Invalid: authenticator set, transaction hash null
		invalidProof1 := &models.APIInclusionProof{
			MerkleTreePath:  merkleTreePath,
			Authenticator:   authenticator,
			TransactionHash: nil,
		}
		
		err = api.ValidateInclusionProof(invalidProof1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Authenticator and transaction hash must be both set or both null")
		
		// Invalid: authenticator null, transaction hash set
		invalidTransactionHash := models.NewHexBytes(make([]byte, 34))
		invalidProof2 := &models.APIInclusionProof{
			MerkleTreePath:  merkleTreePath,
			Authenticator:   nil,
			TransactionHash: &invalidTransactionHash,
		}
		
		err = api.ValidateInclusionProof(invalidProof2)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Authenticator and transaction hash must be both set or both null")
	})
}