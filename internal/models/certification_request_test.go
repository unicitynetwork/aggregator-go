package models

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// BenchmarkCreateLeafValue benchmarks the createLeafValue function
func BenchmarkCreateLeafValue(b *testing.B) {
	// Setup test data
	publicKey := []byte{0x02, 0x79, 0xbe, 0x66}
	signature := []byte{0xa0, 0xb3, 0x7f, 0x8f}
	sourceStateHash, _ := api.NewImprintHexString("0000deadbeef")
	transactionHash, _ := api.NewImprintHexString("0000feedcafe")

	certData := CertificationData{
		OwnerPredicate:  api.NewHexBytes(publicKey),
		SourceStateHash: sourceStateHash,
		TransactionHash: transactionHash,
		Witness:         api.NewHexBytes(signature),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := certData.ToAPI().Hash()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestCreateLeafValue(t *testing.T) {
	jsonStr := `{
		"ownerPredicate": "03d791430260906e5f87525f8816012defb3325b91595d277b7f5e15d4ee4e1138", 
		"witness": "fc208c58e9f92d8ce06ec51b333a9c04f771728b7aa227ec52f897f2e59ab44274d72e6412f23ee9a994bf0149042d286d3bdf87598b696613c30e155233384c01", 
		"sourceStateHash": "0000fc0bc026fd9119b8c143888d952bf256561c9f378e709d4b373ab4bdc46f22c6", 
		"transactionHash": "0000924b7b556def13f556bc813119921cefc62617e71e4471c637c4ae2cc43ee40b"
	}`
	// the expected leaf value calculated by java sdk
	expectedLeafValue := "0000b3a9d04a54cd84467e56feff411f0846f7f776bdc6c201047c9219b3d1d339d8"

	certData := CertificationData{}
	require.NoError(t, json.Unmarshal([]byte(jsonStr), &certData))

	certRequest := CertificationRequest{CertificationData: certData}
	value, err := certRequest.CertificationData.ToAPI().Hash()
	require.NoError(t, err)
	require.Equal(t, expectedLeafValue, fmt.Sprintf("%x", value))
}
