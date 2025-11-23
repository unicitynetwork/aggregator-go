package models

import (
	"testing"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// BenchmarkCreateLeafValue benchmarks the createLeafValue function
func BenchmarkCreateLeafValue(b *testing.B) {
	// Setup test data
	publicKey := []byte{0x02, 0x79, 0xbe, 0x66}
	signature := []byte{0xa0, 0xb3, 0x7f, 0x8f}
	sourceStateHash, _ := api.NewImprintHexString("0000deadbeef")
	transactionHash, _ := api.NewImprintHexString("0000feedcafe")
	stateID, _ := api.CreateStateID(sourceStateHash, publicKey)

	certData := CertificationData{
		PublicKey:       api.NewHexBytes(publicKey),
		Signature:       api.NewHexBytes(signature),
		SourceStateHash: sourceStateHash,
		TransactionHash: transactionHash,
	}
	certificationRequest := NewCertificationRequest(stateID, certData)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := certificationRequest.CreateLeafValue()
		if err != nil {
			b.Fatal(err)
		}
	}
}
