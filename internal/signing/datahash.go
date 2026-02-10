package signing

import (
	"crypto/sha256"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

func CreateDataHash(data []byte) api.ImprintV2 {
	hash := sha256.Sum256(data)
	return hash[:]
}
