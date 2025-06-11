package signing

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// CreateDataHashImprint creates a DataHash imprint in the TypeScript format:
// 2 bytes algorithm (big-endian) + actual hash bytes
// For SHA256: algorithm = 0, so prefix is [0x00, 0x00]
func CreateDataHashImprint(data []byte) api.ImprintHexString {
	// Hash the data with SHA256
	hash := sha256.Sum256(data)

	// Create imprint: algorithm (0x00, 0x00 for SHA256) + hash
	imprint := make([]byte, 2+len(hash))
	imprint[0] = 0x00 // SHA256 algorithm high byte
	imprint[1] = 0x00 // SHA256 algorithm low byte
	copy(imprint[2:], hash[:])

	return api.ImprintHexString(hex.EncodeToString(imprint))
}
