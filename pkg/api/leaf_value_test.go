package api

import (
	"encoding/hex"
	"testing"
)

// Shared across the Rust, Java and TypeScript implementations: the leaf value
// is SHA-256 over the deterministic CBOR array [transactionHash, referenceTime].
const (
	sharedTransactionHash = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	sharedReferenceTime   = uint64(1755000000)
	sharedLeafValue       = "0235bd52cfa10c9785dfa01942bc396f201fe715dbc3896ee117a97e895e1e36"
)

func TestLeafValueMatchesTheSharedTestVector(t *testing.T) {
	txHash, err := hex.DecodeString(sharedTransactionHash)
	if err != nil {
		t.Fatalf("failed to decode transaction hash: %v", err)
	}

	got := hex.EncodeToString(LeafValue(txHash, sharedReferenceTime))
	if got != sharedLeafValue {
		t.Fatalf("leaf value mismatch: got %s, want %s", got, sharedLeafValue)
	}
}

func TestLeafValueChangesWithTheReferenceTime(t *testing.T) {
	txHash, _ := hex.DecodeString(sharedTransactionHash)

	if hex.EncodeToString(LeafValue(txHash, sharedReferenceTime+1)) == sharedLeafValue {
		t.Fatal("leaf value did not change with the reference time")
	}
}
