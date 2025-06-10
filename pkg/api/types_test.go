package api

import (
	"encoding/json"
	"testing"
)

func TestRequestIDMarshalJSON(t *testing.T) {
	requestID := RequestID("0000cfe84a1828e2edd0a7d9533b23e519f746069a938d549a150e07e14dc0f9cf00")

	data, err := json.Marshal(requestID)
	if err != nil {
		t.Fatalf("Failed to marshal RequestID: %v", err)
	}

	var unmarshaledID RequestID
	if err := json.Unmarshal(data, &unmarshaledID); err != nil {
		t.Fatalf("Failed to unmarshal RequestID: %v", err)
	}

	if unmarshaledID != requestID {
		t.Errorf("RequestID mismatch: got %s, want %s", unmarshaledID, requestID)
	}
}

func TestHexBytesMarshalJSON(t *testing.T) {
	hexBytes := HexBytes{0x01, 0x02, 0x03, 0x04}

	data, err := json.Marshal(hexBytes)
	if err != nil {
		t.Fatalf("Failed to marshal HexBytes: %v", err)
	}

	var unmarshaledHex HexBytes
	if err := json.Unmarshal(data, &unmarshaledHex); err != nil {
		t.Fatalf("Failed to unmarshal HexBytes: %v", err)
	}

	if len(unmarshaledHex) != len(hexBytes) {
		t.Errorf("HexBytes length mismatch: got %d, want %d", len(unmarshaledHex), len(hexBytes))
	}

	for i, b := range hexBytes {
		if unmarshaledHex[i] != b {
			t.Errorf("HexBytes byte mismatch at index %d: got %x, want %x", i, unmarshaledHex[i], b)
		}
	}
}

func TestSubmitCommitmentRequestJSON(t *testing.T) {
	req := &SubmitCommitmentRequest{
		RequestID:       "0000cfe84a1828e2edd0a7d9533b23e519f746069a938d549a150e07e14dc0f9cf00",
		TransactionHash: "00008a51b5b84171e6c7c345bf3610cc18fa1b61bad33908e1522520c001b0e7fd1d",
		Authenticator: Authenticator{
			Algorithm: "secp256k1",
			PublicKey: HexBytes{0x03, 0x20, 0x44, 0xf2},
			Signature: HexBytes{0x41, 0x67, 0x51, 0xe8},
			StateHash: ImprintHexString("0000cd60"),
		},
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Failed to marshal SubmitCommitmentRequest: %v", err)
	}

	var unmarshaledReq SubmitCommitmentRequest
	if err := json.Unmarshal(data, &unmarshaledReq); err != nil {
		t.Fatalf("Failed to unmarshal SubmitCommitmentRequest: %v", err)
	}

	if unmarshaledReq.RequestID != req.RequestID {
		t.Errorf("RequestID mismatch: got %s, want %s", unmarshaledReq.RequestID, req.RequestID)
	}

	if unmarshaledReq.TransactionHash != req.TransactionHash {
		t.Errorf("TransactionHash mismatch: got %s, want %s", unmarshaledReq.TransactionHash, req.TransactionHash)
	}

	if unmarshaledReq.Authenticator.Algorithm != req.Authenticator.Algorithm {
		t.Errorf("Algorithm mismatch: got %s, want %s", unmarshaledReq.Authenticator.Algorithm, req.Authenticator.Algorithm)
	}
}
