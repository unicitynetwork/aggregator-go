package signing

import (
	"encoding/json"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ReceiptSigner handles signing of commitment receipts
type ReceiptSigner struct {
	privateKey []byte
	publicKey  []byte
	algorithm  string
}

// NewReceiptSigner creates a new receipt signer from a private key
func NewReceiptSigner(privateKey []byte) (*ReceiptSigner, error) {
	if len(privateKey) != 32 {
		return nil, fmt.Errorf("private key must be 32 bytes, got %d", len(privateKey))
	}

	// Derive public key from private key
	privKey, _ := btcec.PrivKeyFromBytes(privateKey)
	publicKey := privKey.PubKey().SerializeCompressed()

	return &ReceiptSigner{
		privateKey: privateKey,
		publicKey:  publicKey,
		algorithm:  AlgorithmSecp256k1,
	}, nil
}

// SignReceipt creates a signed receipt for a commitment
func (rs *ReceiptSigner) SignReceipt(requestID api.RequestID, transactionHash api.TransactionHash, stateHash api.StateHash) (*api.Receipt, error) {
	// Create the receipt request data to be signed
	request := api.ReceiptRequest{
		Service:         "aggregator",
		Method:          "submit_commitment",
		RequestID:       requestID,
		TransactionHash: transactionHash,
		StateHash:       stateHash,
	}

	// Serialize the request to JSON for signing
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize receipt request: %w", err)
	}

	// Sign the serialized request
	signingService := NewSigningService()
	signature, err := signingService.Sign(requestBytes, rs.privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign receipt: %w", err)
	}

	return &api.Receipt{
		Algorithm: rs.algorithm,
		PublicKey: rs.publicKey,
		Signature: signature,
		Request:   request,
	}, nil
}

// PublicKey returns the signer's public key
func (rs *ReceiptSigner) PublicKey() []byte {
	return rs.publicKey
}

// Algorithm returns the signing algorithm
func (rs *ReceiptSigner) Algorithm() string {
	return rs.algorithm
}

