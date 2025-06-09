package models

// Authenticator represents the authentication data for a commitment
type Authenticator struct {
	StateHash HexBytes `json:"stateHash" bson:"stateHash"`
	PublicKey HexBytes `json:"publicKey" bson:"publicKey"`
	Signature HexBytes `json:"signature" bson:"signature"`
	SignAlg   string   `json:"signAlg" bson:"signAlg"`
	HashAlg   string   `json:"hashAlg" bson:"hashAlg"`
}

// Commitment represents a state transition request
type Commitment struct {
	RequestID       RequestID       `json:"requestId" bson:"requestId"`
	TransactionHash TransactionHash `json:"transactionHash" bson:"transactionHash"`
	Authenticator   Authenticator   `json:"authenticator" bson:"authenticator"`
	CreatedAt       *Timestamp      `json:"createdAt" bson:"createdAt"`
	ProcessedAt     *Timestamp      `json:"processedAt,omitempty" bson:"processedAt,omitempty"`
}

// NewCommitment creates a new commitment
func NewCommitment(requestID RequestID, transactionHash TransactionHash, authenticator Authenticator) *Commitment {
	return &Commitment{
		RequestID:       requestID,
		TransactionHash: transactionHash,
		Authenticator:   authenticator,
		CreatedAt:       Now(),
	}
}

// AggregatorRecord represents a finalized commitment with proof data
type AggregatorRecord struct {
	RequestID       RequestID       `json:"requestId" bson:"requestId"`
	TransactionHash TransactionHash `json:"transactionHash" bson:"transactionHash"`
	Authenticator   Authenticator   `json:"authenticator" bson:"authenticator"`
	BlockNumber     *BigInt         `json:"blockNumber" bson:"blockNumber"`
	LeafIndex       *BigInt         `json:"leafIndex" bson:"leafIndex"`
	CreatedAt       *Timestamp      `json:"createdAt" bson:"createdAt"`
	FinalizedAt     *Timestamp      `json:"finalizedAt" bson:"finalizedAt"`
}

// NewAggregatorRecord creates a new aggregator record from a commitment
func NewAggregatorRecord(commitment *Commitment, blockNumber, leafIndex *BigInt) *AggregatorRecord {
	return &AggregatorRecord{
		RequestID:       commitment.RequestID,
		TransactionHash: commitment.TransactionHash,
		Authenticator:   commitment.Authenticator,
		BlockNumber:     blockNumber,
		LeafIndex:       leafIndex,
		CreatedAt:       commitment.CreatedAt,
		FinalizedAt:     Now(),
	}
}

// Receipt represents a signed receipt for a commitment submission
type Receipt struct {
	Algorithm string        `json:"algorithm"`
	PublicKey HexBytes      `json:"publicKey"`
	Signature HexBytes      `json:"signature"`
	Request   ReceiptRequest `json:"request"`
}

// ReceiptRequest represents the request data in a receipt
type ReceiptRequest struct {
	Service         string          `json:"service"`
	Method          string          `json:"method"`
	RequestID       RequestID       `json:"requestId"`
	TransactionHash TransactionHash `json:"transactionHash"`
	StateHash       HexBytes        `json:"stateHash"`
}

// NewReceipt creates a new receipt for a commitment
func NewReceipt(commitment *Commitment, algorithm string, publicKey, signature HexBytes) *Receipt {
	return &Receipt{
		Algorithm: algorithm,
		PublicKey: publicKey,
		Signature: signature,
		Request: ReceiptRequest{
			Service:         "aggregator",
			Method:          "submit_commitment",
			RequestID:       commitment.RequestID,
			TransactionHash: commitment.TransactionHash,
			StateHash:       commitment.Authenticator.StateHash,
		},
	}
}