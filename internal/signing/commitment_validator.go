package signing

import (
	"encoding/hex"
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/models"
)

// ValidationStatus represents the result of commitment validation
type ValidationStatus int

const (
	ValidationStatusSuccess ValidationStatus = iota
	ValidationStatusRequestIDMismatch
	ValidationStatusSignatureVerificationFailed
	ValidationStatusInvalidSignatureFormat
	ValidationStatusInvalidPublicKeyFormat
	ValidationStatusInvalidStateHashFormat
	ValidationStatusInvalidTransactionHashFormat
	ValidationStatusUnsupportedAlgorithm
)

func (s ValidationStatus) String() string {
	switch s {
	case ValidationStatusSuccess:
		return "SUCCESS"
	case ValidationStatusRequestIDMismatch:
		return "REQUEST_ID_MISMATCH"
	case ValidationStatusSignatureVerificationFailed:
		return "SIGNATURE_VERIFICATION_FAILED"
	case ValidationStatusInvalidSignatureFormat:
		return "INVALID_SIGNATURE_FORMAT"
	case ValidationStatusInvalidPublicKeyFormat:
		return "INVALID_PUBLIC_KEY_FORMAT"
	case ValidationStatusInvalidStateHashFormat:
		return "INVALID_STATE_HASH_FORMAT"
	case ValidationStatusInvalidTransactionHashFormat:
		return "INVALID_TRANSACTION_HASH_FORMAT"
	case ValidationStatusUnsupportedAlgorithm:
		return "UNSUPPORTED_ALGORITHM"
	default:
		return "UNKNOWN"
	}
}

// ValidationResult contains the result of commitment validation
type ValidationResult struct {
	Status ValidationStatus
	Error  error
}

// CommitmentValidator validates commitment signatures and request IDs
type CommitmentValidator struct {
	signingService     *SigningService
	requestIDGenerator *RequestIDGenerator
}

// NewCommitmentValidator creates a new commitment validator
func NewCommitmentValidator() *CommitmentValidator {
	return &CommitmentValidator{
		signingService:     NewSigningService(),
		requestIDGenerator: NewRequestIDGenerator(),
	}
}

// ValidateCommitment performs complete validation of a commitment
// This mirrors the TypeScript validateCommitment function in AggregatorService
func (v *CommitmentValidator) ValidateCommitment(commitment *models.Commitment) ValidationResult {
	// 1. Validate algorithm support
	if commitment.Authenticator.Algorithm != AlgorithmSecp256k1 {
		return ValidationResult{
			Status: ValidationStatusUnsupportedAlgorithm,
			Error:  fmt.Errorf("unsupported algorithm: %s", commitment.Authenticator.Algorithm),
		}
	}

	// 2. Parse and validate public key
	publicKeyBytes, err := hex.DecodeString(string(commitment.Authenticator.PublicKey))
	if err != nil {
		return ValidationResult{
			Status: ValidationStatusInvalidPublicKeyFormat,
			Error:  fmt.Errorf("failed to decode public key: %w", err),
		}
	}

	if err := v.signingService.ValidatePublicKey(publicKeyBytes); err != nil {
		return ValidationResult{
			Status: ValidationStatusInvalidPublicKeyFormat,
			Error:  fmt.Errorf("invalid public key: %w", err),
		}
	}

	// 3. Parse and validate state hash (should be DataHash imprint: algorithm + data)
	stateHashImprint, err := hex.DecodeString(string(commitment.Authenticator.StateHash))
	if err != nil {
		return ValidationResult{
			Status: ValidationStatusInvalidStateHashFormat,
			Error:  fmt.Errorf("failed to decode state hash: %w", err),
		}
	}
	
	// Validate state hash imprint format (minimum 3 bytes: 2 for algorithm + 1 for data)
	if len(stateHashImprint) < 3 {
		return ValidationResult{
			Status: ValidationStatusInvalidStateHashFormat,
			Error:  fmt.Errorf("state hash imprint must have at least 3 bytes (2 algorithm + 1 data), got %d", len(stateHashImprint)),
		}
	}
	
	// Extract algorithm from state hash imprint (first 2 bytes, big-endian)
	stateHashAlgorithm := (int(stateHashImprint[0]) << 8) | int(stateHashImprint[1])
	if stateHashAlgorithm != 0 { // SHA256 = 0
		return ValidationResult{
			Status: ValidationStatusInvalidStateHashFormat,
			Error:  fmt.Errorf("state hash algorithm must be SHA256 (0), got %d", stateHashAlgorithm),
		}
	}
	
	// Extract actual state hash data (skip first 2 bytes)
	stateHashBytes := stateHashImprint[2:]

	// 4. Validate Request ID matches expected value
	// RequestID should be SHA256(publicKey || stateHash)
	isValidRequestID, err := v.requestIDGenerator.ValidateRequestID(
		commitment.RequestID,
		publicKeyBytes,
		stateHashBytes,
	)
	if err != nil {
		return ValidationResult{
			Status: ValidationStatusRequestIDMismatch,
			Error:  fmt.Errorf("failed to validate request ID: %w", err),
		}
	}

	if !isValidRequestID {
		return ValidationResult{
			Status: ValidationStatusRequestIDMismatch,
			Error:  fmt.Errorf("request ID does not match expected value"),
		}
	}

	// 5. Parse signature
	signatureBytes, err := hex.DecodeString(string(commitment.Authenticator.Signature))
	if err != nil {
		return ValidationResult{
			Status: ValidationStatusInvalidSignatureFormat,
			Error:  fmt.Errorf("failed to decode signature: %w", err),
		}
	}

	// 6. Parse transaction hash (should be DataHash imprint: algorithm + data)
	transactionHashImprint, err := hex.DecodeString(string(commitment.TransactionHash))
	if err != nil {
		return ValidationResult{
			Status: ValidationStatusInvalidTransactionHashFormat,
			Error:  fmt.Errorf("failed to decode transaction hash: %w", err),
		}
	}
	
	// Validate transaction hash imprint format (minimum 3 bytes: 2 for algorithm + 1 for data)
	if len(transactionHashImprint) < 3 {
		return ValidationResult{
			Status: ValidationStatusInvalidTransactionHashFormat,
			Error:  fmt.Errorf("transaction hash imprint must have at least 3 bytes (2 algorithm + 1 data), got %d", len(transactionHashImprint)),
		}
	}
	
	// Extract algorithm from transaction hash imprint (first 2 bytes, big-endian)
	transactionHashAlgorithm := (int(transactionHashImprint[0]) << 8) | int(transactionHashImprint[1])
	if transactionHashAlgorithm != 0 { // SHA256 = 0
		return ValidationResult{
			Status: ValidationStatusInvalidTransactionHashFormat,
			Error:  fmt.Errorf("transaction hash algorithm must be SHA256 (0), got %d", transactionHashAlgorithm),
		}
	}
	
	// Extract actual transaction hash data (skip first 2 bytes) - this is what gets signed
	transactionHashBytes := transactionHashImprint[2:]

	// 7. Verify signature
	// The signature should be over the transaction hash bytes
	isValidSignature, err := v.signingService.VerifyWithPublicKey(
		transactionHashBytes,
		signatureBytes,
		publicKeyBytes,
	)
	if err != nil {
		return ValidationResult{
			Status: ValidationStatusSignatureVerificationFailed,
			Error:  fmt.Errorf("signature verification error: %w", err),
		}
	}

	if !isValidSignature {
		return ValidationResult{
			Status: ValidationStatusSignatureVerificationFailed,
			Error:  fmt.Errorf("signature verification failed"),
		}
	}

	// All validations passed
	return ValidationResult{
		Status: ValidationStatusSuccess,
		Error:  nil,
	}
}