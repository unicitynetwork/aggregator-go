package signing

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ValidationStatus represents the result of certification request validation
type ValidationStatus int

const (
	ValidationStatusSuccess ValidationStatus = iota
	ValidationStatusStateIDMismatch
	ValidationStatusSignatureVerificationFailed
	ValidationStatusInvalidSignatureFormat
	ValidationStatusInvalidPublicKeyFormat
	ValidationStatusInvalidSourceStateHashFormat
	ValidationStatusInvalidTransactionHashFormat
	ValidationStatusShardMismatch
	ValidationStatusInvalidOwnerPredicate
)

func (s ValidationStatus) String() string {
	switch s {
	case ValidationStatusSuccess:
		return "SUCCESS"
	case ValidationStatusStateIDMismatch:
		return "STATE_ID_MISMATCH"
	case ValidationStatusSignatureVerificationFailed:
		return "SIGNATURE_VERIFICATION_FAILED"
	case ValidationStatusInvalidSignatureFormat:
		return "INVALID_SIGNATURE_FORMAT"
	case ValidationStatusInvalidPublicKeyFormat:
		return "INVALID_PUBLIC_KEY_FORMAT"
	case ValidationStatusInvalidSourceStateHashFormat:
		return "INVALID_SOURCE_STATE_HASH_FORMAT"
	case ValidationStatusInvalidTransactionHashFormat:
		return "INVALID_TRANSACTION_HASH_FORMAT"
	case ValidationStatusShardMismatch:
		return "INVALID_SHARD"
	default:
		return "UNKNOWN"
	}
}

// ValidationResult contains the result of certification request validation
type ValidationResult struct {
	Status ValidationStatus
	Error  error
}

// CertificationRequestValidator validates certification request signatures and state IDs
type CertificationRequestValidator struct {
	signingService *SigningService
	shardConfig    config.ShardingConfig
}

// NewCertificationRequestValidator creates a new certification request validator
func NewCertificationRequestValidator(shardConfig config.ShardingConfig) *CertificationRequestValidator {
	return &CertificationRequestValidator{
		signingService: NewSigningService(),
		shardConfig:    shardConfig,
	}
}

// Validate performs complete validation of a commitment
// This mirrors the TypeScript validateCommitment function in AggregatorService
func (v *CertificationRequestValidator) Validate(commitment *models.CertificationRequest) ValidationResult {
	// Parse and validate owner predicate
	publicKeyBytes, err := v.verifyPayToPublicKeyPredicate(commitment.CertificationData.OwnerPredicate)
	if err != nil {
		return ValidationResult{
			Status: ValidationStatusInvalidOwnerPredicate,
			Error:  fmt.Errorf("invalid owner predicate: %w", err),
		}
	}
	if err := v.signingService.ValidatePublicKey(publicKeyBytes); err != nil {
		return ValidationResult{
			Status: ValidationStatusInvalidPublicKeyFormat,
			Error:  fmt.Errorf("invalid public key: %w", err),
		}
	}

	// Validate source state hash length
	sourceStateHash := commitment.CertificationData.SourceStateHash
	if len(sourceStateHash) != 32 {
		return ValidationResult{
			Status: ValidationStatusInvalidSourceStateHashFormat,
			Error: fmt.Errorf("source state hash must be exactly 32 bytes "+
				"got %d", len(sourceStateHash)),
		}
	}
	// StateID should be SHA256(CBOR[sourceStateHash, ownerPredicate])
	isValidStateID, err := api.ValidateStateID(
		commitment.StateID,
		sourceStateHash,
		commitment.CertificationData.OwnerPredicate,
	)
	if err != nil {
		return ValidationResult{
			Status: ValidationStatusStateIDMismatch,
			Error:  fmt.Errorf("failed to validate state ID: %w", err),
		}
	}

	if !isValidStateID {
		return ValidationResult{
			Status: ValidationStatusStateIDMismatch,
			Error:  fmt.Errorf("state ID does not match expected value"),
		}
	}

	// Verify correct shard
	if err := v.ValidateShardID(commitment.StateID); err != nil {
		return ValidationResult{
			Status: ValidationStatusShardMismatch,
			Error:  fmt.Errorf("invalid shard: %w", err),
		}
	}

	// Verify signature
	// Validate signature format (must be 65 bytes for secp256k1)
	signatureBytes := commitment.CertificationData.Witness
	if len(signatureBytes) != 65 {
		return ValidationResult{
			Status: ValidationStatusInvalidSignatureFormat,
			Error:  fmt.Errorf("signature must be 65 bytes, got %d", len(signatureBytes)),
		}
	}

	// Validate transaction hash length
	transactionHash := commitment.CertificationData.TransactionHash
	if len(transactionHash) != 32 {
		return ValidationResult{
			Status: ValidationStatusInvalidTransactionHashFormat,
			Error: fmt.Errorf("transaction hash imprint must have at least 3 bytes (2 algorithm + 1 data), "+
				"got %d", len(transactionHash)),
		}
	}

	sigDataHash := api.SigDataHash(sourceStateHash, transactionHash)
	isValidSignature, err := v.signingService.VerifyDataHashWithPublicKey(
		sigDataHash,
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

func (v *CertificationRequestValidator) verifyPayToPublicKeyPredicate(pred api.Predicate) ([]byte, error) {
	if pred.Engine != 1 {
		return nil, fmt.Errorf("invalid engine type: got %d, expected 1", pred.Engine)
	}
	if len(pred.Code) != 1 || pred.Code[0] != 1 {
		return nil, fmt.Errorf("invalid predicate code, got %x, expected %x", pred.Code, 1)
	}
	return pred.Params, nil
}

// ValidateShardID verifies if the state id belongs to the configured shard
func (v *CertificationRequestValidator) ValidateShardID(stateID api.StateID) error {
	if !v.shardConfig.Mode.IsChild() {
		return nil
	}
	ok, err := verifyShardID(stateID.String(), v.shardConfig.Child.ShardID)
	if err != nil {
		return fmt.Errorf("error verifying shard id: %w", err)
	}
	if !ok {
		return errors.New("state ID shard part does not match the current shard identifier")
	}
	return nil
}

// verifyShardID Checks if commitmentID's least significant bits match the shard bitmask.
func verifyShardID(commitmentID string, shardBitmask int) (bool, error) {
	// convert to big.Ints
	bytes, err := hex.DecodeString(commitmentID)
	if err != nil {
		return false, fmt.Errorf("failed to decode certification state ID: %w", err)
	}
	commitmentIdBigInt := new(big.Int).SetBytes(bytes)
	shardBitmaskBigInt := new(big.Int).SetInt64(int64(shardBitmask))

	// find position of MSB e.g.
	// 0b111 -> BitLen=3 -> 3-1=2
	msbPos := shardBitmaskBigInt.BitLen() - 1

	// build a mask covering bits below MSB e.g.
	// 1<<2=0b100; 0b100-1=0b11; compareMask=0b11
	compareMask := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), uint(msbPos)), big.NewInt(1))

	// remove MSB from shardBitmask to get expected value e.g.
	// 0b111 & 0b11 = 0b11
	expected := new(big.Int).And(shardBitmaskBigInt, compareMask)

	// extract low bits from certification request e.g.
	// commitment=0b11111111 & 0b11 = 0b11
	commitmentLowBits := new(big.Int).And(commitmentIdBigInt, compareMask)

	// return true if the certification request low bits match bitmask bits e.g.
	// 0b11 == 0b11
	return commitmentLowBits.Cmp(expected) == 0, nil
}
