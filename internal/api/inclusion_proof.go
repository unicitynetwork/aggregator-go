package api

import (
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/models"
)

// ValidateInclusionProof validates the consistency of an APIInclusionProof
// Both authenticator and transaction hash must be set, or both must be null
func ValidateInclusionProof(proof *models.APIInclusionProof) error {
	if proof == nil {
		return fmt.Errorf("inclusion proof cannot be nil")
	}

	// Check consistency: both must be set or both must be null
	authenticatorSet := proof.Authenticator != nil
	transactionHashSet := proof.TransactionHash != nil

	if authenticatorSet != transactionHashSet {
		return fmt.Errorf("Authenticator and transaction hash must be both set or both null")
	}

	return nil
}