package trustbase

import (
	"context"
	"errors"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"
)

type TrustBaseValidator struct {
	storage TrustBaseStore
}

func NewTrustBaseValidator(storage TrustBaseStore) *TrustBaseValidator {
	return &TrustBaseValidator{storage: storage}
}

func (t *TrustBaseValidator) Verify(ctx context.Context, trustBase types.RootTrustBase) error {
	if trustBase == nil {
		return errors.New("trust base is nil")
	}
	trustBaseV1, ok := trustBase.(*types.RootTrustBaseV1)
	if !ok {
		return fmt.Errorf("failed to cast trust base to version 1 for epoch %d", trustBase.GetEpoch())
	}

	// verify trust base extends previous trust base
	var previousTrustBaseV1 *types.RootTrustBaseV1
	epoch := trustBaseV1.GetEpoch()
	if epoch > 1 {
		var err error
		previousTrustBaseV1, err = t.storage.GetByEpoch(ctx, epoch-1)
		if err != nil {
			return fmt.Errorf("previous trust base not found for epoch %d: %w", epoch-1, err)
		}
	}
	if err := trustBaseV1.Verify(previousTrustBaseV1); err != nil {
		return fmt.Errorf("failed to verify trust base: %w", err)
	}
	return nil
}
