package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

type TrustBaseValidator struct {
	storage interfaces.TrustBaseStorage
}

func NewTrustBaseValidator(storage interfaces.TrustBaseStorage) *TrustBaseValidator {
	return &TrustBaseValidator{storage: storage}
}

func (t *TrustBaseValidator) Verify(ctx context.Context, trustBase *types.RootTrustBaseV1) error {
	if trustBase == nil {
		return errors.New("trust base is nil")
	}

	// verify trust base extends previous trust base
	var previousTrustBaseV1 *types.RootTrustBaseV1
	epoch := trustBase.GetEpoch()
	if epoch > 1 {
		previousTrustBase, err := t.storage.GetByEpoch(ctx, epoch-1)
		if err != nil {
			return fmt.Errorf("previous trust base not found for epoch %d: %w", epoch-1, err)
		}
		var ok bool
		previousTrustBaseV1, ok = previousTrustBase.(*types.RootTrustBaseV1)
		if !ok {
			return fmt.Errorf("failed to cast previous trust base to version 1 for epoch %d", epoch)
		}
	}
	if err := trustBase.Verify(previousTrustBaseV1); err != nil {
		return fmt.Errorf("failed to verify trust base: %w", err)
	}
	return nil
}
