package trustbase

import (
	"context"
	"errors"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

// TrustBaseManager manages trust base synchronization between BFT node and local state
type TrustBaseManager struct {
	log                *logger.Logger
	trustBaseStore     TrustBaseStore
	bftRestClient      BFTRestClient
	trustBaseValidator TrustBaseVerifier
}

type (
	TrustBaseStore interface {
		GetByEpoch(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error)
		Store(ctx context.Context, trustBase types.RootTrustBase) error
	}
	BFTRestClient interface {
		GetTrustBases(ctx context.Context, epoch1, epoch2 uint64) ([]*types.RootTrustBaseV1, error)
	}
	TrustBaseVerifier interface {
		Verify(ctx context.Context, trustBase types.RootTrustBase) error
	}
)

func NewTrustBaseManager(log *logger.Logger, trustBaseStore TrustBaseStore, bftRestClient BFTRestClient, trustBaseVerifier TrustBaseVerifier) *TrustBaseManager {
	return &TrustBaseManager{
		log:                log,
		trustBaseStore:     trustBaseStore,
		bftRestClient:      bftRestClient,
		trustBaseValidator: trustBaseVerifier,
	}
}

// GetAdmitted returns the trust base of the epoch only if it is already admitted to the
// local store. It never asks the BFT node and never stores anything: it is the lookup for
// history the aggregator already knows (for example the start of the next epoch, which
// ends the interval an epoch's keys may certify in).
func (s *TrustBaseManager) GetAdmitted(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, bool, error) {
	tb, err := s.trustBaseStore.GetByEpoch(ctx, epoch)
	if err != nil {
		if errors.Is(err, interfaces.ErrTrustBaseNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to load admitted trust base for epoch %d: %w", epoch, err)
	}
	return tb, tb != nil, nil
}

// GetByEpoch returns trust base from (in order of precedence):
// cache -> storage -> BFT node -> error if not found.
// If new trust base is found from BFT node then the trust base is persisted to TrustBaseStore before returning.
func (s *TrustBaseManager) GetByEpoch(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	// attempt to load from local store (cache + disk via decorator chain)
	tbFromCache, err := s.trustBaseStore.GetByEpoch(ctx, epoch)
	if err != nil && !errors.Is(err, interfaces.ErrTrustBaseNotFound) {
		return nil, fmt.Errorf("failed to load trust base for epoch %d: %w", epoch, err)
	}
	if tbFromCache != nil {
		return tbFromCache, nil
	}

	// not found locally: query BFT node
	trustBases, err := s.bftRestClient.GetTrustBases(ctx, epoch, epoch)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch trust base from BFT node for epoch %d: %w", epoch, err)
	}

	// validate trust base received from BFT node
	if len(trustBases) != 1 {
		return nil, fmt.Errorf("trust base for epoch %d not found for BFT node, len(trustBases)=%d", epoch, len(trustBases))
	}
	tb := trustBases[0]
	if err = s.trustBaseValidator.Verify(ctx, tb); err != nil {
		return nil, fmt.Errorf("failed to verify trust base received from BFT node for epoch %d: %w", epoch, err)
	}

	// store the received trust base
	if err = s.trustBaseStore.Store(ctx, tb); err != nil {
		if errors.Is(err, interfaces.ErrTrustBaseAlreadyExists) {
			s.log.WithContext(ctx).Warn("Trust base already stored, not overwriting it", "epoch", tb.GetEpoch())
		} else {
			return nil, fmt.Errorf("failed to store trust base for epoch %d: %w", epoch, err)
		}
	} else {
		s.log.WithContext(ctx).Info("Updated trust base storage with new trust base from BFT node", "epoch", tb.GetEpoch())
	}

	// return the received trust base
	return tb, nil
}
