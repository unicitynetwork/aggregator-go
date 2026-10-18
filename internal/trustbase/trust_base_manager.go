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
	trustBaseStore     ManagerStore
	bftRestClient      BFTRestClient
	trustBaseValidator TrustBaseVerifier
}

type (
	TrustBaseStore interface {
		GetByEpoch(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error)
		Store(ctx context.Context, trustBase types.RootTrustBase) error
	}
	// ManagerStore is the store the manager needs: the validator's view plus the admitted head.
	ManagerStore interface {
		TrustBaseStore
		GetLatest(ctx context.Context) (*types.RootTrustBaseV1, error)
	}
	BFTRestClient interface {
		GetTrustBases(ctx context.Context, epoch1, epoch2 uint64) ([]*types.RootTrustBaseV1, error)
	}
	TrustBaseVerifier interface {
		Verify(ctx context.Context, trustBase types.RootTrustBase) error
	}
)

func NewTrustBaseManager(log *logger.Logger, trustBaseStore ManagerStore, bftRestClient BFTRestClient, trustBaseVerifier TrustBaseVerifier) *TrustBaseManager {
	return &TrustBaseManager{
		log:                log,
		trustBaseStore:     trustBaseStore,
		bftRestClient:      bftRestClient,
		trustBaseValidator: trustBaseVerifier,
	}
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

// ErrNextEpochUnavailable is returned by AdmitNext when the BFT node has no trust base for the epoch
// after the admitted head (or answered with something unusable).
var ErrNextEpochUnavailable = errors.New("the next trust base epoch is not available")

// GetByEpochLocal returns the trust base of the epoch from the local store only: it never queries the
// BFT node, so it is safe to call for every received message.
func (s *TrustBaseManager) GetByEpochLocal(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	return s.trustBaseStore.GetByEpoch(ctx, epoch)
}

// Head returns the highest admitted epoch. Admission only ever extends the history by one verified
// successor, so the stored epochs below the head are contiguous.
func (s *TrustBaseManager) Head(ctx context.Context) (uint64, error) {
	tb, err := s.trustBaseStore.GetLatest(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to load the latest trust base: %w", err)
	}
	return tb.GetEpoch(), nil
}

// AdmitNext asks the BFT node for the epoch after the admitted head, verifies it against its
// predecessor (previous-trust-base hash, quorum of the previous epoch's validators, network) and stores
// it. It never trusts or follows an epoch number that came from a received message, and never skips a
// predecessor. The admitted epoch is returned.
func (s *TrustBaseManager) AdmitNext(ctx context.Context) (uint64, error) {
	head, err := s.Head(ctx)
	if err != nil {
		return 0, err
	}
	next := head + 1
	trustBases, err := s.bftRestClient.GetTrustBases(ctx, next, next)
	if err != nil {
		return 0, fmt.Errorf("%w: epoch %d: %w", ErrNextEpochUnavailable, next, err)
	}
	if len(trustBases) != 1 || trustBases[0] == nil || trustBases[0].GetEpoch() != next {
		return 0, fmt.Errorf("%w: epoch %d", ErrNextEpochUnavailable, next)
	}
	tb := trustBases[0]
	if err := s.trustBaseValidator.Verify(ctx, tb); err != nil {
		return 0, fmt.Errorf("failed to verify trust base for epoch %d: %w", next, err)
	}
	if err := s.trustBaseStore.Store(ctx, tb); err != nil {
		if !errors.Is(err, interfaces.ErrTrustBaseAlreadyExists) {
			return 0, fmt.Errorf("failed to store trust base for epoch %d: %w", next, err)
		}
	}
	s.log.Info("Admitted trust base epoch", "epoch", next)
	return next, nil
}
