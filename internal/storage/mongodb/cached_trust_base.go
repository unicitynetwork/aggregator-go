package mongodb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

// CachedTrustBaseStorage is a cached decorator of TrustBaseStorage.
type CachedTrustBaseStorage struct {
	storage *TrustBaseStorage

	trustBaseByEpoch map[uint64]types.RootTrustBase
	mu               sync.RWMutex
}

func NewCachedTrustBaseStorage(storage *TrustBaseStorage) *CachedTrustBaseStorage {
	return &CachedTrustBaseStorage{
		storage:          storage,
		trustBaseByEpoch: make(map[uint64]types.RootTrustBase),
	}
}

// Store stores a new trust base and updates the cache.
func (s *CachedTrustBaseStorage) Store(ctx context.Context, trustBase types.RootTrustBase) error {
	if err := s.storage.Store(ctx, trustBase); err != nil {
		return fmt.Errorf("failed to store trust base: %w", err)
	}
	s.updateCache(trustBase)
	return nil
}

// GetByEpoch retrieves a trust base by epoch.
// Falls back to the only available trust base if the requested epoch is not found.
func (s *CachedTrustBaseStorage) GetByEpoch(ctx context.Context, epoch uint64) (types.RootTrustBase, error) {
	tbFromCache := s.getByEpoch(epoch)
	if tbFromCache != nil {
		return tbFromCache, nil
	}

	// in HA mode another node may have updated the trust base,
	// so we must check storage
	tbFromStorage, err := s.storage.GetByEpoch(ctx, epoch)
	if err == nil {
		s.updateCache(tbFromStorage)
		return tbFromStorage, nil
	}

	if errors.Is(err, interfaces.ErrTrustBaseNotFound) {
		if fallbackTB := s.tryFallbackToSingleTrustBase(ctx, epoch); fallbackTB != nil {
			return fallbackTB, nil
		}
	}

	return nil, fmt.Errorf("failed to fetch trust base from storage: %w", err)
}

// tryFallbackToSingleTrustBase returns the only trust base if exactly one exists.
func (s *CachedTrustBaseStorage) tryFallbackToSingleTrustBase(ctx context.Context, requestedEpoch uint64) types.RootTrustBase {
	allTrustBases, err := s.storage.GetAll(ctx)
	if err != nil || len(allTrustBases) != 1 {
		return nil
	}

	tb := allTrustBases[0]
	slog.Warn("Using fallback trust base", "requestedEpoch", requestedEpoch, "actualEpoch", tb.GetEpoch())
	s.updateCache(tb)
	return tb
}

func (s *CachedTrustBaseStorage) getByEpoch(epoch uint64) types.RootTrustBase {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.trustBaseByEpoch[epoch]
}

// ReloadCache reloads the cache from storage.
func (s *CachedTrustBaseStorage) ReloadCache(ctx context.Context) error {
	trustBases, err := s.storage.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to get all trust bases: %w", err)
	}
	s.reloadCache(trustBases)
	return nil
}

func (s *CachedTrustBaseStorage) reloadCache(trustBases []types.RootTrustBase) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newCache := make(map[uint64]types.RootTrustBase, len(trustBases))
	for _, tb := range trustBases {
		newCache[tb.GetEpoch()] = tb
	}

	s.trustBaseByEpoch = newCache
}

func (s *CachedTrustBaseStorage) updateCache(trustBase types.RootTrustBase) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.trustBaseByEpoch[trustBase.GetEpoch()] = trustBase
}

// GetAll retrieves all trust bases from storage.
func (s *CachedTrustBaseStorage) GetAll(ctx context.Context) ([]types.RootTrustBase, error) {
	return s.storage.GetAll(ctx)
}
