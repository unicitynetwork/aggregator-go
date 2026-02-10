package mongodb

import (
	"context"
	"fmt"
	"sync"

	"github.com/unicitynetwork/bft-go-base/types"
)

// CachedTrustBaseStorage is a cached decorator of TrustBaseStorage.
type CachedTrustBaseStorage struct {
	storage *TrustBaseStorage

	trustBaseByEpoch map[uint64]*types.RootTrustBaseV1
	mu               sync.RWMutex
}

func NewCachedTrustBaseStorage(storage *TrustBaseStorage) *CachedTrustBaseStorage {
	return &CachedTrustBaseStorage{
		storage:          storage,
		trustBaseByEpoch: make(map[uint64]*types.RootTrustBaseV1),
	}
}

// Store stores a new trust base and updates the cache.
func (s *CachedTrustBaseStorage) Store(ctx context.Context, trustBase types.RootTrustBase) error {
	if err := s.storage.Store(ctx, trustBase); err != nil {
		return fmt.Errorf("failed to store trust base: %w", err)
	}
	if err := s.updateCache(trustBase); err != nil {
		return fmt.Errorf("failed to update cache: %w", err)
	}
	return nil
}

// GetByEpoch retrieves a trust base by epoch.
func (s *CachedTrustBaseStorage) GetByEpoch(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	tbFromCache := s.getByEpoch(epoch)
	if tbFromCache != nil {
		return tbFromCache, nil
	}

	// on cache miss delegate to storage because in HA mode the followers may not be up to date with the latest
	tbFromStorage, err := s.storage.GetByEpoch(ctx, epoch)
	if err != nil {
		return nil, fmt.Errorf("failed to load trust base from storage: %w", err)
	}
	if err := s.updateCache(tbFromStorage); err != nil {
		return nil, fmt.Errorf("failed to update cache: %w", err)
	}
	return tbFromStorage, nil
}

func (s *CachedTrustBaseStorage) getByEpoch(epoch uint64) *types.RootTrustBaseV1 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.trustBaseByEpoch[epoch]
}

// GetLatest retrieves the latest trust base.
func (s *CachedTrustBaseStorage) GetLatest(ctx context.Context) (*types.RootTrustBaseV1, error) {
	// always delegate to storage because in HA mode the followers may not be up to date with the latest
	tb, err := s.storage.GetLatest(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load latest trust base: %w", err)
	}
	if err := s.updateCache(tb); err != nil {
		return nil, fmt.Errorf("failed to update cache: %w", err)
	}
	return tb, nil
}

// GetTrustBases retrieves trust bases within the given epoch range.
func (s *CachedTrustBaseStorage) GetTrustBases(ctx context.Context, from, to uint64) ([]*types.RootTrustBaseV1, error) {
	var trustBases []*types.RootTrustBaseV1
	for i := from; i <= to; i++ {
		tb, err := s.GetByEpoch(ctx, i)
		if err != nil {
			return nil, fmt.Errorf("failed to load trust base for epoch %d: %w", i, err)
		}
		trustBases = append(trustBases, tb)
	}
	return trustBases, nil
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

func (s *CachedTrustBaseStorage) reloadCache(trustBases []*types.RootTrustBaseV1) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newCache := make(map[uint64]*types.RootTrustBaseV1, len(trustBases))
	for _, tb := range trustBases {
		newCache[tb.GetEpoch()] = tb
	}

	s.trustBaseByEpoch = newCache
}

func (s *CachedTrustBaseStorage) updateCache(trustBase types.RootTrustBase) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	trustBaseV1, ok := trustBase.(*types.RootTrustBaseV1)
	if !ok {
		return fmt.Errorf("failed to cast trust base to version 1 for epoch %d", trustBase.GetEpoch())
	}
	s.trustBaseByEpoch[trustBase.GetEpoch()] = trustBaseV1

	return nil
}
