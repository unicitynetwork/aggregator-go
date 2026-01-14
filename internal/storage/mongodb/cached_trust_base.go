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
func (s *CachedTrustBaseStorage) GetByEpoch(ctx context.Context, epoch uint64) (types.RootTrustBase, error) {
	tbFromCache := s.getByEpoch(epoch)
	if tbFromCache != nil {
		return tbFromCache, nil
	}

	// in HA mode another node may have updated the trust base,
	// so we must check storage
	tbFromStorage, err := s.storage.GetByEpoch(ctx, epoch)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch trust base from storage: %w", err)
	}
	s.updateCache(tbFromStorage)

	return tbFromStorage, nil
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
