package mongodb

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/unicitynetwork/bft-go-base/types"
)

// CachedTrustBaseStorage is a cached decorator of TrustBaseStorage.
type CachedTrustBaseStorage struct {
	storage *TrustBaseStorage

	sortedTrustBases []types.RootTrustBase
	mu               sync.RWMutex
}

func NewCachedTrustBaseStorage(storage *TrustBaseStorage) *CachedTrustBaseStorage {
	return &CachedTrustBaseStorage{
		storage: storage,
	}
}

// Store stores a new trust base and updates the cache.
func (s *CachedTrustBaseStorage) Store(ctx context.Context, trustBase types.RootTrustBase) error {
	if err := s.storage.Store(ctx, trustBase); err != nil {
		return fmt.Errorf("failed to store trust base: %w", err)
	}
	if err := s.UpdateCache(ctx); err != nil {
		return fmt.Errorf("failed to reload cache: %w", err)
	}
	return nil
}

// GetByEpoch retrieves a trust base by epoch.
func (s *CachedTrustBaseStorage) GetByEpoch(_ context.Context, epoch uint64) (types.RootTrustBase, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if epoch < uint64(len(s.sortedTrustBases)) {
		return s.sortedTrustBases[epoch], nil
	}
	return nil, ErrTrustBaseNotFound
}

// GetByRound retrieves a trust base by epoch start round.
func (s *CachedTrustBaseStorage) GetByRound(_ context.Context, epochStart uint64) (types.RootTrustBase, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.getByEpochStartRound(epochStart)
}

// GetAll returns all trust bases in sorted order.
func (s *CachedTrustBaseStorage) GetAll(_ context.Context) ([]types.RootTrustBase, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.sortedTrustBases, nil
}

// UpdateCache updates the cache from storage.
func (s *CachedTrustBaseStorage) UpdateCache(ctx context.Context) error {
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

	// make sure trust bases are in sorted order by epoch
	sort.Slice(trustBases, func(i, j int) bool {
		return trustBases[i].GetEpoch() < trustBases[j].GetEpoch()
	})
	s.sortedTrustBases = trustBases
}

func (s *CachedTrustBaseStorage) getByEpochStartRound(epochStart uint64) (types.RootTrustBase, error) {
	for i := len(s.sortedTrustBases) - 1; i >= 0; i-- {
		if s.sortedTrustBases[i].GetEpochStart() <= epochStart {
			return s.sortedTrustBases[i], nil
		}
	}
	return nil, ErrTrustBaseNotFound
}
