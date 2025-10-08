package state

import (
	"math/big"
	"sync/atomic"
)

// Tracker is a thread safe wrapper for last synced block number,
// used by multiple readers and writers.
type Tracker struct {
	lastSyncedBlock atomic.Value // stores a *big.Int
}

func NewSyncStateTracker() *Tracker {
	t := &Tracker{}
	t.lastSyncedBlock.Store(big.NewInt(0))
	return t
}

func (t *Tracker) SetLastSyncedBlock(num *big.Int) {
	t.lastSyncedBlock.Store(num)
}

func (t *Tracker) GetLastSyncedBlock() *big.Int {
	return t.lastSyncedBlock.Load().(*big.Int)
}
