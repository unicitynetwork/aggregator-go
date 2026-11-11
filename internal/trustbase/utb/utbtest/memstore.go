package utbtest

import (
	"bytes"
	"context"
	"sort"
	"sync"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/trustbase/utb"
)

// MemStore is an in-memory utb.Store for tests, with hooks to make writes fail. Two histories sharing one MemStore behave like two aggregators sharing a
// database. It is never a production fallback.
type MemStore struct {
	mu      sync.Mutex
	bound   bool
	meta    utb.Meta
	entries map[uint64][]byte

	// PutHook runs before every Put. If it returns an error the Put fails; with WriteAnyway set the bytes are stored first (an ambiguous failure).
	PutHook func(epoch uint64) (writeAnyway bool, err error)
	// AdvanceHook runs before every Advance; an error fails it.
	AdvanceHook func(epoch uint64) error
	// Puts counts the successful writes of new entries.
	Puts int
}

// NewMemStore returns an empty, unbound store.
func NewMemStore() *MemStore { return &MemStore{entries: map[uint64][]byte{}} }

func (s *MemStore) Bind(_ context.Context, network types.NetworkID, pin [types.TrustBaseV2IDLen]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.bound {
		s.bound, s.meta.Network, s.meta.Pin = true, network, pin
		return nil
	}
	if s.meta.Network != network || s.meta.Pin != pin {
		return utb.ErrStoreMismatch
	}
	return nil
}

func (s *MemStore) Load(_ context.Context, from uint64) (utb.Meta, [][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var epochs []uint64
	for e := range s.entries {
		if e >= from {
			epochs = append(epochs, e)
		}
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })
	var out [][]byte
	for _, e := range epochs {
		out = append(out, bytes.Clone(s.entries[e]))
	}
	return s.meta, out, nil
}

func (s *MemStore) Get(_ context.Context, epoch uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.entries[epoch]
	if !ok {
		return nil, utb.ErrNotStored
	}
	return bytes.Clone(raw), nil
}

func (s *MemStore) Put(_ context.Context, epoch uint64, raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.PutHook != nil {
		writeAnyway, err := s.PutHook(epoch)
		if err != nil {
			if writeAnyway {
				if _, held := s.entries[epoch]; !held {
					s.entries[epoch] = bytes.Clone(raw)
				}
			}
			return err
		}
	}
	if _, held := s.entries[epoch]; held {
		return utb.ErrExists
	}
	s.entries[epoch] = bytes.Clone(raw)
	s.Puts++
	return nil
}

func (s *MemStore) Advance(_ context.Context, epoch uint64, id [types.TrustBaseV2IDLen]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.AdvanceHook != nil {
		if err := s.AdvanceHook(epoch); err != nil {
			return err
		}
	}
	if epoch > s.meta.TipEpoch {
		s.meta.TipEpoch, s.meta.TipID = epoch, id
	}
	return nil
}

// Drop deletes a stored entry (a lost or corrupt tail); Corrupt replaces its bytes.
func (s *MemStore) Drop(epoch uint64) { s.mu.Lock(); delete(s.entries, epoch); s.mu.Unlock() }

// Corrupt overwrites the stored bytes of an epoch.
func (s *MemStore) Corrupt(epoch uint64, raw []byte) {
	s.mu.Lock()
	s.entries[epoch] = raw
	s.mu.Unlock()
}

// Tip returns the durable tip epoch.
func (s *MemStore) Tip() uint64 { s.mu.Lock(); defer s.mu.Unlock(); return s.meta.TipEpoch }

// Has reports whether an epoch is stored.
func (s *MemStore) Has(epoch uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.entries[epoch]
	return ok
}
