package proofmaterial

import (
	"container/list"
	"sync"
)

// DefaultStagingEntries bounds the staging cache.
const DefaultStagingEntries = 64

// Staging is a bounded, UC-keyed cache of validated partition views waiting for
// their exact UC to be accepted and finalized. Entries are keyed by the complete UC
// digest; a view can never be associated with another UC. Least recently added
// entries are evicted first.
type Staging struct {
	mu      sync.Mutex
	max     int
	order   *list.List // front = newest
	entries map[Digest]*list.Element
}

type stagedEntry struct {
	digest Digest
	view   []byte
}

func NewStaging(maxEntries int) *Staging {
	if maxEntries <= 0 {
		maxEntries = DefaultStagingEntries
	}
	return &Staging{max: maxEntries, order: list.New(), entries: map[Digest]*list.Element{}}
}

// A nil *Staging is a valid empty cache which stores nothing.
//
// Put stores a private copy of the view. An existing entry for the digest is kept
// (a validated view for a UC is unique), only refreshed in recency.
func (s *Staging) Put(d Digest, view []byte) {
	if s == nil || len(view) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.entries[d]; ok {
		s.order.MoveToFront(el)
		return
	}
	s.entries[d] = s.order.PushFront(&stagedEntry{digest: d, view: append([]byte(nil), view...)})
	for s.order.Len() > s.max {
		last := s.order.Back()
		delete(s.entries, last.Value.(*stagedEntry).digest)
		s.order.Remove(last)
	}
}

// Get returns the staged view (shared, immutable) for the exact UC digest.
func (s *Staging) Get(d Digest) ([]byte, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	el, ok := s.entries[d]
	if !ok {
		return nil, false
	}
	return el.Value.(*stagedEntry).view, true
}

func (s *Staging) Len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.order.Len()
}
