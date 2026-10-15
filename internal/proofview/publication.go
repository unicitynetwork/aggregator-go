package proofview

import "crypto/sha256"

// MarkLatestUnavailable records that the newest finalized state could not be
// retained. Latest then reports ErrViewNotRetained until a later publication
// succeeds; it never falls back to an older view. Exact-root lookups of other
// retained views are unaffected.
func (c *Catalog) MarkLatestUnavailable() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.latestStale = true
	c.curView = nil
}

// ViewForDigest returns the identity and root of the retained view which holds the
// certification with exactly this UC digest (SHA-256 of its encoding), without pinning
// it. The digest identifies the certification exactly, so a late partition view reaches
// the right block even when newer blocks share the same state root.
func (c *Catalog) ViewForDigest(digest [sha256.Size]byte) (BlockIdentity, []byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, _ := c.newestWithDigestLocked(digest); v != nil {
		return v.block.clone(), append([]byte(nil), v.root...), true
	}
	return BlockIdentity{}, nil, false
}

// ViewForRootMatching returns the identity of the newest retained view with the given
// root whose latest certification satisfies match (eg is the same input record and
// configuration), without pinning it. Equal roots across blocks never select a view on
// the root alone.
func (c *Catalog) ViewForRootMatching(root []byte, match func(latestUC []byte) bool) (BlockIdentity, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for v := c.byRoot[string(root)]; v != nil; v = v.rootOlder {
		if v.attTail != nil && match(v.attTail.uc) {
			return v.block.clone(), true
		}
	}
	return BlockIdentity{}, false
}

// PartitionViewForDigest returns a copy of the partition view attached to the retained
// certification with this exact UC digest (nil, true when it has none; false when no
// retained view holds that certification). Diagnostic/test inspection; it pins nothing.
func (c *Catalog) PartitionViewForDigest(digest [sha256.Size]byte) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, a := c.newestWithDigestLocked(digest); a != nil {
		return append([]byte(nil), a.view...), true
	}
	return nil, false
}
