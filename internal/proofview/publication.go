package proofview

import "bytes"

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

// BlockForRoot returns the local block identity of the newest retained view
// with the given root, without pinning it. Used to attach certifications (repeat
// UCs, late partition views) to the unchanged finalized state they certify.
func (c *Catalog) BlockForRoot(root []byte) (BlockIdentity, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.views) - 1; i >= 0; i-- {
		if bytes.Equal(c.views[i].root, root) {
			return c.views[i].block.clone(), true
		}
	}
	return BlockIdentity{}, false
}

// ViewForRoot returns the identity and the latest certification's UC bytes of the newest
// retained view with the given root, without pinning it. Callers use it to check that
// further certifications are compatible with that state before attaching them.
func (c *Catalog) ViewForRoot(root []byte) (BlockIdentity, []byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := len(c.views) - 1; i >= 0; i-- {
		v := c.views[i]
		if bytes.Equal(v.root, root) && len(v.atts) > 0 {
			return v.block.clone(), append([]byte(nil), v.atts[len(v.atts)-1].uc...), true
		}
	}
	return BlockIdentity{}, nil, false
}
