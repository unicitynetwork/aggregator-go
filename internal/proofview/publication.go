package proofview

import (
	"bytes"
	"crypto/sha256"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// HasLatest accepts a compatible, superseding certification without taking a
// reader pin. Pin saturation is not a reason to rebuild an already retained view.
func (c *Catalog) HasLatest(number *api.BigInt, root, uc []byte) bool {
	certRoot, round, compat, err := decodeUC(uc)
	if err != nil || !bytes.Equal(certRoot, root) {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	v := c.tail
	return !c.closed && !c.latestStale && v != nil && number != nil &&
		v.block.Number.Cmp(number.Int) == 0 && bytes.Equal(v.root, root) &&
		v.attTail != nil && v.attTail.round >= round && v.attTail.compat == compat
}

// MarkLatestUnavailable records that the newest finalized state could not be
// retained. Latest then reports ErrViewNotRetained until a later publication
// succeeds at or above the failed block/certification watermark. Exact-root
// lookups of other retained views are unaffected.
func (c *Catalog) MarkLatestUnavailable(number *api.BigInt, round uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.markLatestUnavailableLocked(number, round)
}

func (c *Catalog) markLatestUnavailableLocked(number *api.BigInt, round uint64) {
	if number != nil && (c.curNumber == nil || number.Cmp(c.curNumber.Int) >= 0) {
		if c.curNumber == nil || number.Cmp(c.curNumber.Int) > 0 {
			c.curNumber = api.NewBigInt(nil)
			c.curNumber.Set(number.Int)
			c.curRound = round
		} else {
			c.curRound = max(c.curRound, round)
		}
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

// ViewForRootCompatible returns the identity of the newest retained view of the given root whose latest
// certification has this CompatDigest (the same accepted input record and configuration), without pinning
// it. Equal roots across blocks never select a view on the root alone. It is a single index probe,
// independent of how many retained states share the root.
func (c *Catalog) ViewForRootCompatible(root []byte, compat [sha256.Size]byte) (BlockIdentity, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.compatProbes++
	if cs := c.byCompat[compatKey{root: string(root), digest: compat}]; cs != nil && cs.newest != nil {
		return cs.newest.block.clone(), true
	}
	return BlockIdentity{}, false
}

// HasCertifiedRoot reports whether some retained view of the root has a certification (O(1)).
func (c *Catalog) HasCertifiedRoot(root []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.compatProbes++
	return c.rootCertified[string(root)] > 0
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
