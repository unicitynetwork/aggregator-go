package api

import (
	"bytes"
	"errors"
	"fmt"
)

var (
	ErrCertDepthOverlap      = errors.New("inclusion cert: parent and child cert overlap in depth")
	ErrCertDepthOrder        = errors.New("inclusion cert: parent cert depths must be shallower than child cert depths")
	ErrCertChildRootMismatch = errors.New("inclusion cert: parent fragment shard leaf value does not match child root")
	ErrCertMissingChild      = errors.New("inclusion cert: missing child cert")
	ErrCertMissingParent     = errors.New("inclusion cert: missing parent fragment")
)

// ComposeInclusionCert merges a child inclusion certificate with the stored
// parent proof fragment for the child shard. The result is a single public
// InclusionCert that can later be verified against the parent UC.IR.h.
//
// Invariants enforced here:
//   - parentFragment.ShardLeafValue must equal childRoot
//   - parent fragment certificate bytes must decode as a valid InclusionCert
//   - parent and child bitmaps must not overlap in depth
//   - if both certs contain siblings, every parent depth must be shallower
//     than every child depth
//   - merged siblings stay in root-to-leaf order: parent first, then child
func ComposeInclusionCert(parentFragment *ParentInclusionFragment, child *InclusionCert, childRoot []byte) (*InclusionCert, error) {
	if parentFragment == nil {
		return nil, ErrCertMissingParent
	}
	if child == nil {
		return nil, ErrCertMissingChild
	}
	if len(childRoot) != SiblingSize {
		return nil, fmt.Errorf("%w: got %d, want %d", ErrCertRootLength, len(childRoot), SiblingSize)
	}
	if len(parentFragment.ShardLeafValue) != SiblingSize {
		return nil, fmt.Errorf("invalid parent fragment shard leaf value length: got %d, want %d",
			len(parentFragment.ShardLeafValue), SiblingSize)
	}
	if !bytes.Equal(parentFragment.ShardLeafValue, childRoot) {
		return nil, ErrCertChildRootMismatch
	}

	var parent InclusionCert
	if err := parent.UnmarshalBinary(parentFragment.CertificateBytes); err != nil {
		return nil, fmt.Errorf("failed to decode parent fragment cert: %w", err)
	}

	if bitmapOverlap(&parent.Bitmap, &child.Bitmap) {
		return nil, ErrCertDepthOverlap
	}

	parentHasDepths, _, parentMax := bitmapDepthRange(&parent.Bitmap)
	childHasDepths, childMin, _ := bitmapDepthRange(&child.Bitmap)
	if parentHasDepths && childHasDepths && parentMax >= childMin {
		return nil, fmt.Errorf("%w: deepest parent depth %d, shallowest child depth %d",
			ErrCertDepthOrder, parentMax, childMin)
	}

	out := &InclusionCert{}
	for i := range out.Bitmap {
		out.Bitmap[i] = parent.Bitmap[i] | child.Bitmap[i]
	}
	out.Siblings = make([][SiblingSize]byte, 0, len(parent.Siblings)+len(child.Siblings))
	out.Siblings = append(out.Siblings, parent.Siblings...)
	out.Siblings = append(out.Siblings, child.Siblings...)

	if len(out.Siblings) != bitmapPopcount(&out.Bitmap) {
		return nil, fmt.Errorf("%w: have %d, want %d",
			ErrCertBitmapMismatch, len(out.Siblings), bitmapPopcount(&out.Bitmap))
	}
	return out, nil
}

func bitmapOverlap(a, b *[BitmapSize]byte) bool {
	for i := 0; i < BitmapSize; i++ {
		if a[i]&b[i] != 0 {
			return true
		}
	}
	return false
}

func bitmapDepthRange(bitmap *[BitmapSize]byte) (ok bool, minDepth, maxDepthSeen int) {
	minDepth = BitmapSize * 8
	for depth := 0; depth < BitmapSize*8; depth++ {
		if (bitmap[depth/8]>>(uint(depth)%8))&1 == 0 {
			continue
		}
		if !ok {
			minDepth = depth
			maxDepthSeen = depth
			ok = true
			continue
		}
		maxDepthSeen = depth
	}
	return ok, minDepth, maxDepthSeen
}
