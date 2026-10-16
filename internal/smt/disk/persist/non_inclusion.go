package persist

import (
	"errors"
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ErrNonInclusionCorrupt: the stored tree does not have the shape the certificate walk relies
// on (non binary node, inconsistent depth, unsupported terminal value). It is a storage failure,
// never an absence.
var ErrNonInclusionCorrupt = errors.New("disk SMT persist: tree is not walkable for a non-inclusion certificate")

// BuildNonInclusionCert builds the non-inclusion certificate of key against the tree at rootHash,
// reading ONLY from reader (a pinned read snapshot for retained views): the terminal leaf
// reached by following the key's bit at every actual junction, with the junction siblings.
//
// Unlike the inclusion materializer, which stops at the first compressed-prefix divergence,
// the walk continues past every divergence: a compressed edge carries no junction, so the
// query's bits inside it do not matter, and the first junction below always has a defined
// side. Child node keys come from each node's stored path, never from the query, so only
// nodes of the retained version are read. Every loaded node is checked against the hash its
// parent commits to, and a missing or inconsistent node fails the call.
//
// The empty tree yields the empty-tree certificate (no reader access). It returns
// api.ErrStateIncluded when the terminal is the key's own leaf.
func BuildNonInclusionCert(rootHash disk.Hash, reader storage.ReadStore, key []byte) (*api.NonInclusionCert, error) {
	if len(key) != api.StateTreeKeyLengthBytes {
		return nil, fmt.Errorf("%w: got %d, want %d", api.ErrCertKeyLength, len(key), api.StateTreeKeyLengthBytes)
	}
	diskKey, err := disk.KeyFromBytes(key)
	if err != nil {
		return nil, err
	}
	if rootHash == disk.EmptyRootHash() {
		return api.EmptyTreeNonInclusionCert(), nil
	}
	if reader == nil {
		return nil, fmt.Errorf("disk SMT persist: nil read store")
	}

	var bitmap [api.BitmapSize]byte
	var sibs [][api.SiblingSize]byte
	branch, err := loadProofBranch(reader, disk.RootNodeKey(), rootHash)
	if err != nil {
		return nil, err
	}
	var prefix disk.PrefixBits
	startBit := 0
	prevDepth := -1
	for branch.Kind != disk.BranchKindLeaf {
		if branch.Kind != disk.BranchKindInternal || branch.Internal == nil {
			return nil, fmt.Errorf("%w: unexpected branch kind %d", ErrNonInclusionCorrupt, branch.Kind)
		}
		node := branch.Internal
		split := startBit + node.Path.Len()
		if split != int(node.Depth) || split <= prevDepth || split >= api.StateTreeKeyLengthBits {
			return nil, fmt.Errorf("%w: node depth %d at split bit %d", ErrNonInclusionCorrupt, node.Depth, split)
		}
		if node.Left == nil || node.Right == nil {
			return nil, fmt.Errorf("%w: unary node at depth %d", ErrNonInclusionCorrupt, split)
		}
		direction := disk.KeyBit(diskKey, split)
		child, sibling := node.Left, node.Right
		if direction == 1 {
			child, sibling = node.Right, node.Left
		}
		siblingHash, err := sibling.HashValue()
		if err != nil {
			return nil, err
		}
		api.SetBitBE(bitmap[:], split)
		var sb [api.SiblingSize]byte
		copy(sb[:], siblingHash[:])
		sibs = append(sibs, sb)

		childKey, childPrefix, childStart, err := childNodeKey(prefix, startBit, node.Path, direction)
		if err != nil {
			return nil, err
		}
		prevDepth, prefix, startBit = split, childPrefix, childStart
		switch child.Kind {
		case disk.BranchKindStub:
			branch, err = loadProofBranch(reader, childKey, child.StubHash)
			if err != nil {
				return nil, err
			}
		case disk.BranchKindLeaf, disk.BranchKindInternal:
			branch = child
		default:
			return nil, fmt.Errorf("%w: unknown child kind %d", ErrNonInclusionCorrupt, child.Kind)
		}
	}
	leaf := branch.Leaf
	if leaf == nil || len(leaf.Value) != api.NonInclusionTerminalValueSize {
		return nil, fmt.Errorf("%w: terminal leaf", ErrNonInclusionCorrupt)
	}
	if leaf.Key == diskKey {
		return nil, api.ErrStateIncluded
	}
	return api.NewNonInclusionCert(bitmap, sibs, leaf.Key[:], leaf.Value)
}
