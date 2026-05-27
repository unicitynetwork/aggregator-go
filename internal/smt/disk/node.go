package disk

import "fmt"

type BranchKind uint8

const (
	BranchKindInternal BranchKind = iota
	BranchKindLeaf
	BranchKindStub
)

type Branch struct {
	Kind     BranchKind
	Leaf     *LeafNode
	Internal *InternalNode
	StubHash Hash
}

type LeafNode struct {
	Key   Key
	Value []byte
	Hash  Hash
}

type InternalNode struct {
	Path  CompressedPath
	Depth uint8
	Left  *Branch
	Right *Branch
	Hash  Hash
}

func NewLeaf(key Key, value []byte) *Branch {
	valueCopy := append([]byte(nil), value...)
	return &Branch{
		Kind: BranchKindLeaf,
		Leaf: &LeafNode{
			Key:   key,
			Value: valueCopy,
			Hash:  HashLeaf(key, valueCopy),
		},
	}
}

func NewInternal(path CompressedPath, depth uint8, left, right *Branch) (*Branch, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("disk smt: internal node requires both children")
	}
	leftHash, err := left.HashValue()
	if err != nil {
		return nil, fmt.Errorf("disk smt: left child hash: %w", err)
	}
	rightHash, err := right.HashValue()
	if err != nil {
		return nil, fmt.Errorf("disk smt: right child hash: %w", err)
	}
	return &Branch{
		Kind: BranchKindInternal,
		Internal: &InternalNode{
			Path:  path,
			Depth: depth,
			Left:  left,
			Right: right,
			Hash:  HashNode(leftHash, rightHash, depth),
		},
	}, nil
}

func NewStub(hash Hash) *Branch {
	return &Branch{
		Kind:     BranchKindStub,
		StubHash: hash,
	}
}

func (b *Branch) HashValue() (Hash, error) {
	if b == nil {
		return Hash{}, fmt.Errorf("disk smt: nil branch")
	}
	switch b.Kind {
	case BranchKindLeaf:
		if b.Leaf == nil {
			return Hash{}, fmt.Errorf("disk smt: leaf branch missing leaf node")
		}
		return b.Leaf.Hash, nil
	case BranchKindInternal:
		if b.Internal == nil {
			return Hash{}, fmt.Errorf("disk smt: internal branch missing internal node")
		}
		return b.Internal.Hash, nil
	case BranchKindStub:
		return b.StubHash, nil
	default:
		return Hash{}, fmt.Errorf("disk smt: unknown branch kind %d", b.Kind)
	}
}
