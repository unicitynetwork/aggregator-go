package disk

import (
	"bytes"
	"fmt"
)

const rootNodeKeyDepth = uint16(0xffff)

type PrefixBits [KeySize]byte

// NodeKey is the local-storage key for a persisted SMT node. It encodes the
// absolute routing prefix accumulated from the root. The root itself uses a
// sentinel and does not collide with the depth-zero non-root key.
type NodeKey struct {
	root   bool
	depth  uint16
	prefix PrefixBits
}

func RootNodeKey() NodeKey {
	return NodeKey{root: true, depth: rootNodeKeyDepth}
}

func NewNodeKey(depth int, prefix PrefixBits) (NodeKey, error) {
	if depth < 0 || depth > KeyBits {
		return NodeKey{}, fmt.Errorf("disk smt: invalid node key depth %d", depth)
	}
	key := NodeKey{depth: uint16(depth), prefix: prefix}
	key.clearUnusedPrefixBits()
	return key, nil
}

func ParseNodeKey(data []byte) (NodeKey, error) {
	if len(data) < 2 {
		return NodeKey{}, fmt.Errorf("disk smt: node key too short: %d", len(data))
	}
	depth := uint16(data[0]) | uint16(data[1])<<8
	if depth == rootNodeKeyDepth {
		if len(data) != 2 {
			return NodeKey{}, fmt.Errorf("disk smt: root node key has trailing bytes")
		}
		return RootNodeKey(), nil
	}
	if depth > KeyBits {
		return NodeKey{}, fmt.Errorf("disk smt: invalid node key depth %d", depth)
	}
	byteLen := prefixByteLen(int(depth))
	if len(data) != 2+byteLen {
		return NodeKey{}, fmt.Errorf("disk smt: invalid node key length: got %d, want %d", len(data), 2+byteLen)
	}
	var prefix PrefixBits
	copy(prefix[:], data[2:])
	key, err := NewNodeKey(int(depth), prefix)
	if err != nil {
		return NodeKey{}, err
	}
	if !bytes.Equal(key.Bytes(), data) {
		return NodeKey{}, fmt.Errorf("disk smt: node key has non-zero unused bits")
	}
	return key, nil
}

func (k NodeKey) IsRoot() bool {
	return k.root
}

func (k NodeKey) DepthBits() int {
	if k.root {
		return 0
	}
	return int(k.depth)
}

func (k NodeKey) Bytes() []byte {
	return k.AppendBytes(nil)
}

func (k NodeKey) AppendBytes(dst []byte) []byte {
	if k.root {
		return append(dst, 0xff, 0xff)
	}
	depth := int(k.depth)
	dst = append(dst, byte(k.depth), byte(k.depth>>8))
	return append(dst, k.prefix[:prefixByteLen(depth)]...)
}

func (k NodeKey) Less(other NodeKey) bool {
	left0, left1 := byte(k.depth), byte(k.depth>>8)
	if k.root {
		left0, left1 = 0xff, 0xff
	}
	right0, right1 := byte(other.depth), byte(other.depth>>8)
	if other.root {
		right0, right1 = 0xff, 0xff
	}

	if left0 != right0 {
		return left0 < right0
	}
	if left1 != right1 {
		return left1 < right1
	}
	if k.root || other.root {
		return false
	}
	return bytes.Compare(k.prefix[:prefixByteLen(int(k.depth))], other.prefix[:prefixByteLen(int(other.depth))]) < 0
}

func (k *NodeKey) clearUnusedPrefixBits() {
	if k.root {
		k.prefix = PrefixBits{}
		return
	}
	depth := int(k.depth)
	byteLen := prefixByteLen(depth)
	if byteLen < len(k.prefix) {
		for i := byteLen; i < len(k.prefix); i++ {
			k.prefix[i] = 0
		}
	}
	if rem := depth % 8; rem != 0 {
		k.prefix[byteLen-1] &= byte(1<<uint(rem)) - 1
	}
}
