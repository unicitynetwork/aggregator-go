package disk

import (
	"encoding/binary"
	"fmt"
)

const (
	TagInternal byte = 0x00
	TagLeaf     byte = 0x01
)

func SerializedTag(data []byte) (byte, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("disk smt: empty serialized node")
	}
	return data[0], nil
}

func MarshalLeaf(leaf *LeafNode) ([]byte, error) {
	if leaf == nil {
		return nil, fmt.Errorf("disk smt: nil leaf")
	}
	out := make([]byte, 0, 1+KeySize+binary.MaxVarintLen64+len(leaf.Value)+HashSize)
	out = append(out, TagLeaf)
	out = append(out, leaf.Key[:]...)
	out = binary.AppendUvarint(out, uint64(len(leaf.Value)))
	out = append(out, leaf.Value...)
	out = append(out, leaf.Hash[:]...)
	return out, nil
}

func UnmarshalLeaf(data []byte) (*LeafNode, error) {
	if len(data) < 1+KeySize+1+HashSize {
		return nil, fmt.Errorf("disk smt: serialized leaf too short: %d", len(data))
	}
	if data[0] != TagLeaf {
		return nil, fmt.Errorf("disk smt: serialized leaf has tag %d", data[0])
	}
	pos := 1

	var key Key
	copy(key[:], data[pos:pos+KeySize])
	pos += KeySize

	valueLen, n := binary.Uvarint(data[pos:])
	if n <= 0 {
		return nil, fmt.Errorf("disk smt: invalid leaf value length varint")
	}
	pos += n
	if valueLen > uint64(len(data)-pos-HashSize) {
		return nil, fmt.Errorf("disk smt: leaf value length %d exceeds serialized size", valueLen)
	}
	valueEnd := pos + int(valueLen)
	if len(data) != valueEnd+HashSize {
		return nil, fmt.Errorf("disk smt: serialized leaf has trailing bytes: got %d, want %d", len(data), valueEnd+HashSize)
	}

	value := append([]byte(nil), data[pos:valueEnd]...)
	pos = valueEnd

	var hash Hash
	copy(hash[:], data[pos:pos+HashSize])

	expectedHash := HashLeaf(key, value)
	if hash != expectedHash {
		return nil, fmt.Errorf("disk smt: serialized leaf hash mismatch")
	}

	return &LeafNode{
		Key:   key,
		Value: value,
		Hash:  hash,
	}, nil
}

func MarshalInternal(node *InternalNode) ([]byte, error) {
	if node == nil {
		return nil, fmt.Errorf("disk smt: nil internal node")
	}
	leftHash, err := node.Left.HashValue()
	if err != nil {
		return nil, fmt.Errorf("disk smt: left child hash: %w", err)
	}
	rightHash, err := node.Right.HashValue()
	if err != nil {
		return nil, fmt.Errorf("disk smt: right child hash: %w", err)
	}
	pathBytes := node.Path.Bytes()
	out := make([]byte, 0, 1+1+1+len(pathBytes)+2*HashSize)
	out = append(out, TagInternal)
	out = append(out, node.Depth)
	out = append(out, byte(node.Path.Len()))
	out = append(out, pathBytes...)
	out = append(out, leftHash[:]...)
	out = append(out, rightHash[:]...)
	return out, nil
}

func UnmarshalInternal(data []byte) (*InternalNode, error) {
	if len(data) < 1+1+1+2*HashSize {
		return nil, fmt.Errorf("disk smt: serialized internal node too short: %d", len(data))
	}
	if data[0] != TagInternal {
		return nil, fmt.Errorf("disk smt: serialized internal node has tag %d", data[0])
	}
	pos := 1

	depth := data[pos]
	pos++

	pathLen := int(data[pos])
	pos++
	pathByteLen := prefixByteLen(pathLen)
	if len(data) < pos+pathByteLen {
		return nil, fmt.Errorf("disk smt: serialized internal path truncated")
	}
	path, err := NewCompressedPathFromRaw(pathLen, data[pos:pos+pathByteLen])
	if err != nil {
		return nil, err
	}
	pos += pathByteLen

	if len(data) < pos+2*HashSize {
		return nil, fmt.Errorf("disk smt: serialized internal child hashes truncated")
	}
	leftHash := Hash{}
	copy(leftHash[:], data[pos:pos+HashSize])
	pos += HashSize
	rightHash := Hash{}
	copy(rightHash[:], data[pos:pos+HashSize])
	pos += HashSize
	if len(data) != pos {
		return nil, fmt.Errorf("disk smt: serialized internal node has trailing bytes: got %d, consumed %d", len(data), pos)
	}
	hash := HashNode(leftHash, rightHash, depth)

	return &InternalNode{
		Path:  path,
		Depth: depth,
		Left:  NewStub(leftHash),
		Right: NewStub(rightHash),
		Hash:  hash,
	}, nil
}
