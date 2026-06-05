package disk

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTreeApplyFirstLeafMatchesMemory(t *testing.T) {
	tree := NewTree()
	key := keyWithFirstByte(0x01)
	value := []byte("value-one")

	result, err := tree.ApplyLeaves([]LeafInput{treeInput(key, value)})
	require.NoError(t, err)

	require.Equal(t, emptyMemoryRoot(t), result.OldRoot)
	require.Equal(t, []int{0}, result.AcceptedIndexes)
	require.Empty(t, result.DuplicateIndexes)
	require.Empty(t, result.Rejected)
	require.Equal(t, HashLeaf(key, value), result.CandidateRoot)
	require.Equal(t, memoryRootAfterLeaves(t, leafInput{key: key, value: value}), result.CandidateRoot)
	require.Equal(t, result.CandidateRoot, tree.RootHash())
	require.Zero(t, result.Stats.MaterializedNodes)
	require.Zero(t, result.Stats.MaterializedBytes)
	require.Zero(t, result.Stats.BranchesVisited)
	require.Zero(t, result.Stats.BranchBytesVisited)
	require.Positive(t, result.Stats.OverlayEntries)
	require.Positive(t, result.Stats.OverlayBytes)
}

func TestTreeApplyEmptyBatchIsNoOp(t *testing.T) {
	tree := NewTree()

	result, err := tree.ApplyLeaves(nil)
	require.NoError(t, err)

	require.Equal(t, emptyMemoryRoot(t), result.OldRoot)
	require.Equal(t, emptyMemoryRoot(t), result.CandidateRoot)
	require.Empty(t, result.AcceptedIndexes)
	require.Empty(t, result.DuplicateIndexes)
	require.Empty(t, result.Rejected)
	require.Zero(t, result.Stats.MaterializedNodes)
	require.Zero(t, result.Stats.MaterializedBytes)
	require.Zero(t, result.Stats.BranchesVisited)
	require.Zero(t, result.Stats.BranchBytesVisited)
	require.Zero(t, result.Stats.OverlayEntries)
	require.Zero(t, result.Stats.OverlayBytes)
	require.Equal(t, emptyMemoryRoot(t), tree.RootHash())
}

func TestTreeApplyLeavesReturnsErrorOnInternalBatchInsertFailure(t *testing.T) {
	key := keyWithFirstByte(0x01)
	value := []byte("value-one")
	stubRoot := HashLeaf(key, []byte("existing"))
	tree := NewTreeWithRoot(NewStub(stubRoot))

	result, err := tree.ApplyLeaves([]LeafInput{treeInput(key, value)})

	require.True(t, errors.Is(err, ErrStubOnPath), "expected ErrStubOnPath, got %v", err)
	require.Empty(t, result.AcceptedIndexes)
	require.Empty(t, result.DuplicateIndexes)
	require.Empty(t, result.Rejected)
	require.Equal(t, stubRoot, tree.RootHash())
}

func TestTreeApplySecondLeafSplitsSingleLeaf(t *testing.T) {
	tree := NewTree()
	k1 := keyWithFirstByte(0x01)
	k2 := keyWithFirstByte(0x03)
	v1 := []byte("value-one")
	v2 := []byte("value-two")

	first, err := tree.ApplyLeaves([]LeafInput{treeInput(k1, v1)})
	require.NoError(t, err)
	require.Equal(t, memoryRootAfterLeaves(t, leafInput{key: k1, value: v1}), first.CandidateRoot)

	second, err := tree.ApplyLeaves([]LeafInput{treeInput(k2, v2)})
	require.NoError(t, err)

	require.Equal(t, first.CandidateRoot, second.OldRoot)
	require.Equal(t, []int{0}, second.AcceptedIndexes)
	require.Empty(t, second.DuplicateIndexes)
	require.Empty(t, second.Rejected)
	require.Equal(t, memoryRootAfterLeaves(t,
		leafInput{key: k1, value: v1},
		leafInput{key: k2, value: v2},
	), second.CandidateRoot)
	require.Equal(t, second.CandidateRoot, tree.RootHash())
	require.Zero(t, second.Stats.MaterializedNodes)
	require.Zero(t, second.Stats.MaterializedBytes)
	require.Positive(t, second.Stats.BranchesVisited)
	require.Positive(t, second.Stats.BranchBytesVisited)
	require.GreaterOrEqual(t, second.Stats.OverlayEntries, 2)
	require.NotNil(t, tree.Root().Internal)
	require.Equal(t, uint8(1), tree.Root().Internal.Depth)
	require.Equal(t, 1, tree.Root().Internal.Path.Len())
}

func TestTreeApplyThirdLeafSplitsInternalCompressedPath(t *testing.T) {
	tree := NewTree()
	k1 := keyWithFirstByte(0x07) // bits 1110...
	k2 := keyWithFirstByte(0x0f) // bits 1111...
	k3 := keyWithFirstByte(0x01) // bits 1000..., splits the root path at bit 1
	v1 := []byte("value-one")
	v2 := []byte("value-two")
	v3 := []byte("value-three")

	firstTwo, err := tree.ApplyLeaves([]LeafInput{
		treeInput(k1, v1),
		treeInput(k2, v2),
	})
	require.NoError(t, err)
	require.Equal(t, memoryRootAfterLeaves(t,
		leafInput{key: k1, value: v1},
		leafInput{key: k2, value: v2},
	), firstTwo.CandidateRoot)
	require.NotNil(t, tree.Root().Internal)
	require.Equal(t, uint8(3), tree.Root().Internal.Depth)
	require.Equal(t, 3, tree.Root().Internal.Path.Len())

	third, err := tree.ApplyLeaves([]LeafInput{treeInput(k3, v3)})
	require.NoError(t, err)

	require.Equal(t, firstTwo.CandidateRoot, third.OldRoot)
	require.Equal(t, []int{0}, third.AcceptedIndexes)
	require.Empty(t, third.DuplicateIndexes)
	require.Empty(t, third.Rejected)
	require.Equal(t, memoryRootAfterLeaves(t,
		leafInput{key: k1, value: v1},
		leafInput{key: k2, value: v2},
		leafInput{key: k3, value: v3},
	), third.CandidateRoot)
	require.Equal(t, third.CandidateRoot, tree.RootHash())
	require.NotNil(t, tree.Root().Internal)
	require.Equal(t, uint8(1), tree.Root().Internal.Depth)
	require.Equal(t, 1, tree.Root().Internal.Path.Len())
}

func TestTreeApplyDuplicateCommittedLeafIsNoOp(t *testing.T) {
	tree := NewTree()
	key := keyWithFirstByte(0x01)
	value := []byte("value-one")

	first, err := tree.ApplyLeaves([]LeafInput{treeInput(key, value)})
	require.NoError(t, err)

	duplicate, err := tree.ApplyLeaves([]LeafInput{treeInput(key, value)})
	require.NoError(t, err)

	require.Equal(t, first.CandidateRoot, duplicate.OldRoot)
	require.Equal(t, first.CandidateRoot, duplicate.CandidateRoot)
	require.Empty(t, duplicate.AcceptedIndexes)
	require.Equal(t, []int{0}, duplicate.DuplicateIndexes)
	require.Empty(t, duplicate.Rejected)
	require.Zero(t, duplicate.Stats.MaterializedNodes)
	require.Zero(t, duplicate.Stats.MaterializedBytes)
	require.Positive(t, duplicate.Stats.BranchesVisited)
	require.Positive(t, duplicate.Stats.BranchBytesVisited)
	require.Zero(t, duplicate.Stats.OverlayEntries)
	require.Equal(t, first.CandidateRoot, tree.RootHash())
}

func TestTreeApplySameKeyDifferentValueIsRejected(t *testing.T) {
	tree := NewTree()
	key := keyWithFirstByte(0x01)
	value := []byte("value-one")

	first, err := tree.ApplyLeaves([]LeafInput{treeInput(key, value)})
	require.NoError(t, err)

	modification, err := tree.ApplyLeaves([]LeafInput{treeInput(key, []byte("value-two"))})
	require.NoError(t, err)

	require.Equal(t, first.CandidateRoot, modification.OldRoot)
	require.Equal(t, first.CandidateRoot, modification.CandidateRoot)
	require.Empty(t, modification.AcceptedIndexes)
	require.Empty(t, modification.DuplicateIndexes)
	require.Len(t, modification.Rejected, 1)
	require.Equal(t, 0, modification.Rejected[0].Index)
	require.Equal(t, RejectLeafModification, modification.Rejected[0].Reason)
	require.ErrorIs(t, modification.Rejected[0].Err, ErrLeafModification)
	require.Equal(t, first.CandidateRoot, tree.RootHash())
}

func TestTreeApplyDuplicateInSameBatchIsNoOp(t *testing.T) {
	tree := NewTree()
	key := keyWithFirstByte(0x01)
	value := []byte("value-one")

	result, err := tree.ApplyLeaves([]LeafInput{
		treeInput(key, value),
		treeInput(key, value),
	})
	require.NoError(t, err)

	require.Equal(t, []int{0}, result.AcceptedIndexes)
	require.Equal(t, []int{1}, result.DuplicateIndexes)
	require.Empty(t, result.Rejected)
	require.Equal(t, memoryRootAfterLeaves(t, leafInput{key: key, value: value}), result.CandidateRoot)
	require.Equal(t, result.CandidateRoot, tree.RootHash())
}

func TestTreeApplySameKeyDifferentValueInBatchUsesFirstOccurrence(t *testing.T) {
	tree := NewTree()
	key := keyWithFirstByte(0x01)
	firstValue := []byte("value-one")
	secondValue := []byte("value-two")

	result, err := tree.ApplyLeaves([]LeafInput{
		treeInput(key, firstValue),
		treeInput(key, secondValue),
	})
	require.NoError(t, err)

	require.Equal(t, []int{0}, result.AcceptedIndexes)
	require.Equal(t, []int{1}, result.DuplicateIndexes)
	require.Empty(t, result.Rejected)
	require.Equal(t, memoryRootAfterLeaves(t, leafInput{key: key, value: firstValue}), result.CandidateRoot)
	require.Equal(t, result.CandidateRoot, tree.RootHash())
}

func TestTreeApplyRejectsInvalidKey(t *testing.T) {
	tree := NewTree()

	result, err := tree.ApplyLeaves([]LeafInput{{Key: []byte{0x01}, Value: []byte("value")}})
	require.NoError(t, err)

	require.Equal(t, emptyMemoryRoot(t), result.OldRoot)
	require.Equal(t, emptyMemoryRoot(t), result.CandidateRoot)
	require.Empty(t, result.AcceptedIndexes)
	require.Empty(t, result.DuplicateIndexes)
	require.Len(t, result.Rejected, 1)
	require.Equal(t, RejectInvalidKey, result.Rejected[0].Reason)
	require.Equal(t, emptyMemoryRoot(t), tree.RootHash())
}

func TestTreeApplyRandomizedParityWithMemory(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	tree := NewTree()
	memoryLeaves := make([]leafInput, 0, 400)
	seen := make(map[Key]struct{})

	for batch := 0; batch < 20; batch++ {
		inputs := make([]LeafInput, 0, 20)
		for i := 0; i < 20; i++ {
			key := randomUniqueKey(t, rng, seen)
			value := make([]byte, 8)
			binary.LittleEndian.PutUint64(value, uint64(batch*20+i))

			inputs = append(inputs, treeInput(key, value))
			memoryLeaves = append(memoryLeaves, leafInput{key: key, value: append([]byte(nil), value...)})
		}

		result, err := tree.ApplyLeaves(inputs)
		require.NoError(t, err)
		require.Len(t, result.AcceptedIndexes, len(inputs))
		require.Empty(t, result.DuplicateIndexes)
		require.Empty(t, result.Rejected)
		require.Equal(t, memoryRootAfterLeaves(t, memoryLeaves...), result.CandidateRoot)
		require.Equal(t, result.CandidateRoot, tree.RootHash())
	}
}

func keyWithFirstByte(value byte) Key {
	var key Key
	key[0] = value
	return key
}

func treeInput(key Key, value []byte) LeafInput {
	return LeafInput{
		Key:   append([]byte(nil), key[:]...),
		Value: append([]byte(nil), value...),
	}
}

func randomUniqueKey(t *testing.T, rng *rand.Rand, seen map[Key]struct{}) Key {
	t.Helper()

	for {
		var key Key
		_, err := rng.Read(key[:])
		require.NoError(t, err)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		return key
	}
}
