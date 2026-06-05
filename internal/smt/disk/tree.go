package disk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"
)

var (
	ErrLeafModification = errors.New("disk smt: attempt to modify an existing leaf")
	ErrStubOnPath       = errors.New("disk smt: stub on insertion path")
)

type LeafInput struct {
	Key   []byte
	Value []byte
}

type ApplyResult struct {
	OldRoot       Hash
	CandidateRoot Hash

	AcceptedIndexes  []int
	DuplicateIndexes []int
	Rejected         []RejectedLeaf

	Stats ApplyStats
}

type RejectedLeaf struct {
	Index  int
	Reason RejectReason
	Err    error
}

type RejectReason string

const (
	RejectDuplicateCommitted RejectReason = "duplicate_committed"
	RejectLeafModification   RejectReason = "leaf_modification"
	RejectInvalidKey         RejectReason = "invalid_key"
	RejectInternal           RejectReason = "internal"
)

type ApplyStats struct {
	// Materialized* counts nodes decoded from storage. The pure in-memory tree
	// does not populate these; persisted materialization will.
	MaterializedNodes         int
	NodeReads                 int
	MaterializedBytes         int64
	MaterializeDuration       time.Duration
	MaterializeSortDuration   time.Duration
	MaterializeReadDuration   time.Duration
	MaterializeDecodeDuration time.Duration
	MaterializeHashDuration   time.Duration
	MaterializeRouteDuration  time.Duration
	MaterializeCacheHits      int
	MaterializeCacheBytes     int64
	MaterializedLeafNodes     int
	MaterializedInternalNodes int
	MaterializedDepthLE8      int
	MaterializedDepthLE12     int
	MaterializedDepthLE16     int
	MaterializedDepthLE18     int
	MaterializedDepthLE20     int
	MaterializedDepthLE22     int
	MaterializedDepthLE24     int
	MaterializedDepthGT24     int
	MaterializeParallelForks  int
	// Branch*Visited counts logical tree traversal work in the in-memory layer.
	BranchesVisited    int
	BranchBytesVisited int64
	ApplyDuration      time.Duration
	OverlayEntries     int
	OverlayBytes       int64
}

// Tree is the algorithm-only path-compressed SMT used by disk snapshots after
// materialization. It is not goroutine-safe; the shared disk backend owns
// synchronization and snapshots are single-consumer.
type Tree struct {
	root *Branch
}

func NewTree() *Tree {
	return &Tree{}
}

func NewTreeWithRoot(root *Branch) *Tree {
	return &Tree{root: root}
}

func (t *Tree) Root() *Branch {
	if t == nil {
		return nil
	}
	return t.root
}

func (t *Tree) RootHash() Hash {
	if t == nil || t.root == nil {
		return EmptyRootHash()
	}
	hash, err := t.root.HashValue()
	if err != nil {
		panic(err)
	}
	return hash
}

func (t *Tree) ApplyLeaves(inputs []LeafInput) (ApplyResult, error) {
	if t == nil {
		return ApplyResult{}, fmt.Errorf("disk smt: nil tree")
	}

	ctx := newApplyContext()
	result := ApplyResult{
		OldRoot:          t.RootHash(),
		AcceptedIndexes:  make([]int, 0, len(inputs)),
		DuplicateIndexes: make([]int, 0),
		Rejected:         make([]RejectedLeaf, 0),
	}

	seen := make(map[Key]struct{}, len(inputs))
	items := make([]batchItem, 0, len(inputs))
	acceptedIndexes := make([]int, 0, len(inputs))
	for i, input := range inputs {
		key, err := KeyFromBytes(input.Key)
		if err != nil {
			result.Rejected = append(result.Rejected, RejectedLeaf{
				Index:  i,
				Reason: RejectInvalidKey,
				Err:    err,
			})
			continue
		}
		if _, ok := seen[key]; ok {
			result.DuplicateIndexes = append(result.DuplicateIndexes, i)
			continue
		}
		seen[key] = struct{}{}
		value := append([]byte(nil), input.Value...)

		if existing := findLeaf(t.root, key, 0, ctx); existing != nil {
			if bytes.Equal(existing.Value, value) {
				result.DuplicateIndexes = append(result.DuplicateIndexes, i)
				continue
			}
			result.Rejected = append(result.Rejected, RejectedLeaf{
				Index:  i,
				Reason: RejectLeafModification,
				Err:    ErrLeafModification,
			})
			continue
		}

		items = append(items, batchItem{Key: key, Value: value, Index: i})
		acceptedIndexes = append(acceptedIndexes, i)
	}

	if len(items) > 0 {
		sort.Slice(items, func(i, j int) bool {
			return keyPathLess(items[i].Key, items[j].Key)
		})
		workers := runtime.GOMAXPROCS(0)
		if workers < 1 {
			workers = 1
		}
		sem := make(chan struct{}, workers)
		root, err := batchInsert(t.root, items, 0, len(items), 0, ctx, sem)
		if err != nil {
			return ApplyResult{}, err
		}
		t.root = root
		result.AcceptedIndexes = append(result.AcceptedIndexes, acceptedIndexes...)
	}

	result.CandidateRoot = t.RootHash()
	result.Stats = ctx.stats
	return result, nil
}

type batchItem struct {
	Key   Key
	Value []byte
	Index int
}

type applyContext struct {
	visited map[*Branch]struct{}
	stats   ApplyStats
	mu      sync.Mutex
}

func newApplyContext() *applyContext {
	return &applyContext{visited: make(map[*Branch]struct{})}
}

func (c *applyContext) recordVisited(branch *Branch) {
	if c == nil || branch == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.visited[branch]; ok {
		return
	}
	c.visited[branch] = struct{}{}
	c.stats.BranchesVisited++
	c.stats.BranchBytesVisited += estimateBranchBytes(branch)
}

func (c *applyContext) recordOverlay(branch *Branch) {
	if c == nil || branch == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats.OverlayEntries++
	c.stats.OverlayBytes += estimateBranchBytes(branch)
}

func findLeaf(branch *Branch, key Key, startBit int, ctx *applyContext) *LeafNode {
	if branch == nil {
		return nil
	}
	ctx.recordVisited(branch)
	switch branch.Kind {
	case BranchKindLeaf:
		if branch.Leaf != nil && branch.Leaf.Key == key {
			return branch.Leaf
		}
		return nil
	case BranchKindInternal:
		node := branch.Internal
		if node == nil {
			return nil
		}
		firstDiv, err := firstDivergenceInPrefix(node.Path, key, startBit)
		if err != nil || firstDiv < node.Path.Len() {
			return nil
		}
		split := startBit + node.Path.Len()
		if split < 0 || split >= KeyBits {
			return nil
		}
		if KeyBit(key, split) == 1 {
			return findLeaf(node.Right, key, split+1, ctx)
		}
		return findLeaf(node.Left, key, split+1, ctx)
	default:
		return nil
	}
}

type insertStatus uint8

const (
	batchInsertParallelThreshold = 64

	insertAccepted insertStatus = iota
	insertDuplicate
	insertLeafModification
)

func batchInsert(branch *Branch, items []batchItem, start, end, startBit int, ctx *applyContext, sem chan struct{}) (*Branch, error) {
	if start == end {
		return branch, nil
	}
	if branch == nil {
		return buildSubtree(items, start, end, startBit, ctx, sem)
	}
	ctx.recordVisited(branch)

	switch branch.Kind {
	case BranchKindLeaf:
		if branch.Leaf == nil {
			return nil, fmt.Errorf("disk smt: malformed leaf branch")
		}
		mixed := make([]batchItem, 0, end-start+1)
		mixed = append(mixed, items[start:end]...)
		mixed = append(mixed, batchItem{
			Key:   branch.Leaf.Key,
			Value: append([]byte(nil), branch.Leaf.Value...),
			Index: -1,
		})
		sort.Slice(mixed, func(i, j int) bool {
			return keyPathLess(mixed[i].Key, mixed[j].Key)
		})
		return buildSubtree(mixed, 0, len(mixed), startBit, ctx, sem)
	case BranchKindInternal:
		node := branch.Internal
		if node == nil {
			return nil, fmt.Errorf("disk smt: malformed internal branch")
		}
		nPath := node.Path.Len()
		firstDiv := nPath
		startDiv, err := firstDivergenceInPrefix(node.Path, items[start].Key, startBit)
		if err != nil {
			return nil, err
		}
		if startDiv < firstDiv {
			firstDiv = startDiv
		}
		endDiv, err := firstDivergenceInPrefix(node.Path, items[end-1].Key, startBit)
		if err != nil {
			return nil, err
		}
		if endDiv < firstDiv {
			firstDiv = endDiv
		}
		if firstDiv < nPath {
			return batchSplitInternal(node, items, start, end, startBit, firstDiv, ctx, sem)
		}

		split := startBit + nPath
		if split < 0 || split >= KeyBits {
			return nil, fmt.Errorf("disk smt: invalid internal split bit %d", split)
		}
		mid := partitionPoint(items, start, end, split)

		var left, right *Branch
		var leftErr, rightErr error
		if end-start >= batchInsertParallelThreshold && tryAcquire(sem) {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer release(sem)
				left, leftErr = batchInsert(node.Left, items, start, mid, split+1, ctx, sem)
			}()
			right, rightErr = batchInsert(node.Right, items, mid, end, split+1, ctx, sem)
			wg.Wait()
		} else {
			left, leftErr = batchInsert(node.Left, items, start, mid, split+1, ctx, sem)
			right, rightErr = batchInsert(node.Right, items, mid, end, split+1, ctx, sem)
		}
		if leftErr != nil {
			return nil, leftErr
		}
		if rightErr != nil {
			return nil, rightErr
		}
		next, err := NewInternal(node.Path, node.Depth, left, right)
		if err != nil {
			return nil, err
		}
		ctx.recordOverlay(next)
		return next, nil
	case BranchKindStub:
		return nil, ErrStubOnPath
	default:
		return nil, fmt.Errorf("disk smt: unknown branch kind %d", branch.Kind)
	}
}

func buildSubtree(items []batchItem, start, end, startBit int, ctx *applyContext, sem chan struct{}) (*Branch, error) {
	if end <= start {
		return nil, fmt.Errorf("disk smt: empty batch subtree")
	}
	if end-start == 1 {
		leaf := NewLeaf(items[start].Key, items[start].Value)
		ctx.recordOverlay(leaf)
		return leaf, nil
	}

	xor := xorKeys(items[start].Key, items[end-1].Key)
	split := firstSetBitFrom(xor, startBit)
	if split >= KeyBits {
		return nil, ErrLeafModification
	}
	mid := partitionPoint(items, start, end, split)
	if mid <= start || mid >= end {
		return nil, fmt.Errorf("disk smt: invalid batch partition split=%d start=%d mid=%d end=%d", split, start, mid, end)
	}
	path, err := NewCompressedPathFromKeyRange(items[start].Key, startBit, split-startBit)
	if err != nil {
		return nil, err
	}

	var left, right *Branch
	var leftErr, rightErr error
	if end-start >= batchInsertParallelThreshold && tryAcquire(sem) {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer release(sem)
			left, leftErr = buildSubtree(items, start, mid, split+1, ctx, sem)
		}()
		right, rightErr = buildSubtree(items, mid, end, split+1, ctx, sem)
		wg.Wait()
	} else {
		left, leftErr = buildSubtree(items, start, mid, split+1, ctx, sem)
		right, rightErr = buildSubtree(items, mid, end, split+1, ctx, sem)
	}
	if leftErr != nil {
		return nil, leftErr
	}
	if rightErr != nil {
		return nil, rightErr
	}
	node, err := NewInternal(path, uint8(split), left, right)
	if err != nil {
		return nil, err
	}
	ctx.recordOverlay(node)
	return node, nil
}

func batchSplitInternal(node *InternalNode, items []batchItem, start, end, startBit, firstDiv int, ctx *applyContext, sem chan struct{}) (*Branch, error) {
	newSplit := startBit + firstDiv
	if newSplit < 0 || newSplit >= KeyBits {
		return nil, fmt.Errorf("disk smt: invalid split bit %d", newSplit)
	}
	oldDir := node.Path.BitAt(firstDiv)
	remaining := node.Path.Len() - firstDiv - 1

	oldPath, err := NewCompressedPathFromPathRange(node.Path, firstDiv+1, remaining)
	if err != nil {
		return nil, err
	}
	oldNode, err := NewInternal(oldPath, node.Depth, node.Left, node.Right)
	if err != nil {
		return nil, err
	}
	ctx.recordOverlay(oldNode)

	newPath, err := NewCompressedPathFromKeyRange(items[start].Key, startBit, firstDiv)
	if err != nil {
		return nil, err
	}
	mid := partitionPoint(items, start, end, newSplit)

	var left, right *Branch
	var leftErr, rightErr error
	if end-start >= batchInsertParallelThreshold && tryAcquire(sem) {
		var wg sync.WaitGroup
		wg.Add(1)
		if oldDir == 0 {
			go func() {
				defer wg.Done()
				defer release(sem)
				left, leftErr = batchInsert(oldNode, items, start, mid, newSplit+1, ctx, sem)
			}()
			right, rightErr = batchInsert(nil, items, mid, end, newSplit+1, ctx, sem)
		} else {
			go func() {
				defer wg.Done()
				defer release(sem)
				left, leftErr = batchInsert(nil, items, start, mid, newSplit+1, ctx, sem)
			}()
			right, rightErr = batchInsert(oldNode, items, mid, end, newSplit+1, ctx, sem)
		}
		wg.Wait()
	} else if oldDir == 0 {
		left, leftErr = batchInsert(oldNode, items, start, mid, newSplit+1, ctx, sem)
		right, rightErr = batchInsert(nil, items, mid, end, newSplit+1, ctx, sem)
	} else {
		left, leftErr = batchInsert(nil, items, start, mid, newSplit+1, ctx, sem)
		right, rightErr = batchInsert(oldNode, items, mid, end, newSplit+1, ctx, sem)
	}
	if leftErr != nil {
		return nil, leftErr
	}
	if rightErr != nil {
		return nil, rightErr
	}
	next, err := NewInternal(newPath, uint8(newSplit), left, right)
	if err != nil {
		return nil, err
	}
	ctx.recordOverlay(next)
	return next, nil
}

func xorKeys(a, b Key) Key {
	var out Key
	for i := range out {
		out[i] = a[i] ^ b[i]
	}
	return out
}

func firstSetBitFrom(key Key, startBit int) int {
	byteIdx := startBit / 8
	bitOff := startBit % 8
	if byteIdx < KeySize {
		masked := key[byteIdx] >> bitOff
		if masked != 0 {
			return startBit + bitsTrailingZeros8(masked)
		}
	}
	for i := byteIdx + 1; i < KeySize; i++ {
		if key[i] != 0 {
			return i*8 + bitsTrailingZeros8(key[i])
		}
	}
	return KeyBits
}

func bitsTrailingZeros8(value byte) int {
	for i := 0; i < 8; i++ {
		if value&(1<<uint(i)) != 0 {
			return i
		}
	}
	return 8
}

func partitionPoint(items []batchItem, start, end, split int) int {
	lo, hi := start, end
	for lo < hi {
		mid := (lo + hi) / 2
		if KeyBit(items[mid].Key, split) == 1 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func keyPathLess(left, right Key) bool {
	for depth := 0; depth < KeyBits; depth++ {
		leftBit := KeyBit(left, depth)
		rightBit := KeyBit(right, depth)
		if leftBit != rightBit {
			return leftBit < rightBit
		}
	}
	return false
}

func tryAcquire(sem chan struct{}) bool {
	select {
	case sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func release(sem chan struct{}) {
	<-sem
}

func insertBranch(branch *Branch, key Key, value []byte, startBit int, ctx *applyContext) (*Branch, insertStatus, error) {
	if branch == nil {
		leaf := NewLeaf(key, value)
		ctx.recordOverlay(leaf)
		return leaf, insertAccepted, nil
	}
	ctx.recordVisited(branch)

	switch branch.Kind {
	case BranchKindLeaf:
		if branch.Leaf == nil {
			return nil, insertAccepted, fmt.Errorf("disk smt: malformed leaf branch")
		}
		div := firstDivergingBit(branch.Leaf.Key, key, startBit)
		if div >= KeyBits {
			if bytes.Equal(branch.Leaf.Value, value) {
				return branch, insertDuplicate, nil
			}
			return branch, insertLeafModification, nil
		}
		path, err := NewCompressedPathFromKeyRange(key, startBit, div-startBit)
		if err != nil {
			return nil, insertAccepted, err
		}
		oldLeaf := branch
		newLeaf := NewLeaf(key, value)
		ctx.recordOverlay(newLeaf)

		var left, right *Branch
		if KeyBit(key, div) == 1 {
			left, right = oldLeaf, newLeaf
		} else {
			left, right = newLeaf, oldLeaf
		}
		node, err := NewInternal(path, uint8(div), left, right)
		if err != nil {
			return nil, insertAccepted, err
		}
		ctx.recordOverlay(node)
		return node, insertAccepted, nil
	case BranchKindInternal:
		node := branch.Internal
		if node == nil {
			return nil, insertAccepted, fmt.Errorf("disk smt: malformed internal branch")
		}
		firstDiv, err := firstDivergenceInPrefix(node.Path, key, startBit)
		if err != nil {
			return nil, insertAccepted, err
		}
		if firstDiv < node.Path.Len() {
			next, err := splitInternalNode(node, key, value, startBit, firstDiv, ctx)
			if err != nil {
				return nil, insertAccepted, err
			}
			return next, insertAccepted, nil
		}

		split := startBit + node.Path.Len()
		if split < 0 || split >= KeyBits {
			return nil, insertAccepted, fmt.Errorf("disk smt: invalid internal split bit %d", split)
		}

		var left, right *Branch
		if KeyBit(key, split) == 1 {
			nextRight, status, err := insertBranch(node.Right, key, value, split+1, ctx)
			if err != nil {
				return nil, insertAccepted, err
			}
			if status != insertAccepted {
				return branch, status, nil
			}
			left, right = node.Left, nextRight
		} else {
			nextLeft, status, err := insertBranch(node.Left, key, value, split+1, ctx)
			if err != nil {
				return nil, insertAccepted, err
			}
			if status != insertAccepted {
				return branch, status, nil
			}
			left, right = nextLeft, node.Right
		}

		next, err := NewInternal(node.Path, node.Depth, left, right)
		if err != nil {
			return nil, insertAccepted, err
		}
		ctx.recordOverlay(next)
		return next, insertAccepted, nil
	case BranchKindStub:
		return nil, insertAccepted, ErrStubOnPath
	default:
		return nil, insertAccepted, fmt.Errorf("disk smt: unknown branch kind %d", branch.Kind)
	}
}

func splitInternalNode(node *InternalNode, key Key, value []byte, startBit, firstDiv int, ctx *applyContext) (*Branch, error) {
	newSplit := startBit + firstDiv
	if newSplit < 0 || newSplit >= KeyBits {
		return nil, fmt.Errorf("disk smt: invalid split bit %d", newSplit)
	}
	oldDir := node.Path.BitAt(firstDiv)
	remaining := node.Path.Len() - firstDiv - 1

	oldPath, err := NewCompressedPathFromPathRange(node.Path, firstDiv+1, remaining)
	if err != nil {
		return nil, err
	}
	oldNode, err := NewInternal(oldPath, node.Depth, node.Left, node.Right)
	if err != nil {
		return nil, err
	}
	ctx.recordOverlay(oldNode)

	newLeaf := NewLeaf(key, value)
	ctx.recordOverlay(newLeaf)

	newPath, err := NewCompressedPathFromKeyRange(key, startBit, firstDiv)
	if err != nil {
		return nil, err
	}

	var left, right *Branch
	if oldDir == 1 {
		left, right = newLeaf, oldNode
	} else {
		left, right = oldNode, newLeaf
	}
	next, err := NewInternal(newPath, uint8(newSplit), left, right)
	if err != nil {
		return nil, err
	}
	ctx.recordOverlay(next)
	return next, nil
}

func firstDivergingBit(a, b Key, startBit int) int {
	for pos := startBit; pos < KeyBits; pos++ {
		if KeyBit(a, pos) != KeyBit(b, pos) {
			return pos
		}
	}
	return KeyBits
}

func firstDivergenceInPrefix(path CompressedPath, key Key, startBit int) (int, error) {
	if startBit < 0 || startBit+path.Len() > KeyBits {
		return 0, fmt.Errorf("disk smt: invalid prefix match start=%d len=%d", startBit, path.Len())
	}
	for i := 0; i < path.Len(); i++ {
		if path.BitAt(i) != KeyBit(key, startBit+i) {
			return i, nil
		}
	}
	return path.Len(), nil
}

func estimateBranchBytes(branch *Branch) int64 {
	if branch == nil {
		return 0
	}
	switch branch.Kind {
	case BranchKindLeaf:
		if branch.Leaf == nil {
			return 0
		}
		return int64(1 + KeySize + uvarintLen(uint64(len(branch.Leaf.Value))) + len(branch.Leaf.Value) + HashSize)
	case BranchKindInternal:
		if branch.Internal == nil {
			return 0
		}
		return int64(1 + 1 + 1 + 1 + prefixByteLen(branch.Internal.Path.Len()) + HashSize)
	case BranchKindStub:
		return HashSize
	default:
		return 0
	}
}

func uvarintLen(value uint64) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], value)
}
