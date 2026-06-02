package persist

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type Tree struct {
	store storage.Store

	mu       sync.RWMutex
	root     *disk.Branch
	rootHash disk.Hash

	materializeMode    string
	frontierReadMode   string
	materializeWorkers int
	nodeCacheDepth     int
	nodeCache          map[disk.NodeKey]cachedNode
	nodeCacheBytes     int64
	nodeCacheMu        sync.RWMutex
}

const (
	MaterializePath     = "path"
	MaterializeFrontier = "frontier"
	// MaterializeParallel uses the same frontier traversal as "frontier", but
	// loads each wave with bounded concurrent point reads. This mirrors the
	// rugregator baseline more closely than storage-engine iterator/MultiGet reads.
	MaterializeParallel = "parallel"

	FrontierReadPoint    = "point"
	FrontierReadIterator = "iterator"
)

type Options struct {
	// EnableNodeCache enables the decoded-node cache. Keep this disabled for
	// production unless a dedicated benchmark proves it helps the live workload.
	EnableNodeCache bool
	// NodeCacheDepth caches persisted nodes at or above this absolute tree
	// depth when EnableNodeCache is true. This is a bounded diagnostic/
	// performance knob, not required for correctness.
	NodeCacheDepth int
	// MaterializeMode selects how a batch's touched paths are loaded.
	// "path" is the original per-key path walk; "frontier" loads the batch as
	// wavefronts of NodeKeys.
	MaterializeMode string
	// FrontierReadMode selects how each frontier wave is read from the disk store.
	// "point" uses sorted point gets; "iterator" uses one storage iterator per
	// wave and SeekGE for each requested key.
	FrontierReadMode string
	// MaterializeWorkers bounds concurrent branch materialization in
	// MaterializeParallel mode. Zero uses GOMAXPROCS.
	MaterializeWorkers int
}

type NodeCacheStats struct {
	Enabled bool
	Depth   int
	Entries int
	Bytes   int64
}

type CommitStats struct {
	CollectDuration     time.Duration
	TombstoneDuration   time.Duration
	BatchBuildDuration  time.Duration
	RootHashDuration    time.Duration
	EngineWriteDuration time.Duration
	CacheUpdateDuration time.Duration
	NodeWrites          int
	NodeDeletes         int
}

type overlayEntry struct {
	value  []byte
	delete bool
}

type nodeOverlay map[disk.NodeKey]overlayEntry

type cachedNode struct {
	branch       *disk.Branch
	hash         disk.Hash
	encodedBytes int64
}

func DefaultOptions() Options {
	return Options{
		MaterializeMode:  MaterializeParallel,
		FrontierReadMode: FrontierReadPoint,
		NodeCacheDepth:   -1,
	}
}

func Open(store storage.Store) (*Tree, error) {
	return OpenWithOptions(store, DefaultOptions())
}

func OpenWithOptions(store storage.Store, opts Options) (*Tree, error) {
	if store == nil {
		return nil, fmt.Errorf("disk SMT persist: nil store")
	}
	if !opts.EnableNodeCache {
		opts.NodeCacheDepth = -1
	}
	if opts.NodeCacheDepth < -1 || opts.NodeCacheDepth > disk.KeyBits {
		return nil, fmt.Errorf("disk SMT persist: invalid node cache depth %d", opts.NodeCacheDepth)
	}
	if opts.MaterializeWorkers < 0 {
		return nil, fmt.Errorf("disk SMT persist: invalid materialize workers %d", opts.MaterializeWorkers)
	}
	if opts.MaterializeMode == "" {
		opts.MaterializeMode = MaterializeParallel
	}
	if opts.FrontierReadMode == "" {
		opts.FrontierReadMode = FrontierReadPoint
	}
	switch opts.MaterializeMode {
	case MaterializePath, MaterializeFrontier, MaterializeParallel:
	default:
		return nil, fmt.Errorf("disk SMT persist: invalid materialize mode %q", opts.MaterializeMode)
	}
	switch opts.FrontierReadMode {
	case FrontierReadPoint, FrontierReadIterator:
	default:
		return nil, fmt.Errorf("disk SMT persist: invalid frontier read mode %q", opts.FrontierReadMode)
	}
	state, err := store.CommittedState()
	if err != nil {
		return nil, err
	}
	tree := &Tree{
		store:              store,
		root:               stubForRoot(state.RootHash),
		rootHash:           state.RootHash,
		materializeMode:    opts.MaterializeMode,
		frontierReadMode:   opts.FrontierReadMode,
		materializeWorkers: opts.MaterializeWorkers,
		nodeCacheDepth:     opts.NodeCacheDepth,
	}
	if opts.NodeCacheDepth >= 0 {
		tree.nodeCache = make(map[disk.NodeKey]cachedNode)
	}
	return tree, nil
}

func (t *Tree) RootHash() disk.Hash {
	if t == nil {
		return disk.EmptyRootHash()
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.rootHash
}

func (t *Tree) NodeCacheStats() NodeCacheStats {
	if t == nil || t.nodeCacheDepth < 0 {
		return NodeCacheStats{Depth: -1}
	}
	t.nodeCacheMu.RLock()
	defer t.nodeCacheMu.RUnlock()
	return NodeCacheStats{
		Enabled: true,
		Depth:   t.nodeCacheDepth,
		Entries: len(t.nodeCache),
		Bytes:   t.nodeCacheBytes,
	}
}

func (t *Tree) CreateSnapshot() (*Snapshot, error) {
	if t == nil || t.store == nil {
		return nil, fmt.Errorf("disk SMT persist: nil tree")
	}
	t.mu.RLock()
	defer t.mu.RUnlock()

	return &Snapshot{
		parent:     t,
		tree:       disk.NewTreeWithRoot(stubForRoot(t.rootHash)),
		baseRoot:   t.rootHash,
		rootHash:   t.rootHash,
		ownOverlay: make(nodeOverlay),
		loadedKeys: make(map[disk.NodeKey]struct{}),
	}, nil
}

func (t *Tree) GetInclusionCert(key []byte) (*api.InclusionCert, error) {
	if t == nil {
		return nil, fmt.Errorf("disk SMT persist: nil tree")
	}
	rootHash := t.RootHash()
	return BuildInclusionCert(rootHash, t.store, key)
}

func (t *Tree) GetInclusionCertWithReadSnapshot(key []byte, openReadSnapshot func() (storage.ReadStore, func(), error)) (*api.InclusionCert, error) {
	if len(key) != api.StateTreeKeyLengthBytes {
		return nil, fmt.Errorf("%w: got %d, want %d", api.ErrCertKeyLength, len(key), api.StateTreeKeyLengthBytes)
	}
	if t == nil {
		return nil, fmt.Errorf("disk SMT persist: nil tree")
	}
	t.mu.RLock()
	rootHash := t.rootHash
	if rootHash == disk.EmptyRootHash() || openReadSnapshot == nil {
		t.mu.RUnlock()
		// The fallback path is not isolated from concurrent commits. It is
		// intended for empty roots and non-production stores that do not expose
		// RocksDB read snapshots.
		return BuildInclusionCert(rootHash, t.store, key)
	}
	reader, closeSnapshot, err := openReadSnapshot()
	t.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if closeSnapshot != nil {
		defer closeSnapshot()
	}
	return BuildInclusionCert(rootHash, reader, key)
}

func (t *Tree) GetInclusionCertsWithReadSnapshot(keys [][]byte, openReadSnapshot func() (storage.ReadStore, func(), error)) ([]*api.InclusionCert, error) {
	for i, key := range keys {
		if len(key) != api.StateTreeKeyLengthBytes {
			return nil, fmt.Errorf("key %d: %w: got %d, want %d", i, api.ErrCertKeyLength, len(key), api.StateTreeKeyLengthBytes)
		}
	}
	if t == nil {
		return nil, fmt.Errorf("disk SMT persist: nil tree")
	}
	t.mu.RLock()
	rootHash := t.rootHash
	if rootHash == disk.EmptyRootHash() || openReadSnapshot == nil {
		t.mu.RUnlock()
		return buildInclusionCerts(rootHash, t.store, keys)
	}
	reader, closeSnapshot, err := openReadSnapshot()
	t.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if closeSnapshot != nil {
		defer closeSnapshot()
	}
	return buildInclusionCerts(rootHash, reader, keys)
}

func buildInclusionCerts(rootHash disk.Hash, reader storage.ReadStore, keys [][]byte) ([]*api.InclusionCert, error) {
	return BuildInclusionCerts(rootHash, reader, keys)
}

// BuildInclusionCerts builds multiple proofs from a fixed root hash and read
// store. It materializes the union of requested proof paths once, then derives
// each certificate from that shared materialized root.
func BuildInclusionCerts(rootHash disk.Hash, reader storage.ReadStore, keys [][]byte) ([]*api.InclusionCert, error) {
	certs := make([]*api.InclusionCert, len(keys))
	if len(keys) == 0 {
		return certs, nil
	}

	diskKeys := make([]disk.Key, len(keys))
	uniqueKeys := make([]disk.Key, 0, len(keys))
	seen := make(map[disk.Key]struct{}, len(keys))
	for i, key := range keys {
		if len(key) != api.StateTreeKeyLengthBytes {
			return nil, fmt.Errorf("key %d: %w: got %d, want %d", i, api.ErrCertKeyLength, len(key), api.StateTreeKeyLengthBytes)
		}
		diskKey, err := disk.KeyFromBytes(key)
		if err != nil {
			return nil, fmt.Errorf("key %d: %w", i, err)
		}
		diskKeys[i] = diskKey
		if _, ok := seen[diskKey]; ok {
			continue
		}
		seen[diskKey] = struct{}{}
		uniqueKeys = append(uniqueKeys, diskKey)
	}
	sort.Slice(uniqueKeys, func(i, j int) bool {
		return keyPathLess(uniqueKeys[i], uniqueKeys[j])
	})

	root := stubForRoot(rootHash)
	if root != nil {
		if reader == nil {
			return nil, fmt.Errorf("disk SMT persist: nil read store")
		}
		if err := materializeProofKeys(&root, reader, uniqueKeys); err != nil {
			return nil, err
		}
	}

	for i, diskKey := range diskKeys {
		var cert api.InclusionCert
		if err := buildInclusionCert(root, diskKey, &cert); err != nil {
			return nil, fmt.Errorf("build inclusion cert %d: %w", i, err)
		}
		certs[i] = &cert
	}
	return certs, nil
}

// BuildInclusionCert builds a proof from a fixed root hash and read store.
// Snapshot-backed callers intentionally bypass Tree's decoded-node cache: cache
// entries describe the latest committed RocksDB view, not the snapshot sequence.
func BuildInclusionCert(rootHash disk.Hash, reader storage.ReadStore, key []byte) (*api.InclusionCert, error) {
	if len(key) != api.StateTreeKeyLengthBytes {
		return nil, fmt.Errorf("%w: got %d, want %d", api.ErrCertKeyLength, len(key), api.StateTreeKeyLengthBytes)
	}
	diskKey, err := disk.KeyFromBytes(key)
	if err != nil {
		return nil, err
	}
	root := stubForRoot(rootHash)
	if root != nil {
		if reader == nil {
			return nil, fmt.Errorf("disk SMT persist: nil read store")
		}
		if err := materializeProofKey(&root, reader, diskKey); err != nil {
			return nil, err
		}
	}
	var cert api.InclusionCert
	if err := buildInclusionCert(root, diskKey, &cert); err != nil {
		return nil, err
	}
	return &cert, nil
}

func materializeProofKey(root **disk.Branch, reader storage.ReadStore, key disk.Key) error {
	if root == nil || *root == nil {
		return nil
	}
	return materializeProofSlot(root, reader, disk.RootNodeKey(), disk.PrefixBits{}, 0, key)
}

func materializeProofKeys(root **disk.Branch, reader storage.ReadStore, keys []disk.Key) error {
	if root == nil || *root == nil || len(keys) == 0 {
		return nil
	}
	reqs := []frontierReq{{
		slot:     root,
		nodeKey:  disk.RootNodeKey(),
		prefix:   disk.PrefixBits{},
		startBit: 0,
		keys:     keys,
	}}
	for len(reqs) > 0 {
		next, err := materializeProofFrontier(reader, reqs)
		if err != nil {
			return err
		}
		reqs = next
	}
	return nil
}

func materializeProofFrontier(reader storage.ReadStore, reqs []frontierReq) ([]frontierReq, error) {
	loads := make([]frontierLoadReq, 0, len(reqs))
	for _, req := range reqs {
		if req.slot == nil || *req.slot == nil {
			continue
		}
		branch := *req.slot
		if branch.Kind != disk.BranchKindStub {
			continue
		}
		loads = append(loads, frontierLoadReq{
			req:  req,
			hash: branch.StubHash,
		})
	}
	if err := loadProofFrontier(reader, loads); err != nil {
		return nil, err
	}

	next := make([]frontierReq, 0, len(reqs)*2)
	for _, req := range reqs {
		if req.slot == nil || *req.slot == nil {
			continue
		}
		var err error
		next, err = appendFrontierChildren(next, req, *req.slot)
		if err != nil {
			return nil, err
		}
	}
	return next, nil
}

func loadProofFrontier(reader storage.ReadStore, loads []frontierLoadReq) error {
	if len(loads) == 0 {
		return nil
	}
	sort.Slice(loads, func(i, j int) bool {
		return nodeKeyLess(loads[i].req.nodeKey, loads[j].req.nodeKey)
	})
	keys := make([]disk.NodeKey, len(loads))
	for i, load := range loads {
		keys[i] = load.req.nodeKey
	}

	results, err := reader.GetNodes(keys, true)
	if err != nil {
		return err
	}
	for i, result := range results {
		load := loads[i]
		if !result.Found {
			return fmt.Errorf("disk SMT persist: node %x missing from store", load.req.nodeKey.Bytes())
		}
		branch, err := decodeAndValidateProofBranch(load.req.nodeKey, result.Value, load.hash)
		if err != nil {
			return err
		}
		*load.req.slot = branch
	}
	return nil
}

func materializeProofSlot(slot **disk.Branch, reader storage.ReadStore, nodeKey disk.NodeKey, prefix disk.PrefixBits, startBit int, key disk.Key) error {
	if slot == nil || *slot == nil {
		return nil
	}
	branch := *slot
	if branch.Kind == disk.BranchKindStub {
		loaded, err := loadProofBranch(reader, nodeKey, branch.StubHash)
		if err != nil {
			return err
		}
		*slot = loaded
		branch = loaded
	}
	if branch.Kind != disk.BranchKindInternal {
		return nil
	}
	node := branch.Internal
	if node == nil {
		return fmt.Errorf("disk SMT persist: malformed internal node")
	}
	firstDiv, err := firstDivergenceInPath(node.Path, key, startBit)
	if err != nil {
		return err
	}
	if firstDiv < node.Path.Len() {
		return nil
	}
	split := startBit + node.Path.Len()
	if split < 0 || split >= disk.KeyBits {
		return fmt.Errorf("disk SMT persist: invalid internal split bit %d", split)
	}
	direction := disk.KeyBit(key, split)
	childKey, childPrefix, childStart, err := childNodeKey(prefix, startBit, node.Path, direction)
	if err != nil {
		return err
	}
	if direction == 1 {
		return materializeProofSlot(&node.Right, reader, childKey, childPrefix, childStart, key)
	}
	return materializeProofSlot(&node.Left, reader, childKey, childPrefix, childStart, key)
}

func loadProofBranch(reader storage.ReadStore, key disk.NodeKey, expectedHash disk.Hash) (*disk.Branch, error) {
	encoded, ok, err := reader.GetNode(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("disk SMT persist: node %x missing from store", key.Bytes())
	}
	branch, err := decodeBranch(encoded)
	if err != nil {
		return nil, err
	}
	hash, err := branch.HashValue()
	if err != nil {
		return nil, err
	}
	if hash != expectedHash {
		return nil, fmt.Errorf("disk SMT persist: node %x hash mismatch: got %x want %x", key.Bytes(), hash, expectedHash)
	}
	return branch, nil
}

func decodeAndValidateProofBranch(key disk.NodeKey, encoded []byte, expectedHash disk.Hash) (*disk.Branch, error) {
	branch, err := decodeBranch(encoded)
	if err != nil {
		return nil, err
	}
	hash, err := branch.HashValue()
	if err != nil {
		return nil, err
	}
	if hash != expectedHash {
		return nil, fmt.Errorf("disk SMT persist: node %x hash mismatch: got %x want %x", key.Bytes(), hash, expectedHash)
	}
	return branch, nil
}

type Snapshot struct {
	parent *Tree
	tree   *disk.Tree

	baseRoot       disk.Hash
	rootHash       disk.Hash
	ownOverlay     nodeOverlay
	parentOverlay  nodeOverlay
	overlayWrites  []storage.NodeWrite
	overlayDeletes []disk.NodeKey
	loadedKeys     map[disk.NodeKey]struct{}
	cacheWrites    map[disk.NodeKey]cachedNode
	cacheDeletes   []disk.NodeKey
	stats          disk.ApplyStats
	statsMu        sync.Mutex
	commitStats    CommitStats
	closed         bool
}

func buildInclusionCert(branch *disk.Branch, key disk.Key, cert *api.InclusionCert) error {
	if branch == nil {
		return fmt.Errorf("disk SMT persist: inclusion cert traversal reached nil subtree")
	}

	switch branch.Kind {
	case disk.BranchKindLeaf:
		if branch.Leaf == nil {
			return fmt.Errorf("disk SMT persist: malformed leaf branch")
		}
		if branch.Leaf.Key != key {
			return fmt.Errorf("disk SMT persist: leaf not found for key %x", key[:])
		}
		return nil
	case disk.BranchKindInternal:
		if branch.Internal == nil {
			return fmt.Errorf("disk SMT persist: malformed internal branch")
		}
		node := branch.Internal
		depth := int(node.Depth)
		if depth < 0 || depth >= api.StateTreeKeyLengthBits {
			return fmt.Errorf("disk SMT persist: invalid branch depth %d", depth)
		}

		var child, sibling *disk.Branch
		if disk.KeyBit(key, depth) == 0 {
			child, sibling = node.Left, node.Right
		} else {
			child, sibling = node.Right, node.Left
		}
		if child == nil || sibling == nil {
			return fmt.Errorf("disk SMT persist: malformed binary internal node at depth %d", depth)
		}

		siblingHash, err := sibling.HashValue()
		if err != nil {
			return err
		}
		cert.Bitmap[depth/8] |= 1 << (uint(depth) % 8)
		var siblingBytes [api.SiblingSize]byte
		copy(siblingBytes[:], siblingHash[:])
		cert.Siblings = append(cert.Siblings, siblingBytes)

		return buildInclusionCert(child, key, cert)
	case disk.BranchKindStub:
		return fmt.Errorf("disk SMT persist: inclusion cert traversal reached unmaterialized stub")
	default:
		return fmt.Errorf("disk SMT persist: unknown branch kind %d", branch.Kind)
	}
}

func (s *Snapshot) RootHash() disk.Hash {
	if s == nil {
		return disk.EmptyRootHash()
	}
	return s.rootHash
}

func (s *Snapshot) LastCommitStats() CommitStats {
	if s == nil {
		return CommitStats{}
	}
	return s.commitStats
}

// Fork creates a child snapshot over this snapshot's overlay. The caller must
// preserve the precollector invariant: at most one uncommitted parent snapshot
// may sit between this child and the committed backend.
func (s *Snapshot) Fork() (*Snapshot, error) {
	if s == nil || s.tree == nil {
		return nil, fmt.Errorf("disk SMT persist: nil snapshot")
	}
	if s.closed {
		return nil, fmt.Errorf("disk SMT persist: snapshot is closed")
	}
	if s.parentOverlay != nil && s.parent.RootHash() != s.baseRoot {
		return nil, fmt.Errorf("disk SMT persist: cannot fork snapshot with uncommitted grandparent overlay")
	}
	if s.rootHash != s.baseRoot && len(s.ownOverlay) == 0 {
		if err := s.rebuildOverlay(); err != nil {
			return nil, err
		}
	}
	return &Snapshot{
		parent:        s.parent,
		tree:          disk.NewTreeWithRoot(stubForRoot(s.rootHash)),
		baseRoot:      s.rootHash,
		rootHash:      s.rootHash,
		ownOverlay:    make(nodeOverlay),
		parentOverlay: cloneOverlay(s.ownOverlay),
		loadedKeys:    make(map[disk.NodeKey]struct{}),
	}, nil
}

func (s *Snapshot) SetCommitTarget(target *Tree) error {
	if s == nil || s.tree == nil {
		return fmt.Errorf("disk SMT persist: nil snapshot")
	}
	if s.closed {
		return fmt.Errorf("disk SMT persist: snapshot is closed")
	}
	if target == nil {
		return fmt.Errorf("disk SMT persist: nil commit target")
	}
	targetRoot := target.RootHash()
	if targetRoot != s.baseRoot {
		return fmt.Errorf("disk SMT persist: commit target root %x does not match snapshot base root %x", targetRoot, s.baseRoot)
	}
	s.parent = target
	s.parentOverlay = nil
	return nil
}

func (s *Snapshot) AddLeaves(inputs []disk.LeafInput) (disk.ApplyResult, error) {
	if s == nil || s.tree == nil {
		return disk.ApplyResult{}, fmt.Errorf("disk SMT persist: nil snapshot")
	}
	if s.closed {
		return disk.ApplyResult{}, fmt.Errorf("disk SMT persist: snapshot is closed")
	}

	materializedBefore := s.stats.MaterializedNodes
	nodeReadsBefore := s.stats.NodeReads
	materializedBytesBefore := s.stats.MaterializedBytes
	materializeSortBefore := s.stats.MaterializeSortDuration
	materializeReadBefore := s.stats.MaterializeReadDuration
	materializeDecodeBefore := s.stats.MaterializeDecodeDuration
	materializeHashBefore := s.stats.MaterializeHashDuration
	materializeRouteBefore := s.stats.MaterializeRouteDuration
	materializeCacheHitsBefore := s.stats.MaterializeCacheHits
	materializeCacheBytesBefore := s.stats.MaterializeCacheBytes
	materializedLeafBefore := s.stats.MaterializedLeafNodes
	materializedInternalBefore := s.stats.MaterializedInternalNodes
	materializedDepthLE8Before := s.stats.MaterializedDepthLE8
	materializedDepthLE12Before := s.stats.MaterializedDepthLE12
	materializedDepthLE16Before := s.stats.MaterializedDepthLE16
	materializedDepthLE18Before := s.stats.MaterializedDepthLE18
	materializedDepthLE20Before := s.stats.MaterializedDepthLE20
	materializedDepthLE22Before := s.stats.MaterializedDepthLE22
	materializedDepthLE24Before := s.stats.MaterializedDepthLE24
	materializedDepthGT24Before := s.stats.MaterializedDepthGT24
	materializeParallelForksBefore := s.stats.MaterializeParallelForks
	materializeStart := time.Now()
	if err := s.materializeInputs(inputs); err != nil {
		return disk.ApplyResult{}, err
	}
	materializeDuration := time.Since(materializeStart)
	applyStart := time.Now()
	result, err := s.tree.ApplyLeaves(inputs)
	applyDuration := time.Since(applyStart)
	if err != nil {
		return result, err
	}
	result.Stats.MaterializedNodes += s.stats.MaterializedNodes - materializedBefore
	result.Stats.NodeReads += s.stats.NodeReads - nodeReadsBefore
	result.Stats.MaterializedBytes += s.stats.MaterializedBytes - materializedBytesBefore
	result.Stats.MaterializeDuration += materializeDuration
	result.Stats.MaterializeSortDuration += s.stats.MaterializeSortDuration - materializeSortBefore
	result.Stats.MaterializeReadDuration += s.stats.MaterializeReadDuration - materializeReadBefore
	result.Stats.MaterializeDecodeDuration += s.stats.MaterializeDecodeDuration - materializeDecodeBefore
	result.Stats.MaterializeHashDuration += s.stats.MaterializeHashDuration - materializeHashBefore
	result.Stats.MaterializeRouteDuration += s.stats.MaterializeRouteDuration - materializeRouteBefore
	result.Stats.MaterializeCacheHits += s.stats.MaterializeCacheHits - materializeCacheHitsBefore
	result.Stats.MaterializeCacheBytes += s.stats.MaterializeCacheBytes - materializeCacheBytesBefore
	result.Stats.MaterializedLeafNodes += s.stats.MaterializedLeafNodes - materializedLeafBefore
	result.Stats.MaterializedInternalNodes += s.stats.MaterializedInternalNodes - materializedInternalBefore
	result.Stats.MaterializedDepthLE8 += s.stats.MaterializedDepthLE8 - materializedDepthLE8Before
	result.Stats.MaterializedDepthLE12 += s.stats.MaterializedDepthLE12 - materializedDepthLE12Before
	result.Stats.MaterializedDepthLE16 += s.stats.MaterializedDepthLE16 - materializedDepthLE16Before
	result.Stats.MaterializedDepthLE18 += s.stats.MaterializedDepthLE18 - materializedDepthLE18Before
	result.Stats.MaterializedDepthLE20 += s.stats.MaterializedDepthLE20 - materializedDepthLE20Before
	result.Stats.MaterializedDepthLE22 += s.stats.MaterializedDepthLE22 - materializedDepthLE22Before
	result.Stats.MaterializedDepthLE24 += s.stats.MaterializedDepthLE24 - materializedDepthLE24Before
	result.Stats.MaterializedDepthGT24 += s.stats.MaterializedDepthGT24 - materializedDepthGT24Before
	result.Stats.MaterializeParallelForks += s.stats.MaterializeParallelForks - materializeParallelForksBefore
	result.Stats.ApplyDuration += applyDuration
	s.rootHash = result.CandidateRoot
	if len(result.AcceptedIndexes) > 0 || len(s.ownOverlay) > 0 {
		if err := s.rebuildOverlay(); err != nil {
			return disk.ApplyResult{}, err
		}
	}
	return result, nil
}

func (s *Snapshot) Commit(blockNumber *api.BigInt) error {
	if s == nil || s.tree == nil || s.parent == nil {
		return fmt.Errorf("disk SMT persist: nil snapshot")
	}
	if s.closed {
		return fmt.Errorf("disk SMT persist: snapshot is closed")
	}

	var stats CommitStats
	stats.CollectDuration = s.commitStats.CollectDuration
	stats.TombstoneDuration = s.commitStats.TombstoneDuration
	defer func() {
		s.commitStats = stats
	}()

	if s.rootHash != s.baseRoot && len(s.ownOverlay) == 0 {
		if err := s.rebuildOverlay(); err != nil {
			return err
		}
	}
	writes := s.overlayWrites
	deletes := s.overlayDeletes
	stats.NodeWrites = len(writes)
	stats.NodeDeletes = len(deletes)

	batchBuildStart := time.Now()
	batch := s.parent.store.NewBatch()
	defer batch.Close()

	if bulk, ok := batch.(storage.BulkEntryBatch); ok {
		if err := bulk.DeleteNodes(deletes); err != nil {
			return err
		}
		if err := bulk.SetNodeEntries(writes); err != nil {
			return err
		}
	} else {
		for _, key := range deletes {
			if err := batch.DeleteNode(key); err != nil {
				return err
			}
		}
		for _, write := range writes {
			if err := batch.SetNode(write.Key, write.Value); err != nil {
				return err
			}
		}
	}
	stats.BatchBuildDuration = time.Since(batchBuildStart)

	if err := batch.SetCommittedState(s.rootHash, blockNumber); err != nil {
		return err
	}

	s.parent.mu.Lock()
	defer s.parent.mu.Unlock()
	if s.parent.rootHash != s.baseRoot {
		return fmt.Errorf("disk SMT persist: commit base root %x does not match current root %x", s.baseRoot, s.parent.rootHash)
	}
	engineWriteStart := time.Now()
	if err := batch.Commit(); err != nil {
		return err
	}
	stats.EngineWriteDuration = time.Since(engineWriteStart)

	cacheUpdateStart := time.Now()
	// Keep committed state bounded in memory; snapshots rematerialize touched
	// paths from the disk store instead of retaining the full committed tree.
	s.parent.root = stubForRoot(s.rootHash)
	s.parent.rootHash = s.rootHash
	s.parent.applyNodeCacheUpdate(s.cacheDeletes, s.cacheWrites)
	stats.CacheUpdateDuration = time.Since(cacheUpdateStart)
	s.closed = true
	return nil
}

func (s *Snapshot) Discard() {
	if s != nil {
		s.closed = true
	}
}

func (s *Snapshot) rebuildOverlay() error {
	if s == nil || s.tree == nil || s.parent == nil {
		return fmt.Errorf("disk SMT persist: nil snapshot")
	}
	collectStart := time.Now()
	writeHint := len(s.loadedKeys) + len(s.loadedKeys)/4 + 1024
	writes := make([]storage.NodeWrite, 0, writeHint)
	writeSet := make(map[disk.NodeKey]struct{}, writeHint)
	cacheWrites := s.parent.newNodeCacheWrites()
	if err := collectWrites(s.tree.Root(), disk.RootNodeKey(), disk.PrefixBits{}, 0, &writes, writeSet, cacheWrites, s.parent.nodeCacheDepth); err != nil {
		return err
	}
	collectDuration := time.Since(collectStart)

	tombstoneStart := time.Now()
	deletes := make([]disk.NodeKey, 0, len(s.loadedKeys))
	for key := range s.loadedKeys {
		if _, written := writeSet[key]; written {
			continue
		}
		deletes = append(deletes, key)
	}
	tombstoneDuration := time.Since(tombstoneStart)

	overlay := make(nodeOverlay, len(writes)+len(deletes))
	for _, key := range deletes {
		overlay[key] = overlayEntry{delete: true}
	}
	for _, write := range writes {
		overlay[write.Key] = overlayEntry{value: write.Value}
	}
	s.ownOverlay = overlay
	s.overlayWrites = writes
	s.overlayDeletes = deletes
	s.cacheWrites = cacheWrites
	s.cacheDeletes = s.parent.prepareNodeCacheDeletes(s.loadedKeys, writeSet)
	s.commitStats.CollectDuration = collectDuration
	s.commitStats.TombstoneDuration = tombstoneDuration
	return nil
}

func cloneOverlay(in nodeOverlay) nodeOverlay {
	if len(in) == 0 {
		return nil
	}
	out := make(nodeOverlay, len(in))
	for key, entry := range in {
		next := overlayEntry{delete: entry.delete}
		if entry.value != nil {
			next.value = append([]byte(nil), entry.value...)
		}
		out[key] = next
	}
	return out
}

func (t *Tree) shouldCacheNode(key disk.NodeKey) bool {
	return t != nil && t.nodeCacheDepth >= 0 && key.DepthBits() <= t.nodeCacheDepth
}

func (t *Tree) getCachedBranch(key disk.NodeKey) (cachedNode, bool) {
	if !t.shouldCacheNode(key) {
		return cachedNode{}, false
	}
	t.nodeCacheMu.RLock()
	defer t.nodeCacheMu.RUnlock()
	entry, ok := t.nodeCache[key]
	return entry, ok
}

func (t *Tree) cacheLoadedBranch(key disk.NodeKey, branch *disk.Branch, encodedBytes int64, hash disk.Hash) {
	if !t.shouldCacheNode(key) || branch == nil {
		return
	}
	t.nodeCacheMu.Lock()
	defer t.nodeCacheMu.Unlock()
	if existing, ok := t.nodeCache[key]; ok {
		t.nodeCacheBytes -= existing.encodedBytes
	}
	t.nodeCache[key] = cachedNode{
		branch:       cloneBranch(branch),
		hash:         hash,
		encodedBytes: encodedBytes,
	}
	t.nodeCacheBytes += encodedBytes
}

func (t *Tree) newNodeCacheWrites() map[disk.NodeKey]cachedNode {
	if t == nil || t.nodeCacheDepth < 0 {
		return nil
	}
	return make(map[disk.NodeKey]cachedNode)
}

func (t *Tree) prepareNodeCacheDeletes(loadedKeys map[disk.NodeKey]struct{}, writeSet map[disk.NodeKey]struct{}) []disk.NodeKey {
	if t == nil || t.nodeCacheDepth < 0 {
		return nil
	}
	deletes := make([]disk.NodeKey, 0)
	for key := range loadedKeys {
		if _, written := writeSet[key]; written || !t.shouldCacheNode(key) {
			continue
		}
		deletes = append(deletes, key)
	}
	return deletes
}

func (t *Tree) applyNodeCacheUpdate(deletes []disk.NodeKey, updates map[disk.NodeKey]cachedNode) {
	if t == nil || t.nodeCacheDepth < 0 {
		return
	}
	t.nodeCacheMu.Lock()
	defer t.nodeCacheMu.Unlock()

	for _, key := range deletes {
		if existing, ok := t.nodeCache[key]; ok {
			t.nodeCacheBytes -= existing.encodedBytes
			delete(t.nodeCache, key)
		}
	}
	for key, entry := range updates {
		if existing, ok := t.nodeCache[key]; ok {
			t.nodeCacheBytes -= existing.encodedBytes
		}
		t.nodeCache[key] = cachedNode{
			branch:       cloneBranch(entry.branch),
			hash:         entry.hash,
			encodedBytes: entry.encodedBytes,
		}
		t.nodeCacheBytes += entry.encodedBytes
	}
}

func (s *Snapshot) materializeInputs(inputs []disk.LeafInput) error {
	if len(s.loadedKeys) == 0 && len(inputs) > 0 {
		// A 10k-leaf batch at tens of millions of leaves usually materializes
		// more than 100k nodes. Pre-sizing avoids repeated large map growth.
		s.loadedKeys = make(map[disk.NodeKey]struct{}, len(inputs)*16)
	}
	seen := make(map[disk.Key]struct{}, len(inputs))
	keys := make([]disk.Key, 0, len(inputs))
	for _, input := range inputs {
		key, err := disk.KeyFromBytes(input.Key)
		if err != nil {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sortStart := time.Now()
	sort.Slice(keys, func(i, j int) bool {
		return keyPathLess(keys[i], keys[j])
	})
	s.stats.MaterializeSortDuration += time.Since(sortStart)
	if s.parent != nil && (s.parent.materializeMode == MaterializeFrontier || s.parent.materializeMode == MaterializeParallel) {
		if s.parent.materializeMode == MaterializeParallel {
			return s.materializeInputsParallel(keys)
		}
		return s.materializeInputsFrontier(keys)
	}
	for _, key := range keys {
		if err := s.materializeKey(key); err != nil {
			return err
		}
	}
	return nil
}

type frontierReq struct {
	slot     **disk.Branch
	nodeKey  disk.NodeKey
	prefix   disk.PrefixBits
	startBit int
	keys     []disk.Key
	root     bool
}

type frontierLoadReq struct {
	req  frontierReq
	hash disk.Hash
}

func (s *Snapshot) materializeInputsFrontier(keys []disk.Key) error {
	if len(keys) == 0 {
		return nil
	}
	root := s.tree.Root()
	if root == nil {
		return nil
	}

	reqs := []frontierReq{{
		slot:     &root,
		nodeKey:  disk.RootNodeKey(),
		prefix:   disk.PrefixBits{},
		startBit: 0,
		keys:     keys,
		root:     true,
	}}

	for len(reqs) > 0 {
		next, err := s.materializeFrontier(reqs)
		if err != nil {
			return err
		}
		reqs = next
	}
	return nil
}

func (s *Snapshot) materializeInputsParallel(keys []disk.Key) error {
	if len(keys) == 0 {
		return nil
	}
	root := s.tree.Root()
	if root == nil {
		return nil
	}
	workers := s.parent.materializeWorkers
	if workers == 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers < 1 {
		workers = 1
	}
	sem := make(chan struct{}, workers)
	loaded, err := s.materializeBranchParallel(root, disk.RootNodeKey(), disk.PrefixBits{}, 0, keys, sem)
	if err != nil {
		return err
	}
	s.tree = disk.NewTreeWithRoot(loaded)
	return nil
}

func (s *Snapshot) materializeBranchParallel(branch *disk.Branch, nodeKey disk.NodeKey, prefix disk.PrefixBits, startBit int, keys []disk.Key, sem chan struct{}) (*disk.Branch, error) {
	if branch == nil {
		return nil, nil
	}
	if branch.Kind == disk.BranchKindStub {
		loaded, err := s.loadBranchParallel(nodeKey, branch.StubHash)
		if err != nil {
			return nil, err
		}
		branch = loaded
	}
	if branch.Kind != disk.BranchKindInternal {
		return branch, nil
	}
	node := branch.Internal
	if node == nil {
		return nil, fmt.Errorf("disk SMT persist: malformed internal node")
	}

	routeStart := time.Now()
	leftKeys, rightKeys, err := routeSortedKeysForInternal(keys, node.Path, startBit)
	s.recordMaterializeRoute(time.Since(routeStart))
	if err != nil {
		return nil, err
	}

	var leftKey, rightKey disk.NodeKey
	var leftPrefix, rightPrefix disk.PrefixBits
	var leftStart, rightStart int
	if len(leftKeys) > 0 {
		leftKey, leftPrefix, leftStart, err = childNodeKey(prefix, startBit, node.Path, 0)
		if err != nil {
			return nil, err
		}
	}
	if len(rightKeys) > 0 {
		rightKey, rightPrefix, rightStart, err = childNodeKey(prefix, startBit, node.Path, 1)
		if err != nil {
			return nil, err
		}
	}

	var leftBranch, rightBranch *disk.Branch
	var leftErr, rightErr error
	if len(leftKeys) > 0 && len(rightKeys) > 0 && tryAcquire(sem) {
		s.recordMaterializeFork()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer release(sem)
			leftBranch, leftErr = s.materializeBranchParallel(node.Left, leftKey, leftPrefix, leftStart, leftKeys, sem)
		}()
		rightBranch, rightErr = s.materializeBranchParallel(node.Right, rightKey, rightPrefix, rightStart, rightKeys, sem)
		wg.Wait()
	} else {
		if len(leftKeys) > 0 {
			leftBranch, leftErr = s.materializeBranchParallel(node.Left, leftKey, leftPrefix, leftStart, leftKeys, sem)
		}
		if len(rightKeys) > 0 {
			rightBranch, rightErr = s.materializeBranchParallel(node.Right, rightKey, rightPrefix, rightStart, rightKeys, sem)
		}
	}
	if leftErr != nil {
		return nil, leftErr
	}
	if rightErr != nil {
		return nil, rightErr
	}
	if len(leftKeys) > 0 {
		node.Left = leftBranch
	}
	if len(rightKeys) > 0 {
		node.Right = rightBranch
	}
	return branch, nil
}

func (s *Snapshot) loadBranchParallel(key disk.NodeKey, expectedHash disk.Hash) (*disk.Branch, error) {
	if branch, ok, err := s.loadBranchFromOverlay(key, expectedHash, true); ok || err != nil {
		return branch, err
	}
	if entry, ok := s.parent.getCachedBranch(key); ok {
		if entry.hash != expectedHash {
			return nil, fmt.Errorf("disk SMT persist: cached node %x hash mismatch: got %x want %x", key.Bytes(), entry.hash, expectedHash)
		}
		s.recordParallelMaterialized(key, entry.branch.Kind, entry.encodedBytes, true, entry.encodedBytes, 0, 0, 0)
		return cloneBranch(entry.branch), nil
	}
	readStart := time.Now()
	encoded, ok, err := s.parent.store.GetNode(key)
	readDuration := time.Since(readStart)
	s.recordNodeReads(1)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("disk SMT persist: node %x missing from store", key.Bytes())
	}
	decodeStart := time.Now()
	branch, err := decodeBranch(encoded)
	decodeDuration := time.Since(decodeStart)
	if err != nil {
		return nil, err
	}
	hashStart := time.Now()
	hash, err := branch.HashValue()
	hashDuration := time.Since(hashStart)
	if err != nil {
		return nil, err
	}
	if hash != expectedHash {
		return nil, fmt.Errorf("disk SMT persist: node %x hash mismatch: got %x want %x", key.Bytes(), hash, expectedHash)
	}
	s.recordParallelMaterialized(key, branch.Kind, int64(len(encoded)), false, 0, readDuration, decodeDuration, hashDuration)
	s.parent.cacheLoadedBranch(key, branch, int64(len(encoded)), hash)
	return branch, nil
}

type overlaySource uint8

const (
	overlaySourceOwn overlaySource = iota + 1
	overlaySourceParent
)

func (s *Snapshot) loadBranchFromOverlay(key disk.NodeKey, expectedHash disk.Hash, lockedStats bool) (*disk.Branch, bool, error) {
	entry, source, ok := s.lookupOverlay(key)
	if !ok {
		return nil, false, nil
	}
	if entry.delete {
		return nil, true, fmt.Errorf("disk SMT persist: node %x is tombstoned in snapshot overlay", key.Bytes())
	}
	branch, err := decodeBranch(entry.value)
	if err != nil {
		return nil, true, err
	}
	hash, err := branch.HashValue()
	if err != nil {
		return nil, true, err
	}
	if hash != expectedHash {
		return nil, true, fmt.Errorf("disk SMT persist: overlay node %x hash mismatch: got %x want %x", key.Bytes(), hash, expectedHash)
	}
	addLoaded := source == overlaySourceParent
	if lockedStats {
		s.recordMaterialized(key, branch.Kind, int64(len(entry.value)), false, 0, 0, 0, 0, addLoaded)
	} else {
		if addLoaded {
			s.loadedKeys[key] = struct{}{}
		}
		s.stats.MaterializedNodes++
		s.stats.MaterializedBytes += int64(len(entry.value))
		switch branch.Kind {
		case disk.BranchKindLeaf:
			s.stats.MaterializedLeafNodes++
		case disk.BranchKindInternal:
			s.stats.MaterializedInternalNodes++
		}
		recordMaterializedDepth(&s.stats, key.DepthBits())
	}
	return branch, true, nil
}

func (s *Snapshot) lookupOverlay(key disk.NodeKey) (overlayEntry, overlaySource, bool) {
	if s == nil {
		return overlayEntry{}, 0, false
	}
	if entry, ok := s.ownOverlay[key]; ok {
		return entry, overlaySourceOwn, true
	}
	if entry, ok := s.parentOverlay[key]; ok {
		return entry, overlaySourceParent, true
	}
	return overlayEntry{}, 0, false
}

func (s *Snapshot) recordParallelMaterialized(key disk.NodeKey, kind disk.BranchKind, encodedBytes int64, cacheHit bool, cacheBytes int64, readDuration, decodeDuration, hashDuration time.Duration) {
	s.recordMaterialized(key, kind, encodedBytes, cacheHit, cacheBytes, readDuration, decodeDuration, hashDuration, true)
}

func (s *Snapshot) recordMaterialized(key disk.NodeKey, kind disk.BranchKind, encodedBytes int64, cacheHit bool, cacheBytes int64, readDuration, decodeDuration, hashDuration time.Duration, addLoaded bool) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	if addLoaded {
		s.loadedKeys[key] = struct{}{}
	}
	s.stats.MaterializedNodes++
	s.stats.MaterializedBytes += encodedBytes
	switch kind {
	case disk.BranchKindLeaf:
		s.stats.MaterializedLeafNodes++
	case disk.BranchKindInternal:
		s.stats.MaterializedInternalNodes++
	}
	recordMaterializedDepth(&s.stats, key.DepthBits())
	if cacheHit {
		s.stats.MaterializeCacheHits++
		s.stats.MaterializeCacheBytes += cacheBytes
	}
	s.stats.MaterializeReadDuration += readDuration
	s.stats.MaterializeDecodeDuration += decodeDuration
	s.stats.MaterializeHashDuration += hashDuration
}

func (s *Snapshot) recordNodeReads(count int) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	s.stats.NodeReads += count
}

func (s *Snapshot) recordMaterializeRoute(duration time.Duration) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	s.stats.MaterializeRouteDuration += duration
}

func (s *Snapshot) recordMaterializeFork() {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	s.stats.MaterializeParallelForks++
}

func recordMaterializedDepth(stats *disk.ApplyStats, depth int) {
	switch {
	case depth <= 8:
		stats.MaterializedDepthLE8++
	case depth <= 12:
		stats.MaterializedDepthLE12++
	case depth <= 16:
		stats.MaterializedDepthLE16++
	case depth <= 18:
		stats.MaterializedDepthLE18++
	case depth <= 20:
		stats.MaterializedDepthLE20++
	case depth <= 22:
		stats.MaterializedDepthLE22++
	case depth <= 24:
		stats.MaterializedDepthLE24++
	default:
		stats.MaterializedDepthGT24++
	}
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

func (s *Snapshot) materializeFrontier(reqs []frontierReq) ([]frontierReq, error) {
	loads := make([]frontierLoadReq, 0, len(reqs))
	for _, req := range reqs {
		if req.slot == nil || *req.slot == nil {
			continue
		}
		branch := *req.slot
		if branch.Kind != disk.BranchKindStub {
			continue
		}
		loads = append(loads, frontierLoadReq{
			req:  req,
			hash: branch.StubHash,
		})
	}
	if err := s.loadFrontier(loads); err != nil {
		return nil, err
	}

	next := make([]frontierReq, 0, len(reqs)*2)
	for _, req := range reqs {
		if req.slot == nil || *req.slot == nil {
			continue
		}
		var err error
		next, err = s.appendFrontierChildren(next, req, *req.slot)
		if err != nil {
			return nil, err
		}
	}
	return next, nil
}

func (s *Snapshot) loadFrontier(loads []frontierLoadReq) error {
	if len(loads) == 0 {
		return nil
	}
	if s.parent != nil && s.parent.materializeMode == MaterializeParallel {
		return s.loadFrontierParallel(loads)
	}

	misses := make([]frontierLoadReq, 0, len(loads))
	for _, load := range loads {
		if branch, ok, err := s.loadBranchFromOverlay(load.req.nodeKey, load.hash, false); ok || err != nil {
			if err != nil {
				return err
			}
			s.attachLoadedBranch(load.req, branch)
			continue
		}
		if entry, ok := s.parent.getCachedBranch(load.req.nodeKey); ok {
			if entry.hash != load.hash {
				return fmt.Errorf("disk SMT persist: cached node %x hash mismatch: got %x want %x", load.req.nodeKey.Bytes(), entry.hash, load.hash)
			}
			s.attachLoadedBranch(load.req, cloneBranch(entry.branch))
			s.loadedKeys[load.req.nodeKey] = struct{}{}
			s.stats.MaterializedNodes++
			s.stats.MaterializedBytes += entry.encodedBytes
			s.stats.MaterializeCacheHits++
			s.stats.MaterializeCacheBytes += entry.encodedBytes
			continue
		}
		misses = append(misses, load)
	}
	if len(misses) == 0 {
		return nil
	}

	sortStart := time.Now()
	sort.Slice(misses, func(i, j int) bool {
		return nodeKeyLess(misses[i].req.nodeKey, misses[j].req.nodeKey)
	})
	s.stats.MaterializeSortDuration += time.Since(sortStart)

	keys := make([]disk.NodeKey, len(misses))
	for i, miss := range misses {
		keys[i] = miss.req.nodeKey
	}

	readStart := time.Now()
	results, err := s.parent.store.GetNodes(keys, s.parent.frontierReadMode == FrontierReadIterator)
	s.stats.MaterializeReadDuration += time.Since(readStart)
	s.stats.NodeReads += len(keys)
	if err != nil {
		return err
	}
	for i, result := range results {
		miss := misses[i]
		if !result.Found {
			return fmt.Errorf("disk SMT persist: node %x missing from store", miss.req.nodeKey.Bytes())
		}
		branch, hash, err := s.decodeAndValidateLoadedBranch(miss.req.nodeKey, result.Value, miss.hash)
		if err != nil {
			return err
		}
		s.attachLoadedBranch(miss.req, branch)
		s.loadedKeys[miss.req.nodeKey] = struct{}{}
		s.stats.MaterializedNodes++
		s.stats.MaterializedBytes += int64(len(result.Value))
		s.parent.cacheLoadedBranch(miss.req.nodeKey, branch, int64(len(result.Value)), hash)
	}
	return nil
}

type parallelLoadResult struct {
	branch       *disk.Branch
	hash         disk.Hash
	encodedBytes int64
	err          error
}

func (s *Snapshot) loadFrontierParallel(loads []frontierLoadReq) error {
	misses := make([]frontierLoadReq, 0, len(loads))
	for _, load := range loads {
		if branch, ok, err := s.loadBranchFromOverlay(load.req.nodeKey, load.hash, false); ok || err != nil {
			if err != nil {
				return err
			}
			s.attachLoadedBranch(load.req, branch)
			continue
		}
		if entry, ok := s.parent.getCachedBranch(load.req.nodeKey); ok {
			if entry.hash != load.hash {
				return fmt.Errorf("disk SMT persist: cached node %x hash mismatch: got %x want %x", load.req.nodeKey.Bytes(), entry.hash, load.hash)
			}
			s.attachLoadedBranch(load.req, cloneBranch(entry.branch))
			s.loadedKeys[load.req.nodeKey] = struct{}{}
			s.stats.MaterializedNodes++
			s.stats.MaterializedBytes += entry.encodedBytes
			s.stats.MaterializeCacheHits++
			s.stats.MaterializeCacheBytes += entry.encodedBytes
			continue
		}
		misses = append(misses, load)
	}
	if len(misses) == 0 {
		return nil
	}

	results := make([]parallelLoadResult, len(misses))
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	if workers > len(misses) {
		workers = len(misses)
	}

	readStart := time.Now()
	jobs := make(chan int)
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wg.Done()
			for idx := range jobs {
				miss := misses[idx]
				encoded, ok, err := s.parent.store.GetNode(miss.req.nodeKey)
				if err != nil {
					results[idx].err = err
					continue
				}
				if !ok {
					results[idx].err = fmt.Errorf("disk SMT persist: node %x missing from store", miss.req.nodeKey.Bytes())
					continue
				}
				branch, err := decodeBranch(encoded)
				if err != nil {
					results[idx].err = err
					continue
				}
				hash, err := branch.HashValue()
				if err != nil {
					results[idx].err = err
					continue
				}
				if hash != miss.hash {
					results[idx].err = fmt.Errorf("disk SMT persist: node %x hash mismatch: got %x want %x", miss.req.nodeKey.Bytes(), hash, miss.hash)
					continue
				}
				results[idx] = parallelLoadResult{
					branch:       branch,
					hash:         hash,
					encodedBytes: int64(len(encoded)),
				}
			}
		}()
	}
	for idx := range misses {
		jobs <- idx
	}
	close(jobs)
	wg.Wait()
	s.stats.MaterializeReadDuration += time.Since(readStart)
	s.stats.NodeReads += len(misses)

	for i, result := range results {
		if result.err != nil {
			return result.err
		}
		miss := misses[i]
		s.attachLoadedBranch(miss.req, result.branch)
		s.loadedKeys[miss.req.nodeKey] = struct{}{}
		s.stats.MaterializedNodes++
		s.stats.MaterializedBytes += result.encodedBytes
		s.parent.cacheLoadedBranch(miss.req.nodeKey, result.branch, result.encodedBytes, result.hash)
	}
	return nil
}

func (s *Snapshot) attachLoadedBranch(req frontierReq, branch *disk.Branch) {
	*req.slot = branch
	if req.root {
		s.tree = disk.NewTreeWithRoot(branch)
	}
}

func (s *Snapshot) decodeAndValidateLoadedBranch(key disk.NodeKey, encoded []byte, expectedHash disk.Hash) (*disk.Branch, disk.Hash, error) {
	decodeStart := time.Now()
	branch, err := decodeBranch(encoded)
	s.stats.MaterializeDecodeDuration += time.Since(decodeStart)
	if err != nil {
		return nil, disk.Hash{}, err
	}
	hashStart := time.Now()
	hash, err := branch.HashValue()
	s.stats.MaterializeHashDuration += time.Since(hashStart)
	if err != nil {
		return nil, disk.Hash{}, err
	}
	if hash != expectedHash {
		return nil, disk.Hash{}, fmt.Errorf("disk SMT persist: node %x hash mismatch: got %x want %x", key.Bytes(), hash, expectedHash)
	}
	return branch, hash, nil
}

func (s *Snapshot) appendFrontierChildren(next []frontierReq, req frontierReq, branch *disk.Branch) ([]frontierReq, error) {
	return appendFrontierChildren(next, req, branch)
}

func appendFrontierChildren(next []frontierReq, req frontierReq, branch *disk.Branch) ([]frontierReq, error) {
	if branch.Kind != disk.BranchKindInternal {
		return next, nil
	}
	node := branch.Internal
	if node == nil {
		return nil, fmt.Errorf("disk SMT persist: malformed internal node")
	}

	leftKeys, rightKeys, err := routeSortedKeysForInternal(req.keys, node.Path, req.startBit)
	if err != nil {
		return nil, err
	}

	if len(leftKeys) > 0 {
		childKey, childPrefix, childStart, err := childNodeKey(req.prefix, req.startBit, node.Path, 0)
		if err != nil {
			return nil, err
		}
		next = append(next, frontierReq{
			slot:     &node.Left,
			nodeKey:  childKey,
			prefix:   childPrefix,
			startBit: childStart,
			keys:     leftKeys,
		})
	}
	if len(rightKeys) > 0 {
		childKey, childPrefix, childStart, err := childNodeKey(req.prefix, req.startBit, node.Path, 1)
		if err != nil {
			return nil, err
		}
		next = append(next, frontierReq{
			slot:     &node.Right,
			nodeKey:  childKey,
			prefix:   childPrefix,
			startBit: childStart,
			keys:     rightKeys,
		})
	}
	return next, nil
}

func routeSortedKeysForInternal(keys []disk.Key, path disk.CompressedPath, startBit int) ([]disk.Key, []disk.Key, error) {
	if len(keys) == 0 {
		return nil, nil, nil
	}
	matchStart := -1
	matchEnd := -1
	for i, key := range keys {
		firstDiv, err := firstDivergenceInPath(path, key, startBit)
		if err != nil {
			return nil, nil, err
		}
		if firstDiv < path.Len() {
			if matchStart >= 0 {
				matchEnd = i
				break
			}
			continue
		}
		if matchStart < 0 {
			matchStart = i
		}
		matchEnd = i + 1
	}
	if matchStart < 0 {
		return nil, nil, nil
	}

	split := startBit + path.Len()
	if split < 0 || split >= disk.KeyBits {
		return nil, nil, fmt.Errorf("disk SMT persist: invalid internal split bit %d", split)
	}
	matching := keys[matchStart:matchEnd]
	rightStart := sort.Search(len(matching), func(i int) bool {
		return disk.KeyBit(matching[i], split) == 1
	})
	return matching[:rightStart], matching[rightStart:], nil
}

func nodeKeyLess(left, right disk.NodeKey) bool {
	return left.Less(right)
}

func keyPathLess(left, right disk.Key) bool {
	for depth := 0; depth < disk.KeyBits; depth++ {
		leftBit := disk.KeyBit(left, depth)
		rightBit := disk.KeyBit(right, depth)
		if leftBit != rightBit {
			return leftBit < rightBit
		}
	}
	return false
}

func (s *Snapshot) materializeKey(key disk.Key) error {
	root := s.tree.Root()
	if root == nil {
		return nil
	}
	return s.materializeSlot(&root, disk.RootNodeKey(), disk.PrefixBits{}, 0, key)
}

// materializeSlot replaces a hash stub with the persisted branch at nodeKey,
// then follows the requested key through the compressed path. For recursive
// calls, slot points at a real child field such as node.Left or node.Right. For
// the root call, slot points at a local variable, so the root replacement also
// has to update s.tree explicitly.
func (s *Snapshot) materializeSlot(slot **disk.Branch, nodeKey disk.NodeKey, prefix disk.PrefixBits, startBit int, key disk.Key) error {
	if slot == nil || *slot == nil {
		return nil
	}
	branch := *slot
	if branch.Kind == disk.BranchKindStub {
		loaded, err := s.loadBranch(nodeKey, branch.StubHash)
		if err != nil {
			return err
		}
		*slot = loaded
		branch = loaded
		if nodeKey.IsRoot() {
			s.tree = disk.NewTreeWithRoot(loaded)
		}
	}

	if branch.Kind != disk.BranchKindInternal {
		return nil
	}
	node := branch.Internal
	if node == nil {
		return fmt.Errorf("disk SMT persist: malformed internal node")
	}

	firstDiv, err := firstDivergenceInPath(node.Path, key, startBit)
	if err != nil {
		return err
	}
	if firstDiv < node.Path.Len() {
		return nil
	}

	split := startBit + node.Path.Len()
	if split < 0 || split >= disk.KeyBits {
		return fmt.Errorf("disk SMT persist: invalid internal split bit %d", split)
	}

	direction := disk.KeyBit(key, split)
	childKey, childPrefix, childStart, err := childNodeKey(prefix, startBit, node.Path, direction)
	if err != nil {
		return err
	}
	if direction == 1 {
		return s.materializeSlot(&node.Right, childKey, childPrefix, childStart, key)
	}
	return s.materializeSlot(&node.Left, childKey, childPrefix, childStart, key)
}

func (s *Snapshot) loadBranch(key disk.NodeKey, expectedHash disk.Hash) (*disk.Branch, error) {
	if branch, ok, err := s.loadBranchFromOverlay(key, expectedHash, false); ok || err != nil {
		return branch, err
	}
	if entry, ok := s.parent.getCachedBranch(key); ok {
		if entry.hash != expectedHash {
			return nil, fmt.Errorf("disk SMT persist: cached node %x hash mismatch: got %x want %x", key.Bytes(), entry.hash, expectedHash)
		}
		s.loadedKeys[key] = struct{}{}
		s.stats.MaterializedNodes++
		s.stats.MaterializedBytes += entry.encodedBytes
		s.stats.MaterializeCacheHits++
		s.stats.MaterializeCacheBytes += entry.encodedBytes
		return cloneBranch(entry.branch), nil
	}
	readStart := time.Now()
	encoded, ok, err := s.parent.store.GetNode(key)
	s.stats.MaterializeReadDuration += time.Since(readStart)
	s.stats.NodeReads++
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("disk SMT persist: node %x missing from store", key.Bytes())
	}
	branch, hash, err := s.decodeAndValidateLoadedBranch(key, encoded, expectedHash)
	if err != nil {
		return nil, err
	}
	s.loadedKeys[key] = struct{}{}
	s.stats.MaterializedNodes++
	s.stats.MaterializedBytes += int64(len(encoded))
	s.parent.cacheLoadedBranch(key, branch, int64(len(encoded)), hash)
	return branch, nil
}

func decodeBranch(encoded []byte) (*disk.Branch, error) {
	tag, err := disk.SerializedTag(encoded)
	if err != nil {
		return nil, err
	}
	switch tag {
	case disk.TagLeaf:
		leaf, err := disk.UnmarshalLeaf(encoded)
		if err != nil {
			return nil, err
		}
		return &disk.Branch{Kind: disk.BranchKindLeaf, Leaf: leaf}, nil
	case disk.TagInternal:
		internal, err := disk.UnmarshalInternal(encoded)
		if err != nil {
			return nil, err
		}
		return &disk.Branch{Kind: disk.BranchKindInternal, Internal: internal}, nil
	default:
		return nil, fmt.Errorf("disk SMT persist: unknown serialized node tag %d", tag)
	}
}

func collectWrites(branch *disk.Branch, key disk.NodeKey, prefix disk.PrefixBits, startBit int, writes *[]storage.NodeWrite, writeSet map[disk.NodeKey]struct{}, cacheWrites map[disk.NodeKey]cachedNode, cacheDepth int) error {
	if branch == nil {
		return nil
	}
	switch branch.Kind {
	case disk.BranchKindLeaf:
		if branch.Leaf == nil {
			return fmt.Errorf("disk SMT persist: malformed leaf branch")
		}
		encoded, err := disk.MarshalLeaf(branch.Leaf)
		if err != nil {
			return err
		}
		*writes = append(*writes, storage.NodeWrite{Key: key, Value: encoded})
		writeSet[key] = struct{}{}
		if err := collectCacheWrite(branch, key, encoded, cacheWrites, cacheDepth); err != nil {
			return err
		}
		return nil
	case disk.BranchKindInternal:
		if branch.Internal == nil {
			return fmt.Errorf("disk SMT persist: malformed internal branch")
		}
		encoded, err := disk.MarshalInternal(branch.Internal)
		if err != nil {
			return err
		}
		*writes = append(*writes, storage.NodeWrite{Key: key, Value: encoded})
		writeSet[key] = struct{}{}
		if err := collectCacheWrite(branch, key, encoded, cacheWrites, cacheDepth); err != nil {
			return err
		}

		leftKey, leftPrefix, leftStart, err := childNodeKey(prefix, startBit, branch.Internal.Path, 0)
		if err != nil {
			return err
		}
		if err := collectWrites(branch.Internal.Left, leftKey, leftPrefix, leftStart, writes, writeSet, cacheWrites, cacheDepth); err != nil {
			return err
		}
		rightKey, rightPrefix, rightStart, err := childNodeKey(prefix, startBit, branch.Internal.Path, 1)
		if err != nil {
			return err
		}
		return collectWrites(branch.Internal.Right, rightKey, rightPrefix, rightStart, writes, writeSet, cacheWrites, cacheDepth)
	case disk.BranchKindStub:
		return nil
	default:
		return fmt.Errorf("disk SMT persist: unknown branch kind %d", branch.Kind)
	}
}

func collectCacheWrite(branch *disk.Branch, key disk.NodeKey, encoded []byte, cacheWrites map[disk.NodeKey]cachedNode, cacheDepth int) error {
	if cacheWrites == nil || key.DepthBits() > cacheDepth {
		return nil
	}
	cacheBranch, hash, err := cacheBranchForPersistedNode(branch)
	if err != nil {
		return err
	}
	cacheWrites[key] = cachedNode{
		branch:       cacheBranch,
		hash:         hash,
		encodedBytes: int64(len(encoded)),
	}
	return nil
}

func cacheBranchForPersistedNode(branch *disk.Branch) (*disk.Branch, disk.Hash, error) {
	hash, err := branch.HashValue()
	if err != nil {
		return nil, disk.Hash{}, err
	}
	switch branch.Kind {
	case disk.BranchKindLeaf:
		if branch.Leaf == nil {
			return nil, disk.Hash{}, fmt.Errorf("disk SMT persist: malformed leaf branch")
		}
		return &disk.Branch{
			Kind: disk.BranchKindLeaf,
			Leaf: &disk.LeafNode{
				Key:   branch.Leaf.Key,
				Value: append([]byte(nil), branch.Leaf.Value...),
				Hash:  branch.Leaf.Hash,
			},
		}, hash, nil
	case disk.BranchKindInternal:
		if branch.Internal == nil {
			return nil, disk.Hash{}, fmt.Errorf("disk SMT persist: malformed internal branch")
		}
		leftHash, err := branch.Internal.Left.HashValue()
		if err != nil {
			return nil, disk.Hash{}, err
		}
		rightHash, err := branch.Internal.Right.HashValue()
		if err != nil {
			return nil, disk.Hash{}, err
		}
		return &disk.Branch{
			Kind: disk.BranchKindInternal,
			Internal: &disk.InternalNode{
				Path:  branch.Internal.Path,
				Depth: branch.Internal.Depth,
				Left:  disk.NewStub(leftHash),
				Right: disk.NewStub(rightHash),
				Hash:  branch.Internal.Hash,
			},
		}, hash, nil
	default:
		return nil, disk.Hash{}, fmt.Errorf("disk SMT persist: cannot cache branch kind %d", branch.Kind)
	}
}

func childNodeKey(prefix disk.PrefixBits, startBit int, path disk.CompressedPath, direction byte) (disk.NodeKey, disk.PrefixBits, int, error) {
	if direction != 0 && direction != 1 {
		return disk.NodeKey{}, disk.PrefixBits{}, 0, fmt.Errorf("disk SMT persist: invalid child direction %d", direction)
	}
	if startBit < 0 || startBit+path.Len() >= disk.KeyBits {
		return disk.NodeKey{}, disk.PrefixBits{}, 0, fmt.Errorf("disk SMT persist: invalid child path start=%d len=%d", startBit, path.Len())
	}
	nextPrefix := prefix
	for i := 0; i < path.Len(); i++ {
		if path.BitAt(i) != 0 {
			setPrefixBit(&nextPrefix, startBit+i)
		}
	}
	split := startBit + path.Len()
	if direction != 0 {
		setPrefixBit(&nextPrefix, split)
	}
	childStart := split + 1
	key, err := disk.NewNodeKey(childStart, nextPrefix)
	if err != nil {
		return disk.NodeKey{}, disk.PrefixBits{}, 0, err
	}
	return key, nextPrefix, childStart, nil
}

func setPrefixBit(prefix *disk.PrefixBits, pos int) {
	if pos < 0 || pos >= disk.KeyBits {
		panic(fmt.Sprintf("disk SMT persist: prefix bit index out of range: %d", pos))
	}
	prefix[pos/8] |= 1 << (uint(pos) % 8)
}

func firstDivergenceInPath(path disk.CompressedPath, key disk.Key, startBit int) (int, error) {
	if startBit < 0 || startBit+path.Len() > disk.KeyBits {
		return 0, fmt.Errorf("disk SMT persist: invalid path match start=%d len=%d", startBit, path.Len())
	}
	for i := 0; i < path.Len(); i++ {
		if path.BitAt(i) != disk.KeyBit(key, startBit+i) {
			return i, nil
		}
	}
	return path.Len(), nil
}

func cloneBranch(branch *disk.Branch) *disk.Branch {
	if branch == nil {
		return nil
	}
	next := &disk.Branch{
		Kind:     branch.Kind,
		StubHash: branch.StubHash,
	}
	if branch.Leaf != nil {
		next.Leaf = &disk.LeafNode{
			Key:   branch.Leaf.Key,
			Value: append([]byte(nil), branch.Leaf.Value...),
			Hash:  branch.Leaf.Hash,
		}
	}
	if branch.Internal != nil {
		next.Internal = &disk.InternalNode{
			Path:  branch.Internal.Path,
			Depth: branch.Internal.Depth,
			Left:  cloneBranch(branch.Internal.Left),
			Right: cloneBranch(branch.Internal.Right),
			Hash:  branch.Internal.Hash,
		}
	}
	return next
}

func stubForRoot(root disk.Hash) *disk.Branch {
	if root == disk.EmptyRootHash() {
		return nil
	}
	return disk.NewStub(root)
}
