package smt

import (
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"unsafe"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// ReadView is an immutable handle to one committed SMT root. It shares nodes
// with the live tree (the tree is persistent: writers replace the path to a
// changed leaf and never mutate a node reachable from a published root), but
// owns its own root pointer, so later commits, snapshots and forks cannot
// change what it serves. Every method is safe for concurrent use and takes no
// tree lock.
//
// Safety rests on two properties, both enforced where the view is frozen:
//   - every reachable node has its hash cache populated, so readers never
//     write node state (the only mutation a node sees after construction);
//   - node key/value/region byte slices are copied on construction and never
//     handed out without copying.
type ReadView struct {
	tree *SparseMerkleTree // detached: not a snapshot, no original, never written; nil after Release
	root []byte            // raw root hash, fixed at freeze time
	seq  uint64            // root's creation sequence, the newest in the view

	// retainedBytes is the conservative incremental memory this view keeps
	// alive beyond the live tree: the nodes it holds that its successor
	// replaced. It is zero for the newest view and set when the successor is
	// frozen.
	retainedBytes atomic.Uint64
}

// readViewOverheadBytes covers the view struct and detached tree header.
const readViewOverheadBytes = 256

// FreezeReadView captures the current committed root as an immutable ReadView.
// The write lock is held only to prime hash caches and copy the root pointer.
//
// prev, when non-nil, is the previously frozen view of this tree; the nodes
// the new root introduced relative to it are charged to prev as the memory it
// retains. Pass nil for the first view.
func (ts *ThreadSafeSMT) FreezeReadView(prev *ReadView) *ReadView {
	ts.rwMux.Lock()
	ts.smt.ensureHashes()
	root := ts.smt.root
	tree := &SparseMerkleTree{
		parentMode: ts.smt.parentMode,
		keyLength:  ts.smt.keyLength,
		algorithm:  ts.smt.algorithm,
		root:       root,
	}
	rootHash := ts.smt.GetRootHashRaw()
	ts.rwMux.Unlock()

	v := &ReadView{tree: tree, root: rootHash, seq: root.seq}
	v.retainedBytes.Store(readViewOverheadBytes)
	if prev != nil {
		prev.retainedBytes.Add(v.introducedBytesSince(prev))
	}
	return v
}

// RootHashRaw returns a copy of the raw root hash this view was frozen at.
func (v *ReadView) RootHashRaw() []byte {
	return append([]byte(nil), v.root...)
}

// KeyLength returns the SMT key length in bits.
func (v *ReadView) KeyLength() int {
	if v.tree == nil {
		return 0
	}
	return v.tree.keyLength
}

// ErrReadViewReleased is returned by a released ReadView's tree accessors.
var ErrReadViewReleased = errors.New("smt: read view released")

// Release drops the view's reference to the tree so its nodes can be collected
// once no other root holds them. The root hash, sequence and retained-byte
// counter stay valid: the next frozen view charges its introduced nodes against
// them. The caller must ensure no GetInclusionCert call is in flight or follows.
func (v *ReadView) Release() { v.tree = nil }

// GetInclusionCert builds a v2 inclusion certificate against the frozen root.
func (v *ReadView) GetInclusionCert(key []byte) (*api.InclusionCert, error) {
	if v.tree == nil {
		return nil, ErrReadViewReleased
	}
	return v.tree.GetInclusionCert(key)
}

// RetainedBytes returns the conservative incremental bytes held by this view.
func (v *ReadView) RetainedBytes() uint64 { return v.retainedBytes.Load() }

// introducedBytesSince estimates the bytes of nodes reachable from v that
// prev's root did not hold. Parents are constructed after their children, so
// descending only into nodes newer than prev's root visits exactly the nodes
// created since (each once, as the tree is a tree). A root older than prev
// means the views are not on one lineage; fall back to charging the whole tree.
func (v *ReadView) introducedBytesSince(prev *ReadView) uint64 {
	floor := prev.seq
	if v.seq == floor {
		return 0 // the same committed root: nothing new is held
	}
	if v.seq < floor {
		floor = 0
	}
	var total uint64
	var walk func(b branch, top bool)
	walk = func(b branch, top bool) {
		switch n := b.(type) {
		case *NodeBranch:
			if n == nil || (!top && n.seq <= floor) {
				return
			}
			total += nodeBytes(n)
			walk(n.Left, false)
			walk(n.Right, false)
		case *LeafBranch:
			if n == nil || n.seq <= floor {
				return
			}
			total += leafBytes(n)
		}
	}
	walk(v.tree.root, true)
	return total
}

func bigIntBytes(i *big.Int) uint64 {
	if i == nil {
		return 0
	}
	return uint64(unsafe.Sizeof(*i)) + uint64((i.BitLen()+63)/64)*8
}

// allocRound rounds up to the allocator's typical 16-byte granularity.
func allocRound(n uint64) uint64 { return (n + 15) &^ 15 }

func nodeBytes(n *NodeBranch) uint64 {
	return allocRound(uint64(unsafe.Sizeof(*n))) + allocRound(bigIntBytes(n.Path)) + allocRound(uint64(len(n.Region)))
}

func leafBytes(l *LeafBranch) uint64 {
	return allocRound(uint64(unsafe.Sizeof(*l))) + allocRound(bigIntBytes(l.Path)) +
		allocRound(uint64(len(l.Key))) + allocRound(uint64(len(l.Value)))
}

// String is for diagnostics only.
func (v *ReadView) String() string {
	return fmt.Sprintf("smt.ReadView{root=%x seq=%d}", v.root, v.seq)
}
