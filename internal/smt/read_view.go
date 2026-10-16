package smt

import (
	"errors"
	"fmt"
	"math/big"
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

	// introduced is the estimated bytes of nodes this root holds that the
	// previously frozen root did not (see FreezeReadView).
	introduced uint64
}

// ReadViewOverheadBytes covers a view's own struct and detached tree header.
const ReadViewOverheadBytes = 256

// FreezeReadView captures the current committed root as an immutable ReadView.
// The write lock is held only to prime hash caches and copy the root pointer.
//
// prevSeq is Seq() of the previously frozen view of this tree, or 0 for the
// first. The view records as Introduced the bytes of nodes created since then;
// the caller decides which surviving view those replaced nodes are charged to.
func (ts *ThreadSafeSMT) FreezeReadView(prevSeq uint64) *ReadView {
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
	if prevSeq != 0 {
		v.introduced = v.introducedBytesSince(prevSeq)
	}
	return v
}

// Seq is the creation sequence of the view's root; later roots have larger
// values on one lineage.
func (v *ReadView) Seq() uint64 { return v.seq }

// Introduced returns the estimated bytes of nodes this view's root holds that
// the previously frozen root did not.
func (v *ReadView) Introduced() uint64 { return v.introduced }

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

// GetNonInclusionCert builds the non-inclusion certificate against the frozen root.
func (v *ReadView) GetNonInclusionCert(key []byte) (*api.NonInclusionCert, error) {
	if v.tree == nil {
		return nil, ErrReadViewReleased
	}
	return v.tree.GetNonInclusionCert(key)
}

// introducedBytesSince estimates the bytes of nodes reachable from v that
// the root with sequence floor did not hold. Parents are constructed after their children, so
// descending only into nodes newer than floor visits exactly the nodes
// created since (each once, as the tree is a tree). A root older than floor
// means the views are not on one lineage; fall back to charging the whole tree.
func (v *ReadView) introducedBytesSince(floor uint64) uint64 {
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
