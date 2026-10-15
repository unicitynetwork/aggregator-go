package persist

import (
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// BatchStats reports the node I/O of one batch proof build, separately for
// requested node reads and distinct nodes. The union-path builder reads every
// node of the union of the proof paths once, so NodeReads == UniqueNodes and
// both are far below Σ(proof path lengths) when the keys share prefixes.
type BatchStats struct {
	// NodeReads counts node keys requested from the reader (repeats included).
	NodeReads uint64
	// UniqueNodes counts distinct node keys requested.
	UniqueNodes uint64
	// Frontiers counts reader round trips (GetNodes batches / single GetNode calls).
	Frontiers uint64
}

// BuildInclusionCertsWithStats is BuildInclusionCerts plus node I/O statistics.
func BuildInclusionCertsWithStats(rootHash disk.Hash, reader storage.ReadStore, keys [][]byte) ([]*api.InclusionCert, BatchStats, error) {
	if reader == nil {
		certs, err := BuildInclusionCerts(rootHash, reader, keys)
		return certs, BatchStats{}, err
	}
	cr := &countingReader{inner: reader, seen: make(map[disk.NodeKey]struct{})}
	certs, err := BuildInclusionCerts(rootHash, cr, keys)
	return certs, BatchStats{NodeReads: cr.reads, UniqueNodes: uint64(len(cr.seen)), Frontiers: cr.calls}, err
}

// countingReader counts the nodes a build requests. A build runs on one goroutine.
type countingReader struct {
	inner storage.ReadStore
	seen  map[disk.NodeKey]struct{}
	reads uint64
	calls uint64
}

func (c *countingReader) GetNode(k disk.NodeKey) ([]byte, bool, error) {
	c.calls++
	c.reads++
	c.seen[k] = struct{}{}
	return c.inner.GetNode(k)
}

func (c *countingReader) GetNodes(keys []disk.NodeKey, sorted bool) ([]storage.NodeReadResult, error) {
	c.calls++
	c.reads += uint64(len(keys))
	for _, k := range keys {
		c.seen[k] = struct{}{}
	}
	return c.inner.GetNodes(keys, sorted)
}
