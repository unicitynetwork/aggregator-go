package smt

import (
	"crypto/rand"
	"math/big"
	mrand "math/rand"
	"testing"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Differential fuzz: random (possibly nonsense) certs, keys, values, roots.
// The impl verifier and the literal spec verifier must always agree.
func TestSpec_DifferentialFuzz(t *testing.T) {
	rng := mrand.New(mrand.NewSource(0xC0FFEE))
	for iter := 0; iter < 20000; iter++ {
		var bm [32]byte
		nbits := rng.Intn(6)
		for i := 0; i < nbits; i++ {
			api.SetBitBE(bm[:], rng.Intn(256))
		}
		pc := 0
		for d := 0; d < 256; d++ {
			pc += int(api.KeyBitBE(bm[:], d))
		}
		// sometimes deliberately mismatch the sibling count
		n := pc
		switch rng.Intn(4) {
		case 0:
			n = pc + 1
		case 1:
			if pc > 0 {
				n = pc - 1
			}
		}
		sibs := make([][32]byte, n)
		for i := range sibs {
			rand.Read(sibs[i][:])
		}
		k := make([]byte, 32)
		v := make([]byte, rng.Intn(40))
		root := make([]byte, 32)
		rand.Read(k)
		rand.Read(v)
		rand.Read(root)

		cert := &api.InclusionCert{Bitmap: bm, Siblings: sibs}
		implOK := cert.Verify(k, v, root, api.SHA256) == nil

		specSibs := make([][]byte, len(sibs))
		for i := range sibs {
			specSibs[i] = sibs[i][:]
		}
		specOK := specVerifyInclusion(bm[:], specSibs, root, k, v)
		if implOK != specOK {
			t.Fatalf("iter %d DIVERGENCE: impl=%v spec=%v bitmap=%x nsibs=%d", iter, implOK, specOK, bm, n)
		}
	}
}

// Forgery attempt: take a genuine cert for leaf A, and try to make it verify
// for a different key that shares the path structure. Spec forbids it because
// regions are derived from the queried key.
func TestSpec_RegionBindsQueriedKey(t *testing.T) {
	tree, leaves := buildRandomTree(t, 128)
	root := tree.GetRootHashRaw()
	for _, l := range leaves[:8] {
		cert, err := tree.GetInclusionCert(l.k)
		if err != nil {
			t.Fatal(err)
		}
		// flip a key bit at a depth BELOW the deepest junction: descent side
		// unchanged, region changes only at depths > flipped bit.
		deepest := -1
		for d := 0; d < 256; d++ {
			if api.KeyBitBE(cert.Bitmap[:], d) == 1 {
				deepest = d
			}
		}
		if deepest >= 255 {
			continue
		}
		bad := append([]byte(nil), l.k...)
		bad[(deepest+1)/8] ^= 0x80 >> uint((deepest+1)%8)
		if err := cert.Verify(bad, l.v, root, api.SHA256); err == nil {
			t.Fatalf("impl ACCEPTED cert for a key differing below deepest junction (deepest=%d)", deepest)
		}
	}
}

// Sharded composition: does the certificate handed to a client verify under
// the LITERAL spec verifier with the state id as key?
func TestSpec_ComposedShardCertUnderSpec(t *testing.T) {
	const shardBits = 4
	parent := NewParentSparseMerkleTree(api.SHA256, shardBits)

	// shard 0b1010 -> shardID sentinel path 0b1_1010? Shard id encoding:
	// sentinel-prefixed int, BitLen()-1 == shardBits.
	shardID := api.ShardID(0b1_0110) // shard bits (path bit order) = 0,1,1,0
	child := NewChildSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits, shardID)

	// build keys that route to this shard
	var kvs []kv
	for len(kvs) < 5 {
		k := make([]byte, 32)
		rand.Read(k)
		p, err := api.FixedBytesToPath(k, api.StateTreeKeyLengthBits)
		if err != nil {
			t.Fatal(err)
		}
		v := make([]byte, 32)
		rand.Read(v)
		if err := child.AddLeaf(p, v); err != nil {
			continue // wrong shard
		}
		kvs = append(kvs, kv{k, v})
	}
	childRoot := child.GetRootHashRaw()

	// publish child root into the parent tree
	shardPath := big.NewInt(int64(shardID))
	if err := parent.AddLeaf(shardPath, childRoot); err != nil {
		t.Fatal(err)
	}
	// fill the other shards with random roots so siblings are non-trivial
	for s := 0; s < 1<<shardBits; s++ {
		id := (1 << shardBits) | s
		if api.ShardID(id) == shardID {
			continue
		}
		r := make([]byte, 32)
		rand.Read(r)
		if err := parent.AddLeaf(big.NewInt(int64(id)), r); err != nil {
			t.Fatal(err)
		}
	}
	parentRoot := parent.GetRootHashRaw()

	frag, err := parent.GetShardInclusionFragment(shardID)
	if err != nil {
		t.Fatal(err)
	}
	if frag == nil {
		t.Fatal("nil fragment")
	}

	for _, l := range kvs {
		childCert, err := child.GetInclusionCert(l.k)
		if err != nil {
			t.Fatal(err)
		}
		composed, err := api.ComposeInclusionCert(frag, childCert, childRoot)
		if err != nil {
			t.Fatalf("compose: %v", err)
		}
		if err := composed.Verify(l.k, l.v, parentRoot, api.SHA256); err != nil {
			t.Fatalf("impl verify composed: %v", err)
		}
		bm, sibs := certToSpec(composed)
		if !specVerifyInclusion(bm, sibs, parentRoot, l.k, l.v) {
			t.Fatalf("SPEC verifier rejected composed cert for key %x", l.k)
		}
	}
	t.Logf("composed certs OK; parent root %x", parentRoot)
}
