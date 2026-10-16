package nifixture

import (
	"bytes"
	"crypto"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/unicitynetwork/bft-core/rootchain/testutils/ucfixture"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"
)

// Version of the corpus layout.
const Version = 1

// Corpus constants.
const (
	// Partition is the application partition of every UC of the corpus (a single,
	// unsharded partition: empty shard ID).
	Partition types.PartitionID = 0xA1
	// ProofTag and ProofVersion identify the non-inclusion proof wrapper.
	ProofTag     = 39034
	ProofVersion = 1
)

// Verdicts: the shared, language independent classification of the outcome of
// verifying one wrapper under one context (checked in this order).
const (
	VerdictAbsent       = "absent"              // verified: the state is absent at the certified root
	VerdictMalformed    = "malformed"           // wrapper or certificate does not decode in the exact profile
	VerdictUCInvalid    = "uc_invalid"          // the UC body is invalid: structure, paths to the seal, partition, configuration pin
	VerdictAuthority    = "authority_rejected"  // the seal is not authenticated by the trusted authority/history
	VerdictStale        = "stale"               // authenticated, but older than the required minimum root round
	VerdictCertInvalid  = "certificate_invalid" // the certificate does not authenticate a terminal leaf under the UC root for this query
	VerdictStateInclude = "state_included"      // authenticated terminal is the query itself: the state is present
)

// File is the content of one corpus file by relative path.
type Files map[string][]byte

// Context is a named verification context: everything the verifier trusts.
type Context struct {
	Description string `json:"description"`
	// TrustBase is the corpus path of the CBOR encoded trust base (authority).
	TrustBase string `json:"trustBase"`
	// Partition is the expected application partition.
	Partition uint32 `json:"partition"`
	// NextEpochStart is the authenticated end boundary of the epoch (first round of
	// the next epoch); "0" when the history does not provide it.
	NextEpochStart string `json:"nextEpochStart"`
	// MinRootRound is the minimum certified root chain round the caller requires
	// ("0": none).
	MinRootRound string `json:"minRootRound"`
	// ShardConfHash optionally pins the shard configuration hash (hex, "" none).
	ShardConfHash string `json:"shardConfHash"`
}

// Expected is the expected verdict of a vector.
type Expected struct {
	Verdict string `json:"verdict"`
	Note    string `json:"note,omitempty"`
}

// LeafJSON is a tree leaf.
type LeafJSON struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Vector is one corpus case file (cases/<name>.json, negative/<name>.json).
type Vector struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	// Base is the positive case a negative derives from.
	Base string `json:"base,omitempty"`
	// Leaves of the certified tree (positive cases and their negatives).
	Leaves []LeafJSON `json:"leaves"`
	// Query is the queried state key (32 bytes hex).
	Query string `json:"query"`
	// Root is the certified state root (hex); "" is the certified empty tree.
	Root string `json:"root"`
	// Block is the wrapper's block number (decimal string).
	Block string `json:"block"`
	// Certificate is the raw certificate (hex, "" is the empty-tree certificate).
	Certificate string `json:"certificate"`
	// UC is the native tagged CBOR of the unicity certificate (hex).
	UC string `json:"uc"`
	// Wrapper is the complete result CBOR: [block, 39034([1, certificate, UC])] (hex).
	// For negatives it is the exact (possibly malformed) bytes under test.
	Wrapper string `json:"wrapper"`
	// Context names the verification context (contexts.json).
	Context  string   `json:"context"`
	Expected Expected `json:"expected"`
}

// ---- reference verification of the whole wrapper -----------------------------------------------

// Verdict is the reference classification of wrapper under ctx for query. It is the
// executable specification of SPEC.md "Verification order"; production verifiers must agree.
func Verdict(wrapper []byte, query [KeySize]byte, ctx Context, trustBase types.RootTrustBase) string {
	_, cert, ucRaw, err := DecodeWrapper(wrapper)
	if err != nil {
		return VerdictMalformed
	}
	if len(cert) > MaxCertificateBytes {
		return VerdictMalformed
	}
	var uc types.UnicityCertificate
	if err := types.Cbor.Unmarshal(ucRaw, &uc); err != nil {
		return VerdictMalformed
	}
	if re, err := uc.MarshalCBOR(); err != nil || !bytes.Equal(re, ucRaw) {
		return VerdictMalformed
	}
	if len(cert) > 0 {
		if len(cert) < 32+KeySize+ValueSize || (len(cert)-32-KeySize-ValueSize)%32 != 0 {
			return VerdictMalformed
		}
		pop := 0
		for _, b := range cert[:32] {
			for ; b != 0; b &= b - 1 {
				pop++
			}
		}
		if len(cert) != 32+32*pop+KeySize+ValueSize {
			return VerdictMalformed
		}
	}
	if err := refUCBody(&uc, ctx); err != nil {
		return VerdictUCInvalid
	}
	var next uint64
	fmt.Sscan(ctx.NextEpochStart, &next)
	var verifier types.SealVerifier = types.TrustBaseSealVerifier(trustBase)
	if next != 0 {
		verifier = types.TrustBaseSealVerifierUntil(trustBase, next)
	}
	if err := verifier.VerifySeal(uc.UnicitySeal); err != nil {
		return VerdictAuthority
	}
	var min uint64
	fmt.Sscan(ctx.MinRootRound, &min)
	if uc.UnicitySeal.RootChainRoundNumber < min {
		return VerdictStale
	}
	var root *[HashSize]byte
	switch h := uc.InputRecord.Hash; {
	case len(h) == 0:
	case len(h) == HashSize:
		r := [HashSize]byte(h)
		root = &r
	default:
		return VerdictCertInvalid
	}
	switch err := Verify(cert, query, root); {
	case err == nil:
		return VerdictAbsent
	case errors.Is(err, ErrStateIncluded):
		return VerdictStateInclude
	case errors.Is(err, ErrMalformed):
		return VerdictMalformed
	default:
		return VerdictCertInvalid
	}
}

// refUCBody is the body check of a UC: components present, structure valid, shard and
// partition paths reach the seal hash. The empty shard ID with an empty shard path is the
// unsharded case; the partition path may be nonempty.
func refUCBody(uc *types.UnicityCertificate, ctx Context) error {
	if uc.InputRecord == nil || uc.UnicitySeal == nil {
		return errors.New("missing component")
	}
	if uc.UnicityTreeCertificate == nil {
		return errors.New("no unicity tree certificate")
	}
	for _, s := range uc.UnicityTreeCertificate.HashSteps {
		if s == nil {
			return errors.New("null path step")
		}
	}
	var pin []byte
	if ctx.ShardConfHash != "" {
		pin, _ = hex.DecodeString(ctx.ShardConfHash)
	}
	if err := uc.IsValid(types.PartitionID(ctx.Partition), types.ShardID{}, pin); err != nil {
		return err
	}
	if uc.ShardTreeCertificate.Shard.Length() != 0 || len(uc.ShardTreeCertificate.SiblingHashes) != 0 {
		return errors.New("not an unsharded certificate")
	}
	shardRoot, err := uc.ShardTreeCertificate.ComputeCertificateHash(uc.InputRecord, uc.TRHash, uc.ShardConfHash, crypto.SHA256)
	if err != nil {
		return err
	}
	root, err := uc.UnicityTreeCertificate.EvalAuthPath(shardRoot, crypto.SHA256)
	if err != nil {
		return err
	}
	if !bytes.Equal(root, uc.UnicitySeal.Hash) {
		return errors.New("path does not reach the seal hash")
	}
	return nil
}

// ---- wrapper codec (hand written: this is the specification of the bytes) ---------------------------

func cborHead(major byte, n uint64) []byte {
	switch {
	case n < 24:
		return []byte{major<<5 | byte(n)}
	case n <= 0xff:
		return []byte{major<<5 | 24, byte(n)}
	case n <= 0xffff:
		return binary.BigEndian.AppendUint16([]byte{major<<5 | 25}, uint16(n))
	case n <= 0xffffffff:
		return binary.BigEndian.AppendUint32([]byte{major<<5 | 26}, uint32(n))
	}
	return binary.BigEndian.AppendUint64([]byte{major<<5 | 27}, n)
}

// EncodeWrapper encodes the response result: [block, 39034([1, certificate (bstr), uc (embedded value)])].
func EncodeWrapper(block uint64, cert, uc []byte) []byte {
	return EncodeWrapperWith(block, ProofTag, ProofVersion, cert, uc)
}

// EncodeWrapperWith is EncodeWrapper with explicit tag and version (negative vectors).
func EncodeWrapperWith(block, tag, version uint64, cert, uc []byte) []byte {
	out := cborHead(4, 2)
	out = append(out, cborHead(0, block)...)
	out = append(out, cborHead(6, tag)...)
	out = append(out, cborHead(4, 3)...)
	out = append(out, cborHead(0, version)...)
	out = append(out, cborHead(2, uint64(len(cert)))...)
	out = append(out, cert...)
	return append(out, uc...)
}

type cborReader struct {
	b []byte
	i int
}

func (r *cborReader) head() (major byte, n uint64, err error) {
	if r.i >= len(r.b) {
		return 0, 0, errors.New("truncated")
	}
	ib := r.b[r.i]
	r.i++
	major, ai := ib>>5, ib&31
	switch {
	case ai < 24:
		return major, uint64(ai), nil
	case ai >= 24 && ai <= 27:
		size := 1 << (ai - 24)
		if r.i+size > len(r.b) {
			return 0, 0, errors.New("truncated")
		}
		var v uint64
		for _, c := range r.b[r.i : r.i+size] {
			v = v<<8 | uint64(c)
		}
		r.i += size
		// canonical (shortest form) only
		if (ai == 24 && v < 24) || (ai == 25 && v <= 0xff) || (ai == 26 && v <= 0xffff) || (ai == 27 && v <= 0xffffffff) {
			return 0, 0, errors.New("non canonical integer")
		}
		return major, v, nil
	}
	return 0, 0, errors.New("unsupported additional information")
}

// skip returns the byte range of one complete data item.
func (r *cborReader) skip() ([]byte, error) {
	start := r.i
	var walk func() error
	walk = func() error {
		major, n, err := r.head()
		if err != nil {
			return err
		}
		switch major {
		case 0, 1, 7:
		case 2, 3:
			if uint64(len(r.b)-r.i) < n {
				return errors.New("truncated")
			}
			r.i += int(n)
		case 4:
			for k := uint64(0); k < n; k++ {
				if err := walk(); err != nil {
					return err
				}
			}
		case 5:
			for k := uint64(0); k < 2*n; k++ {
				if err := walk(); err != nil {
					return err
				}
			}
		case 6:
			return walk()
		}
		return nil
	}
	if err := walk(); err != nil {
		return nil, err
	}
	return r.b[start:r.i], nil
}

// DecodeWrapper decodes the exact profile and nothing else: no trailing bytes, array(2)
// [uint64, tag 39034 array(3)[1, bstr, UC item]], shortest-form integers.
func DecodeWrapper(b []byte) (block uint64, cert, uc []byte, err error) {
	r := &cborReader{b: b}
	if m, n, e := r.head(); e != nil || m != 4 || n != 2 {
		return 0, nil, nil, errors.New("not a two element array")
	}
	m, block, e := r.head()
	if e != nil || m != 0 {
		return 0, nil, nil, errors.New("block number is not an unsigned integer")
	}
	if m, n, e := r.head(); e != nil || m != 6 || n != ProofTag {
		return 0, nil, nil, errors.New("wrong tag")
	}
	if m, n, e := r.head(); e != nil || m != 4 || n != 3 {
		return 0, nil, nil, errors.New("proof is not a three element array")
	}
	if m, n, e := r.head(); e != nil || m != 0 || n != ProofVersion {
		return 0, nil, nil, errors.New("unsupported version")
	}
	m, n, e := r.head()
	if e != nil || m != 2 || uint64(len(b)-r.i) < n {
		return 0, nil, nil, errors.New("certificate is not a byte string")
	}
	cert = append([]byte{}, r.b[r.i:r.i+int(n)]...)
	r.i += int(n)
	uc, e = r.skip()
	if e != nil {
		return 0, nil, nil, e
	}
	if r.i != len(b) {
		return 0, nil, nil, errors.New("trailing bytes")
	}
	return block, cert, append([]byte(nil), uc...), nil
}

// ---- corpus generation ---------------------------------------------------------------------------------

type caseSpec struct {
	name, desc string
	leaves     []Leaf
	query      [KeySize]byte
	verdict    string
	// empty requests the certified empty tree (a timeout repeat of the initial empty record)
	empty bool
	// zeroRoot certifies the all-zero 32 byte hash (which is not an empty tree)
	zeroRoot bool
	// signers overrides the signing quorum
	signers []int
}

func k32(prefix ...byte) (k [KeySize]byte) { copy(k[:], prefix); return }

func v32(b byte) (v [ValueSize]byte) {
	for i := range v {
		v[i] = b
	}
	return
}

func hashKey(parts ...any) [KeySize]byte {
	h := sha256.New()
	for _, p := range parts {
		switch v := p.(type) {
		case string:
			h.Write([]byte(v))
		case int:
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(v))
			h.Write(b[:])
		}
	}
	var out [KeySize]byte
	h.Sum(out[:0])
	return out
}

// keyWithBit returns a key sharing the first d bits with base, with bit d flipped.
func keyDiffAt(base [KeySize]byte, d int, tag string) [KeySize]byte {
	k := hashKey("tail", tag, d)
	for i := 0; i < d; i++ {
		if bit(base[:], i) == 1 {
			k[i/8] |= 0x80 >> uint(i%8)
		} else {
			k[i/8] &^= 0x80 >> uint(i%8)
		}
	}
	if bit(base[:], d) == 1 {
		k[d/8] &^= 0x80 >> uint(d%8)
	} else {
		k[d/8] |= 0x80 >> uint(d%8)
	}
	return k
}

func caseSpecs() []caseSpec {
	var specs []caseSpec
	specs = append(specs, caseSpec{name: "empty-tree", verdict: VerdictAbsent, empty: true, query: hashKey("q-empty"),
		desc: "certified empty tree: empty certificate, UC with the empty input record hash"})
	two := []Leaf{{Key: k32(), Value: v32(1)}, {Key: k32(0x80), Value: v32(2)}}
	specs = append(specs, caseSpec{name: "golden-two-leaf", verdict: VerdictAbsent, leaves: two, query: k32(0x01),
		desc: "rugregator/Rust golden vector: leaves 00..00->01..01 and 80 00..00->02..02, query 01 00..00, root 5158a5f4...7a53"})
	specs = append(specs, caseSpec{name: "singleton", verdict: VerdictAbsent, leaves: []Leaf{{Key: hashKey("only"), Value: v32(7)}}, query: hashKey("other"),
		desc: "one leaf: the certificate is the bare terminal (zero bitmap)"})
	specs = append(specs, caseSpec{name: "singleton-included", verdict: VerdictStateInclude, leaves: []Leaf{{Key: hashKey("only"), Value: v32(7)}}, query: hashKey("only"),
		desc: "the query is the only leaf: the terminal is the query, state included"})
	three := []Leaf{{Key: k32(0x10), Value: v32(3)}, {Key: k32(0x18), Value: v32(4)}, {Key: k32(0xf0), Value: v32(5)}}
	specs = append(specs, caseSpec{name: "shared-prefix", verdict: VerdictAbsent, leaves: three, query: k32(0x11),
		desc: "keys sharing prefixes (junctions at depth 0 and 4): query follows the left subtree"})
	specs = append(specs, caseSpec{name: "shared-prefix-included", verdict: VerdictStateInclude, leaves: three, query: k32(0x18),
		desc: "the query is a leaf of the shared prefix tree"})
	specs = append(specs, caseSpec{name: "diverges-in-compressed-edge", verdict: VerdictAbsent, leaves: three, query: k32(0x20),
		desc: "the query leaves the key set inside the compressed edge below the root junction: the terminal is the first leaf of its side and the certificate still authenticates"})
	var d0, d255 = []Leaf{{Key: k32(), Value: v32(8)}, {Key: k32(0x80), Value: v32(9)}}, []Leaf{}
	base := hashKey("depth255")
	d255 = []Leaf{{Key: base, Value: v32(10)}, {Key: keyDiffAt(base, 255, "d255"), Value: v32(11)}}
	specs = append(specs, caseSpec{name: "depth-0", verdict: VerdictAbsent, leaves: d0, query: k32(0x7f, 0xff),
		desc: "junction at depth 0 (bit 0 of the bitmap)"})
	specs = append(specs, caseSpec{name: "depth-255", verdict: VerdictAbsent, leaves: d255, query: keyDiffAt(base, 254, "q254"),
		desc: "junction at depth 255, the last bit (bit 7 of bitmap byte 31); the query shares 254 bits with both leaves and leaves the compressed edge at bit 254"})
	// a chain: leaves diverging at every depth of a 256 deep path (all bitmap bits set)
	chain := []Leaf{}
	cb := hashKey("chain")
	chain = append(chain, Leaf{Key: cb, Value: v32(1)})
	for d := 0; d < 256; d++ {
		chain = append(chain, Leaf{Key: keyDiffAt(cb, d, "chain"), Value: v32(byte(d))})
	}
	qc := keyDiffAt(cb, 254, "chain-q")
	qc[31] ^= 0x01 // not the leaf which also differs from cb at bit 254 only
	specs = append(specs, caseSpec{name: "full-depth-chain", verdict: VerdictAbsent, leaves: chain, query: qc,
		desc: "257 leaves diverging from one path at every depth 0..255; the absent query leaves that path at depth 254: 255 junctions, 8256 byte certificate (the longest absent one)"})
	specs = append(specs, caseSpec{name: "full-depth-chain-included", verdict: VerdictStateInclude, leaves: chain, query: cb,
		desc: "the same tree queried at its deepest leaf: all 256 bitmap bits set, 256 siblings, the 8288 byte maximum certificate (the terminal is the query)"})
	rng := rand.New(rand.NewSource(20261016))
	for _, size := range []int{5, 16, 64} {
		var leaves []Leaf
		seen := map[[KeySize]byte]bool{}
		for len(leaves) < size {
			var l Leaf
			rng.Read(l.Key[:])
			rng.Read(l.Value[:])
			if !seen[l.Key] {
				seen[l.Key] = true
				leaves = append(leaves, l)
			}
		}
		var q [KeySize]byte
		rng.Read(q[:])
		specs = append(specs, caseSpec{name: fmt.Sprintf("random-%d", size), verdict: VerdictAbsent, leaves: leaves, query: q,
			desc: fmt.Sprintf("seeded random tree of %d leaves, random absent query (math/rand seed 20261016)", size)})
	}
	specs = append(specs,
		caseSpec{name: "zero-root", verdict: VerdictCertInvalid, zeroRoot: true, query: hashKey("q-zero"),
			desc: "the UC certifies the 32 byte all-zero hash: it is NOT an empty tree, the empty certificate does not verify against it"},
		caseSpec{name: "under-quorum", verdict: VerdictAuthority, leaves: two, query: k32(0x01), signers: []int{0, 1},
			desc: "valid structure, but the seal carries fewer signatures than the quorum"},
	)
	return specs
}

func hx(b []byte) string { return hex.EncodeToString(b) }

// Generate builds the complete corpus. commit is recorded in the manifest.
func Generate(aggregatorCommit string) (Files, error) {
	files := Files{}
	prod, err := ucfixture.New(ucfixture.Config{Partition: Partition, ShardBits: 0})
	if err != nil {
		return nil, err
	}
	tb := prod.TrustBase()
	tbBytes, err := prod.TrustBaseCBOR()
	if err != nil {
		return nil, err
	}
	files["authority/trust-base.cbor"] = tbBytes

	// other authorities (reused node IDs, other networks/epochs/keys)
	mkTB := func(network types.NetworkID, keys func(i int) abcrypto.Signer, opts ...types.Option) (*types.RootTrustBaseV1, []byte, error) {
		var nodes []*types.NodeInfo
		for i := 0; i < ucfixture.RootNodes; i++ {
			v, err := keys(i).Verifier()
			if err != nil {
				return nil, nil, err
			}
			pk, err := v.MarshalPublicKey()
			if err != nil {
				return nil, nil, err
			}
			nodes = append(nodes, &types.NodeInfo{NodeID: ucfixture.RootNodeID(i), SigKey: pk, Stake: 1})
		}
		t, err := types.NewTrustBase(network, nodes, opts...)
		if err != nil {
			return nil, nil, err
		}
		b, err := t.MarshalCBOR()
		return t, b, err
	}
	fixtureKey := func(i int) abcrypto.Signer {
		k := sha256.Sum256([]byte(fmt.Sprintf("uc-consolidation fixture key %s %d", "root", i)))
		s, _ := abcrypto.NewInMemorySecp256K1SignerFromKey(k[:])
		return s
	}
	otherKey := func(i int) abcrypto.Signer {
		k := sha256.Sum256([]byte(fmt.Sprintf("non-inclusion corpus untrusted authority %d", i)))
		s, _ := abcrypto.NewInMemorySecp256K1SignerFromKey(k[:])
		return s
	}
	type tbSpec struct {
		file string
		tb   types.RootTrustBase
	}
	tbs := map[string]types.RootTrustBase{"authority/trust-base.cbor": tb}
	addTB := func(file string, network types.NetworkID, keys func(int) abcrypto.Signer, opts ...types.Option) error {
		t, b, err := mkTB(network, keys, opts...)
		if err != nil {
			return err
		}
		files[file] = b
		tbs[file] = t
		return nil
	}
	if err := addTB("authority/other-network.cbor", types.NetworkMainNet, fixtureKey, types.WithEpochStart(1)); err != nil {
		return nil, err
	}
	if err := addTB("authority/other-epoch.cbor", ucfixture.NetworkID, fixtureKey, types.WithEpoch(tb.GetEpoch()+1), types.WithEpochStart(1)); err != nil {
		return nil, err
	}
	if err := addTB("authority/late-epoch-start.cbor", ucfixture.NetworkID, fixtureKey, types.WithEpochStart(1_000_000)); err != nil {
		return nil, err
	}
	if err := addTB("authority/untrusted-keys.cbor", ucfixture.NetworkID, otherKey, types.WithEpochStart(1)); err != nil {
		return nil, err
	}

	// ---- positive cases and the UC chain
	round, ts := uint64(1), ucfixture.GenesisTimestamp
	var cases []Vector
	type built struct {
		spec   caseSpec
		vec    Vector
		uc     *types.UnicityCertificate
		tree   *Tree
		ucByte []byte
	}
	byName := map[string]*built{}
	for i, spec := range caseSpecs() {
		tree, err := Build(spec.leaves)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", spec.name, err)
		}
		round++
		ts += 10
		bs := ucfixture.BlockSpec{Round: round, Timestamp: ts, Signers: spec.signers}
		var rootHex string
		var cert []byte
		switch {
		case spec.empty:
			bs.Changes = []ucfixture.Change{{Shard: 0, Timeout: true}}
			cert = []byte{}
		case spec.zeroRoot:
			bs.Changes = []ucfixture.Change{{Shard: 0, Hash: make([]byte, 32)}}
			rootHex = hx(make([]byte, 32))
			cert = []byte{}
		default:
			r, _ := tree.Root()
			bs.Changes = []ucfixture.Change{{Shard: 0, Hash: r[:]}}
			rootHex = hx(r[:])
			cert = tree.Certificate(spec.query)
		}
		blk, err := prod.Next(bs)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", spec.name, err)
		}
		uc := blk.Shards[0].UC
		if spec.empty && len(uc.InputRecord.Hash) != 0 {
			return nil, fmt.Errorf("%s: empty record has hash %x", spec.name, uc.InputRecord.Hash)
		}
		v := Vector{
			Name: spec.name, Description: spec.desc, Query: hx(spec.query[:]), Root: rootHex,
			Block: fmt.Sprint(100 + i), Certificate: hx(cert), UC: hx(blk.Shards[0].UCBytes),
			Context: "default", Expected: Expected{Verdict: spec.verdict},
		}
		for _, l := range spec.leaves {
			v.Leaves = append(v.Leaves, LeafJSON{Key: hx(l.Key[:]), Value: hx(l.Value[:])})
		}
		if v.Leaves == nil {
			v.Leaves = []LeafJSON{}
		}
		v.Wrapper = hx(EncodeWrapper(uint64(100+i), cert, blk.Shards[0].UCBytes))
		byName[spec.name] = &built{spec: spec, vec: v, uc: uc, tree: tree, ucByte: blk.Shards[0].UCBytes}
		cases = append(cases, v)
	}

	// contexts
	last := cases[len(cases)-1]
	_ = last
	confHash := byName["golden-two-leaf"].uc.ShardConfHash
	contexts := map[string]Context{
		"default": {Description: "trusted authority of the fixture network, the corpus partition, no end boundary, no freshness requirement",
			TrustBase: "authority/trust-base.cbor", Partition: uint32(Partition), NextEpochStart: "0", MinRootRound: "0"},
		"wrong-network": {Description: "authority of another network with the same keys and node IDs",
			TrustBase: "authority/other-network.cbor", Partition: uint32(Partition), NextEpochStart: "0", MinRootRound: "0"},
		"wrong-epoch": {Description: "authority of the next epoch with the same keys: seals of epoch 1 must not verify",
			TrustBase: "authority/other-epoch.cbor", Partition: uint32(Partition), NextEpochStart: "0", MinRootRound: "0"},
		"before-epoch-start": {Description: "authority whose epoch starts after the certified round",
			TrustBase: "authority/late-epoch-start.cbor", Partition: uint32(Partition), NextEpochStart: "0", MinRootRound: "0"},
		"untrusted-authority": {Description: "a different authority (other keys, same network and node IDs)",
			TrustBase: "authority/untrusted-keys.cbor", Partition: uint32(Partition), NextEpochStart: "0", MinRootRound: "0"},
		"wrong-partition": {Description: "expects another application partition",
			TrustBase: "authority/trust-base.cbor", Partition: uint32(Partition) + 1, NextEpochStart: "0", MinRootRound: "0"},
		"config-pin-ok": {Description: "pins the shard configuration hash of the corpus partition",
			TrustBase: "authority/trust-base.cbor", Partition: uint32(Partition), NextEpochStart: "0", MinRootRound: "0", ShardConfHash: hx(confHash)},
		"config-pin-other": {Description: "pins another shard configuration hash",
			TrustBase: "authority/trust-base.cbor", Partition: uint32(Partition), NextEpochStart: "0", MinRootRound: "0", ShardConfHash: hx(bytes.Repeat([]byte{0xcc}, 32))},
	}
	// retirement boundary relative to the base case round
	g := byName["shared-prefix"]
	gr := g.uc.UnicitySeal.RootChainRoundNumber
	contexts["epoch-end-after"] = Context{Description: "authenticated end of the epoch is after the certified round (still valid)",
		TrustBase: "authority/trust-base.cbor", Partition: uint32(Partition), NextEpochStart: fmt.Sprint(gr + 1), MinRootRound: "0"}
	contexts["epoch-end-at-round"] = Context{Description: "the next epoch starts at the certified round: the seal is at the retirement boundary and not valid",
		TrustBase: "authority/trust-base.cbor", Partition: uint32(Partition), NextEpochStart: fmt.Sprint(gr), MinRootRound: "0"}
	contexts["fresh-enough"] = Context{Description: "requires a root round not above the certified one",
		TrustBase: "authority/trust-base.cbor", Partition: uint32(Partition), NextEpochStart: "0", MinRootRound: fmt.Sprint(gr)}
	contexts["too-stale"] = Context{Description: "requires a root round above the certified one",
		TrustBase: "authority/trust-base.cbor", Partition: uint32(Partition), NextEpochStart: "0", MinRootRound: fmt.Sprint(gr + 1)}

	ctxNames := make([]string, 0, len(contexts))
	for n := range contexts {
		ctxNames = append(ctxNames, n)
	}
	sort.Strings(ctxNames)
	cb, err := json.MarshalIndent(contexts, "", "  ")
	if err != nil {
		return nil, err
	}
	files["contexts.json"] = append(cb, '\n')

	verdictOf := func(v Vector) (string, error) {
		w, err := hex.DecodeString(v.Wrapper)
		if err != nil {
			return "", err
		}
		q, err := hex.DecodeString(v.Query)
		if err != nil || len(q) != KeySize {
			return "", errors.New("bad query")
		}
		c := contexts[v.Context]
		return Verdict(w, [KeySize]byte(q), c, tbs[c.TrustBase]), nil
	}
	put := func(dir string, v Vector) error {
		got, err := verdictOf(v)
		if err != nil {
			return fmt.Errorf("%s: %w", v.Name, err)
		}
		if got != v.Expected.Verdict {
			return fmt.Errorf("%s: reference verdict %s, intended %s", v.Name, got, v.Expected.Verdict)
		}
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return err
		}
		files[dir+"/"+v.Name+".json"] = append(b, '\n')
		return nil
	}
	for _, c := range cases {
		if err := put("cases", c); err != nil {
			return nil, err
		}
	}

	// ---- negatives
	negs, err := negatives(byName["shared-prefix"].vec, byName["golden-two-leaf"].vec, byName["empty-tree"].vec, byName["zero-root"].vec, byName["full-depth-chain"].vec)
	if err != nil {
		return nil, err
	}
	for _, n := range negs {
		if err := put("negative", n); err != nil {
			return nil, err
		}
	}
	// context variants of a positive case: the same wrapper under other trust
	for _, cv := range []struct{ ctx, verdict, desc string }{
		{"wrong-network", VerdictAuthority, "the authority is of another network (reused keys and node IDs)"},
		{"wrong-epoch", VerdictAuthority, "the authority is of another epoch (reused keys): exact epoch is required"},
		{"before-epoch-start", VerdictAuthority, "the certified round precedes the epoch start of the trusted authority"},
		{"untrusted-authority", VerdictAuthority, "a self-consistent but untrusted authority: not an anchor"},
		{"wrong-partition", VerdictUCInvalid, "the UC is of another partition than the application expects"},
		{"config-pin-other", VerdictUCInvalid, "the shard configuration hash differs from the pinned one"},
		{"config-pin-ok", VerdictAbsent, "the shard configuration hash equals the pinned one"},
		{"epoch-end-at-round", VerdictAuthority, "the seal round is at the authenticated end of its epoch (retirement boundary)"},
		{"epoch-end-after", VerdictAbsent, "the authenticated end of the epoch is after the seal round"},
		{"too-stale", VerdictStale, "authenticated but older than the minimum root round the caller requires: unavailable/stale, never accepted as current"},
		{"fresh-enough", VerdictAbsent, "the certified round equals the required minimum"},
	} {
		v := g.vec
		v.Name = "ctx-" + cv.ctx
		v.Base = g.vec.Name
		v.Description = cv.desc
		v.Context = cv.ctx
		v.Expected = Expected{Verdict: cv.verdict}
		if err := put("negative", v); err != nil {
			return nil, err
		}
	}
	// UC mutations
	ucNegs, err := ucNegatives(g.vec, tb, prod, contexts, tbs)
	if err != nil {
		return nil, err
	}
	for _, n := range ucNegs {
		if err := put("negative", n); err != nil {
			return nil, err
		}
	}

	// direct-unsharded: tree, UC and raw inclusion certificates for consolidation fixtures (no sidecar)
	if err := directUnsharded(files, byName["random-16"].tree, byName["random-16"].spec.leaves, byName["random-16"].vec); err != nil {
		return nil, err
	}
	files["rpc-errors.json"] = rpcErrors()
	files["status-hints.json"] = statusHints()
	files["SPEC.md"] = []byte(specText)

	m, err := manifest(files, aggregatorCommit)
	if err != nil {
		return nil, err
	}
	files["manifest.json"] = m
	return files, nil
}

func manifest(files Files, commit string) ([]byte, error) {
	type entry struct {
		Name     string `json:"name"`
		Kind     string `json:"kind"`
		Verdict  string `json:"verdict,omitempty"`
		Context  string `json:"context,omitempty"`
		Describe string `json:"description,omitempty"`
	}
	var cases []entry
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		kind := ""
		switch {
		case strings.HasPrefix(n, "cases/"):
			kind = "positive"
		case strings.HasPrefix(n, "negative/"):
			kind = "negative"
		default:
			continue
		}
		var v Vector
		if err := json.Unmarshal(files[n], &v); err != nil {
			return nil, err
		}
		cases = append(cases, entry{Name: strings.TrimSuffix(strings.SplitN(n, "/", 2)[1], ".json"), Kind: kind, Verdict: v.Expected.Verdict, Context: v.Context, Describe: v.Description})
	}
	sums := map[string]string{}
	for _, n := range names {
		h := sha256.Sum256(files[n])
		sums[n] = hx(h[:])
	}
	m := map[string]any{
		"corpus": "non-inclusion", "version": Version,
		"generator": map[string]string{
			"command": "UC_FIXTURE_AGGREGATOR_COMMIT=$(git rev-parse HEAD) go run ./cmd/gen-ni-fixtures -out testdata/non-inclusion/v1",
			"commit":  commit,
		},
		"limits": map[string]uint64{"maxCertificateBytes": MaxCertificateBytes, "maxRequestBytes": 64 << 10, "maxResponseBytes": 8 << 20},
		"cases":  cases,
		"sha256": sums,
	}
	b, err := json.MarshalIndent(m, "", "  ")
	return append(b, '\n'), err
}
