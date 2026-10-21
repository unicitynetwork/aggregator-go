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
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/mod/modfile"

	"github.com/unicitynetwork/bft-core/rootchain/testutils/ucfixture"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"
)

// Version of the corpus layout (schema 2: snapshot results, fixed partition, no freshness verdict).
const Version = 2

// Corpus constants.
const (
	// Partition is the application partition of every accepted UC of the corpus: the fixed aggregation
	// partition (api.AggregationPartitionID, asserted equal in the tests; this model shares no code with pkg/api).
	Partition types.PartitionID = 7
	// OtherPartition is the partition of the validly signed wrong-partition vectors.
	OtherPartition types.PartitionID = 8
	// ProofTag and ProofVersion identify the non-inclusion proof wrapper.
	ProofTag     = 39034
	ProofVersion = 1
)

// Classification of the outcome of verifying one wrapper under one context (checked in this order). The first
// two are the successful outcomes and carry the authenticated snapshot; the others are rejections, which never
// carry snapshot metadata. VerdictAbsent and VerdictStateInclude are the internal names of the two outcomes
// OutcomeAbsent and OutcomeIncluded.
const (
	VerdictAbsent       = "absent"              // verified: the state is absent at the certified snapshot
	VerdictMalformed    = "malformed"           // wrapper or certificate does not decode in the exact profile
	VerdictUCInvalid    = "uc_invalid"          // the UC body is invalid: structure, paths to the seal, partition, shard, configuration pin
	VerdictAuthority    = "authority_rejected"  // the seal is not authenticated by the trusted authority/history
	VerdictCertInvalid  = "certificate_invalid" // the certificate does not authenticate a terminal leaf under the UC root for this query
	VerdictStateInclude = "state_included"      // the authenticated terminal is the query itself: the state is present at the snapshot
)

// Outcomes of the expectations in the corpus files.
const (
	OutcomeAbsent   = "absent_at_snapshot"
	OutcomeIncluded = "included_at_snapshot"
	OutcomeRejected = "rejected"
)

// File is the content of one corpus file by relative path.
type Files map[string][]byte

// Context is a named verification context: everything the verifier trusts. The partition is not part of it: it is
// the fixed Partition.
type Context struct {
	Description string `json:"description"`
	// TrustBase is the corpus path of the CBOR encoded trust base (authority); TrustBaseJSON is the
	// same authority in the JSON form the SDKs read (RootTrustBase.from_json / fromJSON).
	TrustBase     string `json:"trustBase"`
	TrustBaseJSON string `json:"trustBaseJson"`
	// Shard is the expected shard of the partition as a bit string ("" is the unsharded partition, "01" the
	// shard whose keys start with the bits 01).
	Shard string `json:"shard"`
	// NextEpochStart is the authenticated end boundary of the epoch (first round of
	// the next epoch); "0" when the history does not provide it.
	NextEpochStart string `json:"nextEpochStart"`
	// ShardConfHash optionally pins the shard configuration hash (hex, "" none).
	ShardConfHash string `json:"shardConfHash"`
}

// Snapshot is the authenticated certified state a successful verification returns. Every field comes from the
// authenticated unicity certificate (seal and input record), never from RPC metadata. The uint64 fields are
// decimal strings (JS bigint safe); StateRoot is null for the certified empty tree.
type Snapshot struct {
	NetworkID            uint32  `json:"networkId"`
	PartitionID          uint32  `json:"partitionId"`
	ShardID              string  `json:"shardId"`
	RootRound            string  `json:"rootRound"`
	SealTimestampSeconds string  `json:"sealTimestampSeconds"`
	RootEpoch            string  `json:"rootEpoch"`
	ShardRound           string  `json:"shardRound"`
	ShardEpoch           string  `json:"shardEpoch"`
	StateRoot            *string `json:"stateRoot"`
}

// Expected is the expected result of a vector: an outcome with the queried state and the snapshot, or a
// rejection class without any snapshot.
type Expected struct {
	// Verdict is the internal classification the generator intends (not written to the files).
	Verdict string `json:"-"`
	// Outcome is absent_at_snapshot, included_at_snapshot or rejected.
	Outcome string `json:"outcome"`
	// StateID and Snapshot are present exactly for the two successful outcomes.
	StateID  string    `json:"stateId,omitempty"`
	Snapshot *Snapshot `json:"snapshot,omitempty"`
	// Rejection is the class of a rejection.
	Rejection string `json:"rejection,omitempty"`
	// Acceptable lists other REJECTION classes a conformant strict implementation may report
	// for this vector (decoders of different strictness stop at different stages). The two
	// successful outcomes must always match exactly.
	Acceptable []string `json:"acceptable,omitempty"`
	Note       string   `json:"note,omitempty"`
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

// ShardFromBits builds the shard ID of a bit string ("" is the unsharded partition).
func ShardFromBits(bits string) (types.ShardID, error) {
	id := types.ShardID{}
	for _, c := range bits {
		a, b := id.Split()
		switch c {
		case '0':
			id = a
		case '1':
			id = b
		default:
			return types.ShardID{}, fmt.Errorf("shard bit string %q", bits)
		}
	}
	return id, nil
}

// inShard reports whether the key belongs to the shard of the bit string (independent of the library comparator).
func inShard(shardBits string, key []byte) bool {
	for i, c := range shardBits {
		if bit(key, i) != int(c-'0') {
			return false
		}
	}
	return true
}

func dec(n uint64) string { return fmt.Sprint(n) }

// SnapshotOf is the snapshot of an authenticated UC.
func SnapshotOf(uc *types.UnicityCertificate) *Snapshot {
	s := &Snapshot{
		NetworkID:            uint32(uc.UnicitySeal.NetworkID),
		PartitionID:          uint32(uc.UnicityTreeCertificate.Partition),
		ShardID:              uc.ShardTreeCertificate.Shard.String(),
		RootRound:            dec(uc.UnicitySeal.RootChainRoundNumber),
		SealTimestampSeconds: dec(uc.UnicitySeal.Timestamp),
		RootEpoch:            dec(uc.UnicitySeal.Epoch),
		ShardRound:           dec(uc.InputRecord.RoundNumber),
		ShardEpoch:           dec(uc.InputRecord.Epoch),
	}
	if len(uc.InputRecord.Hash) != 0 {
		h := hx(uc.InputRecord.Hash)
		s.StateRoot = &h
	}
	return s
}

// Verdict is the reference classification of wrapper under ctx for query. It is the
// executable specification of SPEC.md "Verification order"; production verifiers must agree.
// The snapshot is returned for the two successful verdicts only, after everything is authenticated.
func Verdict(wrapper []byte, query [KeySize]byte, ctx Context, trustBase types.RootTrustBase) (string, *Snapshot) {
	_, cert, ucRaw, err := DecodeWrapper(wrapper)
	if err != nil {
		return VerdictMalformed, nil
	}
	if len(cert) > MaxCertificateBytes {
		return VerdictMalformed, nil
	}
	var uc types.UnicityCertificate
	if err := types.Cbor.Unmarshal(ucRaw, &uc); err != nil {
		return VerdictMalformed, nil
	}
	if re, err := uc.MarshalCBOR(); err != nil || !bytes.Equal(re, ucRaw) {
		return VerdictMalformed, nil
	}
	if len(cert) > 0 {
		if len(cert) < 32+KeySize+ValueSize || (len(cert)-32-KeySize-ValueSize)%32 != 0 {
			return VerdictMalformed, nil
		}
		pop := 0
		for _, b := range cert[:32] {
			for ; b != 0; b &= b - 1 {
				pop++
			}
		}
		if len(cert) != 32+32*pop+KeySize+ValueSize {
			return VerdictMalformed, nil
		}
	}
	if err := refUCBody(&uc, ctx); err != nil {
		return VerdictUCInvalid, nil
	}
	var next uint64
	fmt.Sscan(ctx.NextEpochStart, &next)
	var verifier types.SealVerifier = types.TrustBaseSealVerifier(trustBase)
	if next != 0 {
		verifier = types.TrustBaseSealVerifierUntil(trustBase, next)
	}
	if err := verifier.VerifySeal(uc.UnicitySeal); err != nil {
		return VerdictAuthority, nil
	}
	var root *[HashSize]byte
	switch h := uc.InputRecord.Hash; {
	case len(h) == 0:
	case len(h) == HashSize:
		r := [HashSize]byte(h)
		root = &r
	default:
		return VerdictCertInvalid, nil
	}
	if ctx.Shard != "" {
		// the query and the authenticated terminal leaf must belong to the certified shard
		if !inShard(ctx.Shard, query[:]) {
			return VerdictCertInvalid, nil
		}
		if len(cert) > 0 {
			tk := cert[len(cert)-KeySize-ValueSize : len(cert)-ValueSize]
			if !inShard(ctx.Shard, tk) {
				return VerdictCertInvalid, nil
			}
		}
	}
	switch err := Verify(cert, query, root); {
	case err == nil:
		return VerdictAbsent, SnapshotOf(&uc)
	case errors.Is(err, ErrStateIncluded):
		return VerdictStateInclude, SnapshotOf(&uc)
	case errors.Is(err, ErrMalformed):
		return VerdictMalformed, nil
	default:
		return VerdictCertInvalid, nil
	}
}

// expectedOf is the Expected of a verdict (and snapshot) for the query.
func expectedOf(verdict string, snap *Snapshot, query string, acceptable []string) Expected {
	e := Expected{Verdict: verdict, Acceptable: acceptable}
	switch verdict {
	case VerdictAbsent:
		e.Outcome, e.StateID, e.Snapshot = OutcomeAbsent, query, snap
	case VerdictStateInclude:
		e.Outcome, e.StateID, e.Snapshot = OutcomeIncluded, query, snap
	default:
		e.Outcome, e.Rejection = OutcomeRejected, verdict
	}
	return e
}

// refUCBody is the body check of a UC: components present, structure valid for the fixed partition and the
// context's shard, shard and partition paths reach the seal hash. The empty shard ID with an empty shard path is
// the unsharded case; the partition path may be nonempty.
func refUCBody(uc *types.UnicityCertificate, ctx Context) error {
	if uc.InputRecord == nil || uc.UnicitySeal == nil {
		return errors.New("missing component")
	}
	if uc.InputRecord.Hash != nil && len(uc.InputRecord.Hash) == 0 {
		return errors.New("an empty byte string is not the empty-tree root (native null is)")
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
	shard, err := ShardFromBits(ctx.Shard)
	if err != nil {
		return err
	}
	if err := uc.IsValid(Partition, shard, pin); err != nil {
		return err
	}
	if !uc.ShardTreeCertificate.Shard.Equal(shard) {
		return errors.New("a certificate of another shard")
	}
	if shard.Length() == 0 && len(uc.ShardTreeCertificate.SiblingHashes) != 0 {
		return errors.New("the empty shard ID carries a shard path")
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
	// shard is the index (canonical order) of the shard of the 4-shard partition which certifies the state;
	// sharded selects that partition (the others use the unsharded one)
	sharded bool
	shard   int
	// ctx is the verification context ("" is default)
	ctx string
	// negative places the vector under negative/ (a rejection built from a tree)
	negative bool
}

// shardKey is a key of the shard with the given index (2 prefix bits): the top byte carries the bits, the rest is
// free to vary.
func shardKey(shard int, rest ...byte) (k [KeySize]byte) {
	k[0] = byte(shard) << 6
	if len(rest) > 0 {
		k[0] |= rest[0] & 0x3f
	}
	if len(rest) > 1 {
		copy(k[1:], rest[1:])
	}
	return k
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
	// historical snapshots: absence at an old snapshot is a valid, authenticated statement even if the state is
	// included later. The caller decides with its own policy which snapshots it accepts (policy.json).
	histLeaves := []Leaf{{Key: hashKey("hist", 1), Value: v32(21)}, {Key: hashKey("hist", 2), Value: v32(22)}}
	histQuery := hashKey("hist", "query")
	specs = append(specs, caseSpec{name: "historical-snapshot-absent", verdict: VerdictAbsent, leaves: histLeaves, query: histQuery,
		desc: "an older snapshot in which the queried state is absent; verifies, and its snapshot says how old it is"})
	specs = append(specs, caseSpec{name: "later-snapshot-included", verdict: VerdictStateInclude, leaves: append(append([]Leaf(nil), histLeaves...), Leaf{Key: histQuery, Value: v32(23)}), query: histQuery,
		desc: "a later snapshot of the same lineage in which the queried state of historical-snapshot-absent is included"})
	// sharded partition of four shards (prefix bits 00, 01, 10, 11)
	sh := func(i int, rest ...byte) [KeySize]byte { return shardKey(i, rest...) }
	specs = append(specs,
		caseSpec{name: "sharded-absent", verdict: VerdictAbsent, sharded: true, shard: 0, ctx: "shard-00",
			leaves: []Leaf{{Key: sh(0, 0x10, 0x00), Value: v32(31)}, {Key: sh(0, 0x10, 0x80), Value: v32(32)}}, query: sh(0, 0x10, 0x01),
			desc: "shard 00 of a four shard partition: two leaves which differ well below the shard prefix; the shard path of the UC has two siblings"},
		caseSpec{name: "sharded-included", verdict: VerdictStateInclude, sharded: true, shard: 0, ctx: "shard-00",
			leaves: []Leaf{{Key: sh(0, 0x10, 0x00), Value: v32(31)}, {Key: sh(0, 0x10, 0x80), Value: v32(32)}}, query: sh(0, 0x10, 0x80),
			desc: "shard 00: the query is a leaf of the shard tree"},
		caseSpec{name: "sharded-empty-tree", verdict: VerdictAbsent, sharded: true, shard: 1, ctx: "shard-01", empty: true, query: sh(1, 0x11),
			desc: "shard 01 certified as the empty tree (timeout repeat of its initial record): the state root is null and the shard path is still present"},
		caseSpec{name: "sharded-shard-01-absent", verdict: VerdictAbsent, sharded: true, shard: 1, ctx: "shard-01",
			leaves: []Leaf{{Key: sh(1, 0x05, 0x00), Value: v32(33)}, {Key: sh(1, 0x05, 0xc0), Value: v32(34)}, {Key: sh(1, 0x2a), Value: v32(35)}}, query: sh(1, 0x05, 0x40),
			desc: "shard 01: a three leaf tree whose junctions lie below the shard prefix"},
		caseSpec{name: "sharded-shard-11-singleton", verdict: VerdictAbsent, sharded: true, shard: 3, ctx: "shard-11",
			leaves: []Leaf{{Key: sh(3, 0x3f, 0xff), Value: v32(36)}}, query: sh(3, 0x01),
			desc: "shard 11: one leaf, the certificate is the bare terminal"},
		caseSpec{name: "sharded-query-outside-shard", verdict: VerdictCertInvalid, sharded: true, shard: 0, ctx: "shard-00", negative: true,
			leaves: []Leaf{{Key: sh(0, 0x10, 0x00), Value: v32(31)}, {Key: sh(0, 0x10, 0x80), Value: v32(32)}}, query: sh(3, 0x10, 0x01),
			desc: "the query belongs to shard 11, the certificate and UC are of shard 00: the junction bits fold exactly like an in-shard query, only the shard membership rule rejects it"},
		caseSpec{name: "sharded-terminal-outside-shard", verdict: VerdictCertInvalid, sharded: true, shard: 0, ctx: "shard-00", negative: true,
			leaves: []Leaf{{Key: sh(3, 0x10, 0x00), Value: v32(37)}, {Key: sh(3, 0x10, 0x80), Value: v32(38)}}, query: sh(0, 0x10, 0x01),
			desc: "the shard 00 tree holds foreign leaves: the in-shard query reaches a terminal of shard 11 which the shard does not own"},
	)
	rngS := rand.New(rand.NewSource(20261018))
	{
		var leaves []Leaf
		seen := map[[KeySize]byte]bool{}
		for len(leaves) < 16 {
			var l Leaf
			rngS.Read(l.Key[:])
			rngS.Read(l.Value[:])
			l.Key[0] = l.Key[0]&0x3f | 2<<6 // shard 10
			if !seen[l.Key] {
				seen[l.Key] = true
				leaves = append(leaves, l)
			}
		}
		var q [KeySize]byte
		rngS.Read(q[:])
		q[0] = q[0]&0x3f | 2<<6
		specs = append(specs, caseSpec{name: "sharded-random-16", verdict: VerdictAbsent, sharded: true, shard: 2, ctx: "shard-10", leaves: leaves, query: q,
			desc: "seeded random tree of 16 leaves in shard 10, random absent query of the shard (math/rand seed 20261018)"})
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

type clock struct{ round, ts uint64 }

// Generate builds the complete corpus. commit is recorded in the manifest.
func Generate(aggregatorCommit string) (Files, error) {
	files := Files{}
	newProd := func(partition types.PartitionID, bits int) (*ucfixture.Producer, error) {
		return ucfixture.New(ucfixture.Config{Partition: partition, ShardBits: bits})
	}
	prod, err := newProd(Partition, 0)
	if err != nil {
		return nil, err
	}
	prodS, err := newProd(Partition, 2)
	if err != nil {
		return nil, err
	}
	prodOther, err := newProd(OtherPartition, 0)
	if err != nil {
		return nil, err
	}
	prodOtherS, err := newProd(OtherPartition, 2)
	if err != nil {
		return nil, err
	}
	tb := prod.TrustBase()
	tbBytes, err := prod.TrustBaseCBOR()
	if err != nil {
		return nil, err
	}
	for _, p := range []*ucfixture.Producer{prodS, prodOther, prodOtherS} {
		b, err := p.TrustBaseCBOR()
		if err != nil || !bytes.Equal(b, tbBytes) {
			return nil, errors.New("the fixture producers do not share one authority")
		}
	}
	clocks := map[*ucfixture.Producer]*clock{}
	for _, p := range []*ucfixture.Producer{prod, prodS, prodOther, prodOtherS} {
		clocks[p] = &clock{round: 1, ts: ucfixture.GenesisTimestamp}
	}
	nextBlock := func(p *ucfixture.Producer, signers []int, changes ...ucfixture.Change) (*ucfixture.Block, error) {
		c := clocks[p]
		c.round++
		c.ts += 10
		return p.Next(ucfixture.BlockSpec{Round: c.round, Timestamp: c.ts, Changes: changes, Signers: signers})
	}

	files["authority/trust-base.cbor"] = tbBytes
	jsonOf := func(file string, t *types.RootTrustBaseV1) error {
		b, err := json.MarshalIndent(t, "", "  ")
		if err != nil {
			return err
		}
		files[strings.TrimSuffix(file, ".cbor")+".json"] = append(b, '\n')
		return nil
	}
	if err := jsonOf("authority/trust-base.cbor", tb); err != nil {
		return nil, err
	}

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
	tbs := map[string]types.RootTrustBase{"authority/trust-base.cbor": tb}
	addTB := func(file string, network types.NetworkID, keys func(int) abcrypto.Signer, opts ...types.Option) error {
		t, b, err := mkTB(network, keys, opts...)
		if err != nil {
			return err
		}
		files[file] = b
		tbs[file] = t
		return jsonOf(file, t)
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
	if err := addTB("authority/big-epoch.cbor", ucfixture.NetworkID, fixtureKey, types.WithEpoch(bigRootEpoch), types.WithEpochStart(1)); err != nil {
		return nil, err
	}

	// ---- positive cases and the UC chain
	var cases []Vector
	type built struct {
		spec   caseSpec
		vec    Vector
		uc     *types.UnicityCertificate
		tree   *Tree
		ucByte []byte
	}
	byName := map[string]*built{}
	vectorOf := func(spec caseSpec, i int, tree *Tree, uc *types.UnicityCertificate, ucBytes []byte) (Vector, []byte) {
		var rootHex string
		var cert []byte
		switch {
		case spec.empty:
			cert = []byte{}
		case spec.zeroRoot:
			rootHex = hx(make([]byte, 32))
			cert = []byte{}
		default:
			r, _ := tree.Root()
			rootHex = hx(r[:])
			cert = tree.Certificate(spec.query)
		}
		ctx := spec.ctx
		if ctx == "" {
			ctx = "default"
		}
		v := Vector{
			Name: spec.name, Description: spec.desc, Query: hx(spec.query[:]), Root: rootHex,
			Block: fmt.Sprint(100 + i), Certificate: hx(cert), UC: hx(ucBytes),
			Context: ctx, Expected: Expected{Verdict: spec.verdict},
		}
		for _, l := range spec.leaves {
			v.Leaves = append(v.Leaves, LeafJSON{Key: hx(l.Key[:]), Value: hx(l.Value[:])})
		}
		if v.Leaves == nil {
			v.Leaves = []LeafJSON{}
		}
		v.Wrapper = hx(EncodeWrapper(uint64(100+i), cert, ucBytes))
		return v, cert
	}
	var negSpecs []Vector
	nSpecs := 0
	for i, spec := range caseSpecs() {
		nSpecs++
		tree, err := Build(spec.leaves)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", spec.name, err)
		}
		p, idx := prod, 0
		if spec.sharded {
			p, idx = prodS, spec.shard
		}
		var ch ucfixture.Change
		switch {
		case spec.empty:
			ch = ucfixture.Change{Shard: idx, Timeout: true}
		case spec.zeroRoot:
			ch = ucfixture.Change{Shard: idx, Hash: make([]byte, 32)}
		default:
			r, _ := tree.Root()
			ch = ucfixture.Change{Shard: idx, Hash: r[:]}
		}
		blk, err := nextBlock(p, spec.signers, ch)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", spec.name, err)
		}
		uc := blk.Shards[idx].UC
		if spec.empty && len(uc.InputRecord.Hash) != 0 {
			return nil, fmt.Errorf("%s: empty record has hash %x", spec.name, uc.InputRecord.Hash)
		}
		v, _ := vectorOf(spec, i, tree, uc, blk.Shards[idx].UCBytes)
		byName[spec.name] = &built{spec: spec, vec: v, uc: uc, tree: tree, ucByte: blk.Shards[idx].UCBytes}
		if spec.negative {
			negSpecs = append(negSpecs, v)
		} else {
			cases = append(cases, v)
		}
	}

	// two shards certified by one block share a seal: verifying one must never vouch for the other
	{
		la := []Leaf{{Key: shardKey(0, 0x20, 0x00), Value: v32(41)}, {Key: shardKey(0, 0x20, 0x80), Value: v32(42)}}
		lb := []Leaf{{Key: shardKey(1, 0x20, 0x00), Value: v32(43)}, {Key: shardKey(1, 0x20, 0x80), Value: v32(44)}}
		ta, err := Build(la)
		if err != nil {
			return nil, err
		}
		tbb, err := Build(lb)
		if err != nil {
			return nil, err
		}
		ra, _ := ta.Root()
		rb, _ := tbb.Root()
		blk, err := nextBlock(prodS, nil, ucfixture.Change{Shard: 0, Hash: ra[:]}, ucfixture.Change{Shard: 1, Hash: rb[:]})
		if err != nil {
			return nil, err
		}
		for k, sp := range []caseSpec{
			{name: "sharded-same-seal-00", verdict: VerdictAbsent, sharded: true, shard: 0, ctx: "shard-00", leaves: la, query: shardKey(0, 0x20, 0x01),
				desc: "shard 00, certified in the same block (same seal) as sharded-same-seal-01"},
			{name: "sharded-same-seal-01", verdict: VerdictAbsent, sharded: true, shard: 1, ctx: "shard-01", leaves: lb, query: shardKey(1, 0x20, 0x01),
				desc: "shard 01, certified in the same block (same seal) as sharded-same-seal-00"},
		} {
			tr := ta
			if k == 1 {
				tr = tbb
			}
			v, _ := vectorOf(sp, nSpecs+k, tr, blk.Shards[k].UC, blk.Shards[k].UCBytes)
			byName[sp.name] = &built{spec: sp, vec: v, uc: blk.Shards[k].UC, tree: tr, ucByte: blk.Shards[k].UCBytes}
			cases = append(cases, v)
		}
		nSpecs += 2
	}

	// every field of the snapshot at a large uint64 value (the seal re-signed by the fixture authority of epoch
	// bigRootEpoch; nothing is patched without re-deriving the paths and the signatures)
	{
		g := byName["shared-prefix"]
		uc, ucb, err := resignWith(g.uc, func(uc *types.UnicityCertificate) {
			uc.UnicitySeal.RootChainRoundNumber = bigRootRound
			uc.UnicitySeal.Timestamp = bigTimestamp
			uc.UnicitySeal.Epoch = bigRootEpoch
			uc.InputRecord.RoundNumber = bigShardRound
			uc.InputRecord.Epoch = bigShardEpoch
		}, fixtureKey)
		if err != nil {
			return nil, err
		}
		sp := g.spec
		sp.name, sp.ctx, sp.verdict = "large-uint64-fields", "big-epoch-authority", VerdictAbsent
		sp.desc = "shared-prefix re-certified with root round, seal timestamp, root epoch, shard round and shard epoch at large uint64 values (2^53+1 and near 2^64): decimal strings, no float or int64 narrowing"
		v, _ := vectorOf(sp, nSpecs, g.tree, uc, ucb)
		nSpecs++
		byName[sp.name] = &built{spec: sp, vec: v, uc: uc, tree: g.tree, ucByte: ucb}
		cases = append(cases, v)
	}

	// contexts
	confHash := byName["golden-two-leaf"].uc.ShardConfHash
	shardConf := byName["sharded-absent"].uc.ShardConfHash
	mk := func(desc, tbFile, shard, next, pin string) Context {
		return Context{Description: desc, TrustBase: tbFile, Shard: shard, NextEpochStart: next, ShardConfHash: pin}
	}
	contexts := map[string]Context{
		"default":                   mk("trusted authority of the fixture network, the aggregation partition (7), unsharded, no end boundary", "authority/trust-base.cbor", "", "0", ""),
		"wrong-network":             mk("authority of another network with the same keys and node IDs", "authority/other-network.cbor", "", "0", ""),
		"wrong-epoch":               mk("authority of the next epoch with the same keys: seals of epoch 1 must not verify", "authority/other-epoch.cbor", "", "0", ""),
		"before-epoch-start":        mk("authority whose epoch starts after the certified round", "authority/late-epoch-start.cbor", "", "0", ""),
		"untrusted-authority":       mk("a different authority (other keys, same network and node IDs)", "authority/untrusted-keys.cbor", "", "0", ""),
		"big-epoch-authority":       mk("the authority of root epoch 2^64-3 (same keys): for the vector with large uint64 fields", "authority/big-epoch.cbor", "", "0", ""),
		"config-pin-ok":             mk("pins the shard configuration hash of the corpus partition", "authority/trust-base.cbor", "", "0", hx(confHash)),
		"config-pin-other":          mk("pins another shard configuration hash", "authority/trust-base.cbor", "", "0", hx(bytes.Repeat([]byte{0xcc}, 32))),
		"shard-00":                  mk("shard 00 of the four shard partition", "authority/trust-base.cbor", "00", "0", ""),
		"shard-01":                  mk("shard 01 of the four shard partition", "authority/trust-base.cbor", "01", "0", ""),
		"shard-10":                  mk("shard 10 of the four shard partition", "authority/trust-base.cbor", "10", "0", ""),
		"shard-11":                  mk("shard 11 of the four shard partition", "authority/trust-base.cbor", "11", "0", ""),
		"shard-00-config-pin-ok":    mk("shard 00 pinning its own configuration hash", "authority/trust-base.cbor", "00", "0", hx(shardConf)),
		"shard-00-config-pin-other": mk("shard 00 pinning another configuration hash", "authority/trust-base.cbor", "00", "0", hx(bytes.Repeat([]byte{0xcc}, 32))),
	}
	// retirement boundary relative to the base case round
	g := byName["shared-prefix"]
	gr := g.uc.UnicitySeal.RootChainRoundNumber
	contexts["epoch-end-after"] = mk("authenticated end of the epoch is after the certified round (still valid)", "authority/trust-base.cbor", "", fmt.Sprint(gr+1), "")
	contexts["epoch-end-at-round"] = mk("the next epoch starts at the certified round: the seal is at the retirement boundary and not valid", "authority/trust-base.cbor", "", fmt.Sprint(gr), "")

	for n, c := range contexts {
		c.TrustBaseJSON = strings.TrimSuffix(c.TrustBase, ".cbor") + ".json"
		contexts[n] = c
	}
	cb, err := json.MarshalIndent(contexts, "", "  ")
	if err != nil {
		return nil, err
	}
	files["contexts.json"] = append(cb, '\n')

	evaluate := func(v Vector, ctxName string) (string, *Snapshot, error) {
		w, err := hex.DecodeString(v.Wrapper)
		if err != nil {
			return "", nil, err
		}
		q, err := hex.DecodeString(v.Query)
		if err != nil || len(q) != KeySize {
			return "", nil, errors.New("bad query")
		}
		c := contexts[ctxName]
		verdict, snap := Verdict(w, [KeySize]byte(q), c, tbs[c.TrustBase])
		return verdict, snap, nil
	}
	byPath := map[string]Vector{}
	put := func(dir string, v Vector) error {
		got, snap, err := evaluate(v, v.Context)
		if err != nil {
			return fmt.Errorf("%s: %w", v.Name, err)
		}
		if got != v.Expected.Verdict {
			return fmt.Errorf("%s: reference verdict %s, intended %s", v.Name, got, v.Expected.Verdict)
		}
		v.Expected = expectedOf(got, snap, v.Query, v.Expected.Acceptable)
		byPath[dir+"/"+v.Name] = v
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
	for _, c := range negSpecs {
		if err := put("negative", c); err != nil {
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
		{"config-pin-other", VerdictUCInvalid, "the shard configuration hash differs from the pinned one"},
		{"config-pin-ok", VerdictAbsent, "the shard configuration hash equals the pinned one"},
		{"epoch-end-at-round", VerdictAuthority, "the seal round is at the authenticated end of its epoch (retirement boundary)"},
		{"epoch-end-after", VerdictAbsent, "the authenticated end of the epoch is after the seal round"},
		{"shard-00", VerdictUCInvalid, "an unsharded UC presented to a verifier of shard 00"},
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
	ucNegs, err := ucNegatives(g.vec, byName["empty-tree"].vec, tb, prod, contexts, tbs)
	if err != nil {
		return nil, err
	}
	for _, n := range ucNegs {
		if err := put("negative", n); err != nil {
			return nil, err
		}
	}
	// partition and shard negatives
	sa := byName["sharded-absent"]
	shNegs, err := shardNegatives(sa.vec, byName["sharded-same-seal-00"].vec, byName["sharded-same-seal-01"].vec)
	if err != nil {
		return nil, err
	}
	for _, n := range shNegs {
		if err := put("negative", n); err != nil {
			return nil, err
		}
	}
	// validly signed UCs of another partition (same authority, same keys): only the partition check rejects them
	{
		r, _ := g.tree.Root()
		blk, err := nextBlock(prodOther, nil, ucfixture.Change{Shard: 0, Hash: r[:]})
		if err != nil {
			return nil, err
		}
		v := derive(g.vec, "uc-wrong-partition-validly-signed", "the same tree certified by partition 8 under the same trusted authority: the UC is authentic but not of the aggregation partition 7", VerdictUCInvalid,
			func(v *Vector) {
				v.UC = hx(blk.Shards[0].UCBytes)
				v.Wrapper = hx(EncodeWrapper(100, unhex(g.vec.Certificate), blk.Shards[0].UCBytes))
				v.Block = "100"
			})
		if err := put("negative", v); err != nil {
			return nil, err
		}
		tr, _ := Build(sa.spec.leaves)
		rs, _ := tr.Root()
		blk, err = nextBlock(prodOtherS, nil, ucfixture.Change{Shard: 0, Hash: rs[:]})
		if err != nil {
			return nil, err
		}
		v = derive(sa.vec, "uc-wrong-partition-sharded-validly-signed", "shard 00 of the four shard partition 8, validly signed by the trusted authority: not the aggregation partition", VerdictUCInvalid,
			func(v *Vector) {
				v.UC = hx(blk.Shards[0].UCBytes)
				v.Wrapper = hx(EncodeWrapper(100, unhex(sa.vec.Certificate), blk.Shards[0].UCBytes))
				v.Block = "100"
			})
		if err := put("negative", v); err != nil {
			return nil, err
		}
	}

	// direct-unsharded: tree, UC and raw inclusion certificates for consolidation fixtures (no sidecar)
	if err := directUnsharded(files, byName["random-16"].tree, byName["random-16"].spec.leaves, byName["random-16"].vec); err != nil {
		return nil, err
	}
	// policy examples and verifier sequences
	if err := policyAndSequences(files, byPath, evaluate); err != nil {
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
		Name      string `json:"name"`
		Kind      string `json:"kind"`
		Outcome   string `json:"outcome,omitempty"`
		Rejection string `json:"rejection,omitempty"`
		Context   string `json:"context,omitempty"`
		Describe  string `json:"description,omitempty"`
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
		cases = append(cases, entry{Name: strings.TrimSuffix(strings.SplitN(n, "/", 2)[1], ".json"), Kind: kind, Outcome: v.Expected.Outcome, Rejection: v.Expected.Rejection, Context: v.Context, Describe: v.Description})
	}
	sums := map[string]string{}
	for _, n := range names {
		h := sha256.Sum256(files[n])
		sums[n] = hx(h[:])
	}
	modules, err := generatorModules()
	if err != nil {
		return nil, err
	}
	m := map[string]any{
		"corpus": "non-inclusion", "version": Version, "schema": "outcomes: absent_at_snapshot | included_at_snapshot (stateId + snapshot) | rejected (rejection class, no snapshot); uint64 fields are decimal strings",
		"generator": map[string]any{
			"command": "UC_FIXTURE_AGGREGATOR_COMMIT=<commit> go run ./cmd/gen-ni-fixtures -out testdata/non-inclusion/v2",
			// commit is the aggregator-go commit which CONTAINS cmd/gen-ni-fixtures and internal/nifixture as
			// used to produce these files (not the commit which added the files)
			"commit":  commit,
			"modules": modules,
		},
		"limits": map[string]uint64{"maxCertificateBytes": MaxCertificateBytes, "maxRequestBytes": 64 << 10, "maxResponseBytes": 8 << 20},
		"cases":  cases,
		"sha256": sums,
	}
	b, err := json.MarshalIndent(m, "", "  ")
	return append(b, '\n'), err
}

// generatorModules are the module versions which determine every UC and signature byte. They are read from the
// go.mod found by walking up from the working directory (the single source of truth), not from the build info,
// which test binaries built by Go <= 1.26 do not carry.
func generatorModules() (map[string]string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	var path string
	for {
		p := filepath.Join(dir, "go.mod")
		if _, err := os.Stat(p); err == nil {
			path = p
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil, errors.New("go.mod not found in any parent of the working directory")
		}
		dir = parent
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	f, err := modfile.ParseLax(path, data, nil)
	if err != nil {
		return nil, err
	}
	want := []string{"github.com/unicitynetwork/bft-core", "github.com/unicitynetwork/bft-go-base"}
	out := map[string]string{}
	for _, r := range f.Require {
		for _, w := range want {
			if r.Mod.Path == w {
				out[w] = r.Mod.Version
			}
		}
	}
	for _, w := range want {
		if out[w] == "" {
			return nil, fmt.Errorf("%s: no require for %s", path, w)
		}
	}
	return out, nil
}
