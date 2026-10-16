package nifixture

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/unicitynetwork/bft-core/rootchain/testutils/ucfixture"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"
)

func unhex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func derive(base Vector, name, desc string, verdict string, mutate func(v *Vector)) Vector {
	v := base
	v.Name = name
	v.Base = base.Name
	v.Description = desc
	v.Expected = Expected{Verdict: verdict}
	mutate(&v)
	return v
}

func rewrap(v *Vector, mutateCert func(c []byte) []byte) {
	block, cert, uc, err := DecodeWrapper(unhex(v.Wrapper))
	if err != nil {
		panic(err)
	}
	cert = mutateCert(append([]byte(nil), cert...))
	v.Certificate = hex.EncodeToString(cert)
	v.Wrapper = hex.EncodeToString(EncodeWrapper(block, cert, uc))
}

func flip(b []byte, byteIdx int, mask byte) []byte {
	b = append([]byte(nil), b...)
	b[byteIdx] ^= mask
	return b
}

// negatives are single-fault mutations of a positive case; the expected verdict is the one
// SPEC.md "Verification order" assigns to the first failing check.
func negatives(base, golden, empty, zero, chain Vector) ([]Vector, error) {
	var out []Vector
	add := func(v Vector) { out = append(out, v) }
	// certificate level (base: shared-prefix, junctions at depth 0 and 4)
	add(derive(base, "cert-bitmap-bit-moved", "a junction bit moved to another depth (same popcount): the fold uses the wrong depth", VerdictCertInvalid,
		func(v *Vector) { rewrap(v, func(c []byte) []byte { c[0] = 0x84; return c }) }))
	add(derive(base, "cert-bitmap-extra-bit", "one more bitmap bit than siblings: length does not match the popcount", VerdictMalformed,
		func(v *Vector) { rewrap(v, func(c []byte) []byte { c[0] |= 0x01; return c }) }))
	add(derive(base, "cert-sibling-flipped", "first sibling hash altered", VerdictCertInvalid,
		func(v *Vector) { rewrap(v, func(c []byte) []byte { return flip(c, 32, 0x01) }) }))
	add(derive(base, "cert-terminal-key-flipped", "terminal key altered below all junction depths", VerdictCertInvalid,
		func(v *Vector) { rewrap(v, func(c []byte) []byte { return flip(c, len(c)-64+2, 0x01) }) }))
	add(derive(base, "cert-terminal-value-flipped", "terminal value altered", VerdictCertInvalid,
		func(v *Vector) { rewrap(v, func(c []byte) []byte { return flip(c, len(c)-1, 0x01) }) }))
	add(derive(base, "cert-truncated", "last byte missing", VerdictMalformed,
		func(v *Vector) { rewrap(v, func(c []byte) []byte { return c[:len(c)-1] }) }))
	add(derive(base, "cert-trailing-byte", "one byte after the terminal value", VerdictMalformed,
		func(v *Vector) { rewrap(v, func(c []byte) []byte { return append(c, 0) }) }))
	add(derive(base, "cert-terminal-value-31-bytes", "terminal value one byte short (the profile fixes 32 bytes)", VerdictMalformed,
		func(v *Vector) {
			rewrap(v, func(c []byte) []byte { return append(c[:len(c)-33:len(c)-33], c[len(c)-32:]...) })
		}))
	add(derive(base, "cert-oversized", "8289 bytes: above the 8288 byte maximum", VerdictMalformed,
		func(v *Vector) { rewrap(v, func(c []byte) []byte { return make([]byte, MaxCertificateBytes+1) }) }))
	add(derive(base, "cert-empty-against-nonempty-root", "the empty-tree certificate against a UC with a nonempty root", VerdictCertInvalid,
		func(v *Vector) { rewrap(v, func(c []byte) []byte { return []byte{} }) }))
	add(derive(empty, "cert-nonempty-against-empty-root", "a singleton certificate against the certified empty tree", VerdictCertInvalid,
		func(v *Vector) {
			rewrap(v, func(c []byte) []byte {
				return append(append(make([]byte, 32), bytes.Repeat([]byte{1}, 32)...), bytes.Repeat([]byte{2}, 32)...)
			})
		}))
	add(derive(base, "query-other-branch", "the query chooses the other branch at the root junction; the certificate is for the left side", VerdictCertInvalid,
		func(v *Vector) { q := unhex(v.Query); q[0] = 0xf1; v.Query = hex.EncodeToString(q) }))
	add(derive(base, "query-is-terminal", "the query equals the authenticated terminal key: the state is included", VerdictStateInclude,
		func(v *Vector) { q := make([]byte, 32); q[0] = 0x10; v.Query = hex.EncodeToString(q) }))
	add(derive(chain, "query-diverges-at-last-junction", "full depth chain, query follows the path to a different terminal leaf than the certificate's", VerdictCertInvalid,
		func(v *Vector) {
			q := unhex(v.Query)
			q[0] ^= 0x80 // other side of the root junction
			v.Query = hex.EncodeToString(q)
		}))
	// wrapper level
	cert := unhex(base.Certificate)
	uc := unhex(base.UC)
	block := uint64(0)
	fmt.Sscan(base.Block, &block)
	wr := func(name, desc string, verdict string, w []byte) {
		v := base
		v.Name, v.Base, v.Description, v.Wrapper, v.Expected = name, base.Name, desc, hex.EncodeToString(w), Expected{Verdict: verdict}
		add(v)
	}
	good := EncodeWrapper(block, cert, uc)
	wr("wrapper-wrong-tag", "tag 39035", VerdictMalformed, EncodeWrapperWith(block, ProofTag+1, ProofVersion, cert, uc))
	wr("wrapper-wrong-version", "version 2", VerdictMalformed, EncodeWrapperWith(block, ProofTag, 2, cert, uc))
	wr("wrapper-trailing-bytes", "a byte after the wrapper", VerdictMalformed, append(append([]byte(nil), good...), 0))
	wr("wrapper-empty", "no bytes", VerdictMalformed, []byte{})
	wr("wrapper-block-noncanonical-integer", "block number encoded in a longer form than needed", VerdictMalformed,
		append([]byte{0x82, 0x19, 0x00, 0x05}, good[2:]...))
	wr("wrapper-three-elements", "outer array of three elements", VerdictMalformed, append([]byte{0x83}, good[1:]...))
	wr("wrapper-proof-two-elements", "proof without the unicity certificate", VerdictMalformed,
		append(append(append(cborHead(4, 2), cborHead(0, block)...), append(cborHead(6, ProofTag), cborHead(4, 2)...)...), append(cborHead(0, 1), append(cborHead(2, uint64(len(cert))), cert...)...)...))
	// the UC as a byte string holding the CBOR (instead of the embedded value)
	{
		w := append(cborHead(4, 2), cborHead(0, block)...)
		w = append(w, cborHead(6, ProofTag)...)
		w = append(w, cborHead(4, 3)...)
		w = append(w, cborHead(0, 1)...)
		w = append(w, cborHead(2, uint64(len(cert)))...)
		w = append(w, cert...)
		w = append(w, cborHead(2, uint64(len(uc)))...)
		w = append(w, uc...)
		wr("wrapper-uc-as-byte-string", "the UC is a byte string holding its CBOR, not the native value", VerdictMalformed, w)
	}
	wr("wrapper-legacy-uc-tag", "an empty array under the legacy tag 1007 instead of a native UC", VerdictMalformed,
		EncodeWrapper(block, cert, append(cborHead(6, 1007), cborHead(4, 0)...)))
	wr("wrapper-certificate-as-text", "certificate is a text string", VerdictMalformed,
		append(append(append(cborHead(4, 2), cborHead(0, block)...), append(cborHead(6, ProofTag), cborHead(4, 3)...)...), append(append(cborHead(0, 1), cborHead(3, 2)...), append([]byte("ab"), uc...)...)...))
	_ = golden
	_ = zero
	return out, nil
}

// ucNegatives mutate the unicity certificate of base (the signature or content), context default.
func ucNegatives(base Vector, tb *types.RootTrustBaseV1, prod *ucfixture.Producer, contexts map[string]Context, tbs map[string]types.RootTrustBase) ([]Vector, error) {
	var out []Vector
	block := uint64(0)
	fmt.Sscan(base.Block, &block)
	cert := unhex(base.Certificate)
	var orig types.UnicityCertificate
	if err := types.Cbor.Unmarshal(unhex(base.UC), &orig); err != nil {
		return nil, err
	}
	clone := func() *types.UnicityCertificate {
		var c types.UnicityCertificate
		if err := types.Cbor.Unmarshal(unhex(base.UC), &c); err != nil {
			panic(err)
		}
		return &c
	}
	emit := func(name, desc, verdict string, uc *types.UnicityCertificate) error {
		ucb, err := uc.MarshalCBOR()
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		v := derive(base, name, desc, verdict, func(v *Vector) {
			v.UC = hex.EncodeToString(ucb)
			v.Wrapper = hex.EncodeToString(EncodeWrapper(block, cert, ucb))
		})
		out = append(out, v)
		return nil
	}
	// altered signature
	{
		uc := clone()
		ids := sigIDs(uc)
		s := append([]byte(nil), uc.UnicitySeal.Signatures[ids[0]]...)
		s[0] ^= 0x01
		uc.UnicitySeal.Signatures[ids[0]] = s
		if err := emit("uc-signature-altered", "one root signature of the seal altered (witness bytes)", VerdictAuthority, uc); err != nil {
			return nil, err
		}
	}
	// signature of a node removed: same as under-quorum seal built by signature removal
	{
		uc := clone()
		for _, k := range sigIDs(uc)[2:] {
			delete(uc.UnicitySeal.Signatures, k)
		}
		if err := emit("uc-signatures-removed-below-quorum", "signatures removed from a valid seal until fewer than the quorum remain", VerdictAuthority, uc); err != nil {
			return nil, err
		}
	}
	// self-signed: same seal content signed by an untrusted authority
	{
		uc := clone()
		uc.UnicitySeal.Signatures = nil
		for i := 0; i < 3; i++ {
			k := sha256.Sum256([]byte(fmt.Sprintf("non-inclusion corpus untrusted authority %d", i)))
			s, err := abcrypto.NewInMemorySecp256K1SignerFromKey(k[:])
			if err != nil {
				return nil, err
			}
			if err := uc.UnicitySeal.Sign(ucfixture.RootNodeID(i), s); err != nil {
				return nil, err
			}
		}
		if err := emit("uc-self-signed", "the seal content re-signed by an authority which is not the trusted one", VerdictAuthority, uc); err != nil {
			return nil, err
		}
	}
	{
		uc := clone()
		uc.InputRecord.Hash = bytes.Repeat([]byte{0x77}, 32)
		if err := emit("uc-input-record-altered", "state hash of the input record replaced: the shard path no longer reaches the seal hash", VerdictUCInvalid, uc); err != nil {
			return nil, err
		}
	}
	{
		uc := clone()
		uc.UnicityTreeCertificate = nil
		if err := emit("uc-no-unicity-tree-certificate", "unicity tree certificate absent", VerdictUCInvalid, uc); err != nil {
			return nil, err
		}
	}
	{
		uc := clone()
		if len(uc.UnicityTreeCertificate.HashSteps) == 0 {
			return nil, fmt.Errorf("base has no partition path")
		}
		uc.UnicityTreeCertificate.HashSteps[0] = nil
		if err := emit("uc-null-partition-path-step", "null entry in the partition path", VerdictUCInvalid, uc); err != nil {
			return nil, err
		}
	}
	{
		uc := clone()
		a, _ := uc.ShardTreeCertificate.Shard.Split()
		uc.ShardTreeCertificate.Shard = a
		if err := emit("uc-nonempty-shard-id-without-path", "a nonempty shard ID with no sibling hashes (not the unsharded form)", VerdictUCInvalid, uc); err != nil {
			return nil, err
		}
	}
	{
		uc := clone()
		uc.ShardTreeCertificate.SiblingHashes = [][]byte{bytes.Repeat([]byte{1}, 32)}
		if err := emit("uc-empty-shard-id-with-sibling", "an empty shard ID with a sibling hash", VerdictUCInvalid, uc); err != nil {
			return nil, err
		}
	}
	_ = tb
	_ = prod
	_ = contexts
	_ = tbs
	_ = json.Marshal
	return out, nil
}

// directUnsharded freezes the data of a latest batch for one unsharded partition: the UC (no
// partition-view sidecar exists for it), the tree and the raw inclusion certificate of every leaf.
func directUnsharded(files Files, tree *Tree, leaves []Leaf, v Vector) error {
	type entry struct {
		Key         string `json:"key"`
		Value       string `json:"value"`
		Certificate string `json:"inclusionCertificate"`
	}
	out := struct {
		Description string  `json:"description"`
		Root        string  `json:"root"`
		UC          string  `json:"uc"`
		Context     string  `json:"context"`
		Leaves      []entry `json:"leaves"`
	}{
		Description: "an unsharded BFT partition: the ordinary authenticated UC (empty shard ID, no shard path) is the consolidation target and no partition view exists. inclusionCertificate is bitmap||siblings (root to leaf) of the leaf against root, the value is the raw tree leaf value",
		Root:        v.Root, UC: v.UC, Context: "default",
	}
	for _, l := range leaves {
		c, ok := tree.InclusionCertificate(l.Key)
		if !ok {
			return fmt.Errorf("leaf missing")
		}
		out.Leaves = append(out.Leaves, entry{Key: hex.EncodeToString(l.Key[:]), Value: hex.EncodeToString(l.Value[:]), Certificate: hex.EncodeToString(c)})
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	files["direct-unsharded/latest.json"] = append(b, '\n')
	return nil
}

// sigIDs are the signer IDs of the seal in ascending order (maps are unordered).
func sigIDs(uc *types.UnicityCertificate) []string {
	var ids []string
	for k := range uc.UnicitySeal.Signatures {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	return ids
}
