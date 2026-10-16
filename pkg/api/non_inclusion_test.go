package api_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/nifixture"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Rust SDK / rugregator golden vector (leaves 00..00->01..01, 80 00..00->02..02, query 01 00..00).
const goldenCert = "8000000000000000000000000000000000000000000000000000000000000000d79ae116bb8c8604819adc35c2a71b7c3acf89cd1be8a382fbedbd6905ac20e700000000000000000000000000000000000000000000000000000000000000000101010101010101010101010101010101010101010101010101010101010101"
const goldenRoot = "5158a5f4029511f5312b7a147c9da48b9281322d1b893a10a25b2c17e2197a53"

func mustHex(t testing.TB, s string) []byte {
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestNonInclusionCertRustGoldenVector(t *testing.T) {
	var c api.NonInclusionCert
	require.NoError(t, c.UnmarshalBinary(mustHex(t, goldenCert)))
	root := mustHex(t, goldenRoot)
	query := make([]byte, 32)
	query[0] = 0x01
	require.NoError(t, c.VerifyAgainstTrustedRoot(query, root, api.SHA256))
	out, err := c.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, mustHex(t, goldenCert), out)
	// the terminal itself is the other side: the state is included
	require.ErrorIs(t, c.VerifyAgainstTrustedRoot(make([]byte, 32), root, api.SHA256), api.ErrStateIncluded)
	// a query choosing the other branch is not covered by this path
	other := make([]byte, 32)
	other[0] = 0x81
	require.ErrorIs(t, c.VerifyAgainstTrustedRoot(other, root, api.SHA256), api.ErrNonInclusionInvalid)
	// wrong root
	bad := bytes.Clone(root)
	bad[0] ^= 1
	require.ErrorIs(t, c.VerifyAgainstTrustedRoot(query, bad, api.SHA256), api.ErrNonInclusionInvalid)
}

func TestNonInclusionCertEmptyTree(t *testing.T) {
	var c api.NonInclusionCert
	require.NoError(t, c.UnmarshalBinary(nil))
	require.True(t, c.IsEmptyTree())
	b, err := c.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, b)
	require.Empty(t, b)
	q := make([]byte, 32)
	require.NoError(t, c.VerifyAgainstTrustedRoot(q, nil, api.SHA256))
	require.ErrorIs(t, c.VerifyAgainstTrustedRoot(q, make([]byte, 32), api.SHA256), api.ErrNonInclusionInvalid, "the all-zero hash is not an empty tree")
	var d api.NonInclusionCert
	require.NoError(t, d.UnmarshalBinary(mustHex(t, goldenCert)))
	require.ErrorIs(t, d.VerifyAgainstTrustedRoot(q, nil, api.SHA256), api.ErrNonInclusionInvalid)
	var unset api.NonInclusionCert
	require.Error(t, unset.VerifyAgainstTrustedRoot(q, nil, api.SHA256))
}

func TestNonInclusionCertDecoderBounds(t *testing.T) {
	var c api.NonInclusionCert
	good := mustHex(t, goldenCert)
	for name, in := range map[string][]byte{
		"short":         good[:95],
		"trailing":      append(bytes.Clone(good), 0),
		"truncated":     good[:len(good)-1],
		"bare bitmap":   make([]byte, 32),
		"oversize":      make([]byte, api.MaxNonInclusionCertBytes+1),
		"value 31":      append(good[:len(good)-33:len(good)-33], good[len(good)-32:]...),
		"bitmap/length": append([]byte{0xff}, make([]byte, 95)...),
	} {
		require.ErrorIs(t, c.UnmarshalBinary(in), api.ErrNonInclusionMalformed, name)
	}
	// the maximum is accepted: 256 junctions
	max := make([]byte, api.MaxNonInclusionCertBytes)
	for i := 0; i < 32; i++ {
		max[i] = 0xff
	}
	require.NoError(t, c.UnmarshalBinary(max))
	require.Equal(t, 256, c.Junctions())
	_, err := api.NewNonInclusionCert([32]byte{0x80}, nil, make([]byte, 32), make([]byte, 32))
	require.ErrorIs(t, err, api.ErrNonInclusionMalformed)
}

// Random trees: the production verifier agrees with the independent oracle on the certificate it
// generates, on included queries, and on every single-bit mutation.
func TestNonInclusionCertAgainstTheOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for iter := 0; iter < 200; iter++ {
		var leaves []nifixture.Leaf
		seen := map[[32]byte]bool{}
		for n := rng.Intn(48); len(leaves) < n; {
			var l nifixture.Leaf
			rng.Read(l.Key[:])
			if len(leaves) > 0 && rng.Intn(2) == 0 {
				l.Key = leaves[rng.Intn(len(leaves))].Key
				l.Key[rng.Intn(32)] ^= 1 << uint(rng.Intn(8))
			}
			rng.Read(l.Value[:])
			if !seen[l.Key] {
				seen[l.Key] = true
				leaves = append(leaves, l)
			}
		}
		tree, err := nifixture.Build(leaves)
		require.NoError(t, err)
		root, ok := tree.Root()
		var rb []byte
		if ok {
			rb = root[:]
		}
		var q [32]byte
		rng.Read(q[:])
		if len(leaves) > 0 && rng.Intn(3) == 0 {
			q = leaves[rng.Intn(len(leaves))].Key
		}
		raw := tree.Certificate(q)
		var c api.NonInclusionCert
		require.NoError(t, c.UnmarshalBinary(raw))
		out, _ := c.MarshalBinary()
		require.Equal(t, raw, out)
		var rp *[32]byte
		if ok {
			rp = &root
		}
		want := nifixture.Verify(raw, q, rp)
		got := c.VerifyAgainstTrustedRoot(q[:], rb, api.SHA256)
		switch {
		case want == nil:
			require.NoError(t, got)
		default:
			require.ErrorIs(t, got, api.ErrStateIncluded)
		}
		for k := 0; k < 6 && len(raw) > 0; k++ {
			m := bytes.Clone(raw)
			m[rng.Intn(len(m))] ^= 1 << uint(rng.Intn(8))
			var mc api.NonInclusionCert
			if err := mc.UnmarshalBinary(m); err != nil {
				require.Error(t, nifixture.Verify(m, q, rp))
				continue
			}
			wantErr := nifixture.Verify(m, q, rp)
			gotErr := mc.VerifyAgainstTrustedRoot(q[:], rb, api.SHA256)
			require.Equal(t, wantErr == nil, gotErr == nil)
		}
	}
}

func TestNonInclusionRequestParsing(t *testing.T) {
	id := hex.EncodeToString(bytes.Repeat([]byte{0xab}, 32))
	r, err := api.ParseGetNonInclusionProofRequestV1(json.RawMessage(`{"stateId":"` + id + `"}`))
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xab}, 32), []byte(r.StateID))
	_, err = api.ParseGetNonInclusionProofRequestV1(json.RawMessage(`{"stateId":"0X` + id + `"}`))
	require.NoError(t, err)
	for name, p := range map[string]string{
		"unknown field": `{"stateId":"` + id + `","stateRoot":"latest"}`,
		"duplicate":     `{"stateId":"` + id + `","stateId":"` + id + `"}`,
		"short":         `{"stateId":"abcd"}`,
		"not hex":       `{"stateId":"` + id[:62] + `zz"}`,
		"array":         `["` + id + `"]`,
		"missing":       `{}`,
		"trailing":      `{"stateId":"` + id + `"} {}`,
		"number":        `{"stateId":5}`,
		"empty":         ``,
		"null":          `null`,
	} {
		_, err := api.ParseGetNonInclusionProofRequestV1(json.RawMessage(p))
		require.Error(t, err, name)
	}
}

// The result is one JSON string holding the hex of [block, proof]; the codec is exact.
func TestNonInclusionResponseCodec(t *testing.T) {
	var cert api.NonInclusionCert
	require.NoError(t, cert.UnmarshalBinary(mustHex(t, goldenCert)))
	uc := []byte{0xd9, 0x03, 0xef, 0x80} // any well-formed CBOR value
	resp := &api.GetNonInclusionProofResponseV1{BlockNumber: 1 << 40, Proof: &api.NonInclusionProofV1{Certificate: &cert, UnicityCertificate: uc}}
	j, err := json.Marshal(resp)
	require.NoError(t, err)
	require.Equal(t, byte('"'), j[0])
	var back api.GetNonInclusionProofResponseV1
	require.NoError(t, json.Unmarshal(j, &back))
	require.Equal(t, resp.BlockNumber, back.BlockNumber)
	require.Equal(t, uc, []byte(back.Proof.UnicityCertificate))
	cbor, err := resp.MarshalCBOR()
	require.NoError(t, err)
	// 82 1b <u64> d9 987a 83 01 58a0 ...
	require.Equal(t, []byte{0x82, 0x1b}, cbor[:2])
	require.Equal(t, []byte{0xd9, 0x98, 0x7a, 0x83, 0x01, 0x58, 0x80}, cbor[10:17])
	for name, in := range map[string][]byte{
		"trailing":  append(bytes.Clone(cbor), 0),
		"truncated": cbor[:len(cbor)-1],
		"empty":     {},
	} {
		var r api.GetNonInclusionProofResponseV1
		require.Error(t, r.UnmarshalCBOR(in), name)
	}
	require.Error(t, json.Unmarshal([]byte(`{"proof":"00"}`), &back))
	huge := `"` + string(bytes.Repeat([]byte{'0'}, 2*api.NonInclusionMaxResponseBytes+2)) + `"`
	require.Error(t, json.Unmarshal([]byte(huge), &back))
}

// Hostile bytes never panic and never decode to a proof which does not re-encode identically.
func TestNonInclusionDecodersAreTotal(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	var base api.GetNonInclusionProofResponseV1
	var cert api.NonInclusionCert
	require.NoError(t, cert.UnmarshalBinary(mustHex(t, goldenCert)))
	valid, err := (&api.GetNonInclusionProofResponseV1{BlockNumber: 5, Proof: &api.NonInclusionProofV1{Certificate: &cert, UnicityCertificate: []byte{0xf6}}}).MarshalCBOR()
	require.NoError(t, err)
	for i := 0; i < 20000; i++ {
		in := bytes.Clone(valid)
		for k := 0; k < 1+rng.Intn(3); k++ {
			in[rng.Intn(len(in))] = byte(rng.Intn(256))
		}
		if rng.Intn(4) == 0 {
			in = in[:rng.Intn(len(in))]
		}
		var r api.GetNonInclusionProofResponseV1
		if err := r.UnmarshalCBOR(in); err == nil {
			re, err := r.MarshalCBOR()
			require.NoError(t, err)
			require.Equal(t, in, re)
		}
	}
	_ = base
}
