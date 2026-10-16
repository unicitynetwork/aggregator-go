package api

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/unicitynetwork/bft-go-base/types"
)

// NonInclusionProofTag is the CBOR tag of the non-inclusion proof (unicity-ids registry).
const NonInclusionProofTag types.CborTag = 39034

const (
	// NonInclusionProofVersion is the only proof version.
	NonInclusionProofVersion = 1
	// GetNonInclusionProofMethod is the JSON-RPC method.
	GetNonInclusionProofMethod = "get_non_inclusion_proof.v1"
	// NonInclusionMaxRequestBytes bounds the whole request body.
	NonInclusionMaxRequestBytes = 64 << 10
	// NonInclusionMaxResponseBytes bounds the serialized response (and its decoding by clients).
	NonInclusionMaxResponseBytes = 8 << 20
)

// JSON-RPC error codes shared with rugregator and the Rust SDK.
const (
	// NonInclusionCodeNotFound: no certified snapshot is available.
	NonInclusionCodeNotFound = -32020
	// NonInclusionCodeStateIncluded: the state is present at the selected root.
	NonInclusionCodeStateIncluded = -32022
	// NonInclusionCodeBusy: the proof admission limit is reached.
	NonInclusionCodeBusy = -32023
)

var ErrNonInclusionProofMalformed = errors.New("non-inclusion proof: malformed")

// NonInclusionProofV1 is the certificate together with the unicity certificate which
// authenticates the root at one certified point in time.
//
//	39034([1, certificate: bstr, unicityCertificate: native tagged CBOR value])
//
// It proves absence at that snapshot only, not current absence. The queried state ID is local
// context (ForState) and never part of the wire.
type NonInclusionProofV1 struct {
	Certificate        *NonInclusionCert
	UnicityCertificate types.RawCBOR

	requested StateID // local binding, not serialized
}

// ForState binds the proof to the state ID it was requested for. Binding twice to
// different IDs is an error.
func (p *NonInclusionProofV1) ForState(id StateID) (*NonInclusionProofV1, error) {
	if len(p.requested) != 0 && !bytes.Equal(p.requested, id) {
		return nil, errors.New("non-inclusion proof is bound to another state ID")
	}
	cp := *p
	cp.requested = bytes.Clone(id)
	return &cp, nil
}

// RequestedStateID is the bound state ID (nil if unbound).
func (p *NonInclusionProofV1) RequestedStateID() StateID { return bytes.Clone(p.requested) }

// MarshalCBOR encodes the tagged proof.
func (p *NonInclusionProofV1) MarshalCBOR() ([]byte, error) {
	if p == nil || p.Certificate == nil || len(p.UnicityCertificate) == 0 {
		return nil, fmt.Errorf("%w: incomplete", ErrNonInclusionProofMalformed)
	}
	cert, err := p.Certificate.MarshalBinary()
	if err != nil {
		return nil, err
	}
	out := cborTag(6, int(NonInclusionProofTag))
	out = append(out, CborArray(3)...)
	out = append(out, CborUint(NonInclusionProofVersion)...)
	out = append(out, CborBytes(len(cert))...)
	out = append(out, cert...)
	return append(out, p.UnicityCertificate...), nil
}

// UnmarshalCBOR decodes the exact profile (tag, version, shapes, shortest integers, no trailing bytes).
func (p *NonInclusionProofV1) UnmarshalCBOR(data []byte) error {
	r := &niReader{b: data}
	if err := r.proof(p); err != nil {
		return err
	}
	if r.i != len(data) {
		return fmt.Errorf("%w: trailing bytes", ErrNonInclusionProofMalformed)
	}
	return nil
}

// GetNonInclusionProofRequestV1 is the params object {"stateId": "<64 hex digits>"}.
type GetNonInclusionProofRequestV1 struct {
	StateID StateID
}

// ParseGetNonInclusionProofRequestV1 decodes the params: exactly one field stateId, no unknown
// or duplicate fields, a decoded 32 byte ID (optional 0x prefix, either case).
func ParseGetNonInclusionProofRequestV1(params json.RawMessage) (*GetNonInclusionProofRequestV1, error) {
	if len(params) == 0 || len(params) > NonInclusionMaxRequestBytes {
		return nil, errors.New("params missing or too large")
	}
	dec := json.NewDecoder(bytes.NewReader(params))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, errors.New("params must be an object")
	}
	var id StateID
	seen := false
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return nil, err
		}
		if k, _ := kt.(string); k != "stateId" || seen {
			return nil, errors.New("unknown or duplicate field")
		}
		seen = true
		var s string
		if err := dec.Decode(&s); err != nil {
			return nil, errors.New("stateId must be a string")
		}
		s = strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
		if len(s) != 2*StateTreeKeyLengthBytes {
			return nil, errors.New("stateId must be 32 bytes")
		}
		b, err := hex.DecodeString(s)
		if err != nil {
			return nil, errors.New("stateId is not hex")
		}
		id = b
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, errors.New("malformed params")
	}
	if _, err := dec.Token(); err == nil {
		return nil, errors.New("trailing data after params")
	}
	if !seen {
		return nil, errors.New("stateId is required")
	}
	return &GetNonInclusionProofRequestV1{StateID: id}, nil
}

// GetNonInclusionProofResponseV1 is the result: the block number of the certified snapshot
// (unauthenticated metadata) and the proof. On the wire the JSON-RPC result is ONE STRING:
// the hex of [blockNumber, proof].
type GetNonInclusionProofResponseV1 struct {
	BlockNumber uint64
	Proof       *NonInclusionProofV1
}

// MarshalCBOR encodes [blockNumber, proof].
func (r *GetNonInclusionProofResponseV1) MarshalCBOR() ([]byte, error) {
	if r == nil || r.Proof == nil {
		return nil, fmt.Errorf("%w: no proof", ErrNonInclusionProofMalformed)
	}
	p, err := r.Proof.MarshalCBOR()
	if err != nil {
		return nil, err
	}
	out := append(CborArray(2), CborUint(r.BlockNumber)...)
	return append(out, p...), nil
}

// UnmarshalCBOR decodes the exact response profile.
func (r *GetNonInclusionProofResponseV1) UnmarshalCBOR(data []byte) error {
	if len(data) > NonInclusionMaxResponseBytes {
		return fmt.Errorf("%w: response too large", ErrNonInclusionProofMalformed)
	}
	rd := &niReader{b: data}
	if m, n, err := rd.head(); err != nil || m != 4 || n != 2 {
		return fmt.Errorf("%w: not a two element array", ErrNonInclusionProofMalformed)
	}
	m, block, err := rd.head()
	if err != nil || m != 0 {
		return fmt.Errorf("%w: block number is not an unsigned integer", ErrNonInclusionProofMalformed)
	}
	var p NonInclusionProofV1
	if err := rd.proof(&p); err != nil {
		return err
	}
	if rd.i != len(data) {
		return fmt.Errorf("%w: trailing bytes", ErrNonInclusionProofMalformed)
	}
	r.BlockNumber, r.Proof = block, &p
	return nil
}

// MarshalJSON encodes the result as the hex string of its CBOR.
func (r *GetNonInclusionProofResponseV1) MarshalJSON() ([]byte, error) {
	b, err := r.MarshalCBOR()
	if err != nil {
		return nil, err
	}
	return json.Marshal(hex.EncodeToString(b))
}

// UnmarshalJSON decodes the result string; the hex length is bounded before decoding.
func (r *GetNonInclusionProofResponseV1) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("%w: the result is a string", ErrNonInclusionProofMalformed)
	}
	if len(s) > 2*NonInclusionMaxResponseBytes {
		return fmt.Errorf("%w: response too large", ErrNonInclusionProofMalformed)
	}
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		return fmt.Errorf("%w: result is not hex", ErrNonInclusionProofMalformed)
	}
	return r.UnmarshalCBOR(b)
}

// niReader is a strict, bounded reader for the fixed profile: shortest-form integers
// only, definite lengths only, lengths checked against the remaining input before any
// allocation.
type niReader struct {
	b []byte
	i int
}

func (r *niReader) head() (major byte, n uint64, err error) {
	if r.i >= len(r.b) {
		return 0, 0, errors.New("truncated")
	}
	ib := r.b[r.i]
	r.i++
	major, ai := ib>>5, ib&31
	if ai < 24 {
		return major, uint64(ai), nil
	}
	if ai > 27 {
		return 0, 0, errors.New("unsupported additional information")
	}
	size := 1 << (ai - 24)
	if r.i+size > len(r.b) {
		return 0, 0, errors.New("truncated")
	}
	var v uint64
	for _, c := range r.b[r.i : r.i+size] {
		v = v<<8 | uint64(c)
	}
	r.i += size
	if (ai == 24 && v < 24) || (ai == 25 && v <= 0xff) || (ai == 26 && v <= 0xffff) || (ai == 27 && v <= 0xffffffff) {
		return 0, 0, errors.New("integer not in shortest form")
	}
	return major, v, nil
}

// item returns the bytes of one complete data item (the unicity certificate).
func (r *niReader) item() ([]byte, error) {
	start := r.i
	var walk func(depth int) error
	walk = func(depth int) error {
		if depth > 64 {
			return errors.New("nesting too deep")
		}
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
		case 4, 5:
			if major == 5 {
				n *= 2
			}
			if n > uint64(len(r.b)-r.i) { // each element is at least one byte
				return errors.New("truncated")
			}
			for k := uint64(0); k < n; k++ {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		case 6:
			return walk(depth + 1)
		}
		return nil
	}
	if err := walk(0); err != nil {
		return nil, err
	}
	return r.b[start:r.i], nil
}

func (r *niReader) proof(p *NonInclusionProofV1) error {
	bad := func(msg string) error { return fmt.Errorf("%w: %s", ErrNonInclusionProofMalformed, msg) }
	if m, n, err := r.head(); err != nil || m != 6 || n != uint64(NonInclusionProofTag) {
		return bad("wrong tag")
	}
	if m, n, err := r.head(); err != nil || m != 4 || n != 3 {
		return bad("not a three element array")
	}
	if m, n, err := r.head(); err != nil || m != 0 || n != NonInclusionProofVersion {
		return bad("unsupported version")
	}
	m, n, err := r.head()
	if err != nil || m != 2 || n > MaxNonInclusionCertBytes || uint64(len(r.b)-r.i) < n {
		return bad("certificate is not a bounded byte string")
	}
	var cert NonInclusionCert
	if err := cert.UnmarshalBinary(r.b[r.i : r.i+int(n)]); err != nil {
		return fmt.Errorf("%w: %w", ErrNonInclusionProofMalformed, err)
	}
	r.i += int(n)
	uc, err := r.item()
	if err != nil {
		return bad("unicity certificate: " + err.Error())
	}
	*p = NonInclusionProofV1{Certificate: &cert, UnicityCertificate: bytes.Clone(uc)}
	return nil
}
