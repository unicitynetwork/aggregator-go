# Inclusion proof wire specification (v2)

Wire format for `get_inclusion_proof.v2`. Three source comments cite this
document as normative: `pkg/api/types.go` (`InclusionProofV2`), and
`pkg/api/inclusion_cert.go` (`InclusionCert`, `ExclusionCert`).

Corresponds to the Unicity yellowpaper's inclusion proof
$\pi^{\mathsf{inc}} = (\mathsf{sid}, v, C^{\mathsf{inc}}, UC)$. **The yellowpaper
is authoritative.** This document describes what the Go implementation actually
emits, and where the two differ it says so explicitly and names the paper as
correct -- it does not present an implementation gap as a specification.

## CBOR tags

| Tag | Structure |
|-----|-----------|
| 39030 | `CertificationRequest` |
| 39031 | `CertificationData` |
| 39033 | `InclusionProofV2` |

## RPC response

The `result` field of `get_inclusion_proof.v2` is a hex-encoded CBOR array:

```
[blockNumber, #39033([version, certificationDataOrNull, referenceTime, certificateBytes, unicityCertificate])]
```

`InclusionProofV2` is a tagged 5-element array:

| Index | Field | Type | Notes |
|-------|-------|------|-------|
| 0 | `version` | uint | `1` |
| 1 | `certificationData` | `#39031([...])` \| null | null ⇒ non-inclusion proof |
| 2 | `referenceTime` | uint \| null | round reference time τ; null only for non-inclusion |
| 3 | `certificateBytes` | bstr | `InclusionCert` or `ExclusionCert`, raw (below) |
| 4 | `unicityCertificate` | raw CBOR | the UC as received from the BFT Core |

**Discriminator.** `certificationData != null` ⇒ inclusion, and
`certificateBytes` is an `InclusionCert`. `certificationData == null` ⇒
non-inclusion, and `certificateBytes` is an `ExclusionCert`. Non-inclusion is
neither generated nor verified in Go, and the `ExclusionCert` layout below
diverges from the yellowpaper -- do not build against it yet.

### `CertificationData`

A tagged 6-element array. The element count never varies with the payload:

| Index | Field | Type |
|-------|-------|------|
| 0 | `version` | uint, `2` |
| 1 | `ownerPredicate` | array |
| 2 | `sourceStateHash` | bstr(32) |
| 3 | `transactionHash` | bstr(32) |
| 4 | `expiresAt` | uint \| null |
| 5 | `witness` | bstr(65) |

`expiresAt` is the exclusive request deadline τ_Q. It holds its position and is
written as CBOR `null` when the requester supplied no deadline, so the array
length never depends on the payload. Absence is distinct from zero: zero is a
legal instant.

## Leaf value

```
v = SHA-256( CBOR([ transactionHash, referenceTime ]) )
```

Raw 32 bytes, no algorithm-id prefix. Concretely the preimage is
`0x82 0x58 0x20 <32-byte transactionHash> <CBOR uint referenceTime>`.

The leaf value binds the reference time the request was validated under, not the
transaction hash alone. The tree is append-only, so a leaf can be certified
afresh against any later root and a later inclusion proof carries a later round's
`UC.IR.t`. Reference time is therefore a property of the leaf, not of the proof.

**Do not recover τ from `UC.IR.t`.** Use the `referenceTime` element. They
coincide only for the proof issued in the leaf's own round.

## `InclusionCert`

Raw binary, no framing:

```
bitmap[32] || s_1[32] || ... || s_n[32]        n = popcount(bitmap)
```

Siblings are in generation order, root-to-leaf: `s_1` is the sibling at the
shallowest depth with a bitmap bit set, `s_n` at the deepest. Verification walks
depths 255..0 and consumes siblings from the end of the slice.

The certificate carries no root, no key and no value. All three come from
outside it:

| Input | Source |
|-------|--------|
| key (sid) | the RPC request parameter |
| value | `SHA-256(CBOR([transactionHash, referenceTime]))` |
| root | `UC.IR.h` — never a field of the certificate |

Decoding rejects: fewer than 32 bytes (truncated), a remainder not a multiple of
32 (misaligned), and a sibling count disagreeing with the bitmap popcount.

## `ExclusionCert` — diverges from the yellowpaper, and is unimplemented

The Go type encodes:

```
k_l[32] || h_l[32] || bitmap[32] || s_1[32] || ... || s_n[32]
```

`appendix-hashtrees.tex` specifies the **opposite order**:

```
bitmap[32] || s_1[32] || ... || s_n[32] || k'[32] || v'
```

These are not interchangeable: one logical certificate encodes to two different
byte strings, and the Go decoder rejects the spec layout with a bitmap/popcount
mismatch. The spec puts `v'` last so the remainder after the fixed-size terminal
key is the value; with the fixed 32-byte field leading, a variable-length `v'` is
structurally unencodable here. The aggregation profile does permit
`len(v') = 32`, so only the ordering diverges — but `h_l` names the leaf **value**
`v'`, not a hash of it, which the field name obscures.

The spec's empty-tree certificate `C^exc_empty` (the empty byte string) is also
undecodable: `UnmarshalBinary(nil)` returns a truncation error, so a genesis tree
has no encodable certificate.

**Nothing generates or verifies these.** `internal/smt` exposes only
`GetInclusionCert`; `ExclusionCert.Verify` returns `ErrExclusionNotImpl`; and a
non-inclusion response carries `certificateBytes` as CBOR null (`f6`) rather than
the spec's empty byte string (`40`). Neither of the two security-critical checks
the spec names — `k' ≠ k`, and `k[d] = k'[d]` at every junction depth, with the
region taken from the authenticated terminal key `k'` — exists in this repo.

This is fail-closed: no forged absence proof is accepted because none is
accepted. But absence and not-yet-certified are indistinguishable on the wire,
and this layout should not be treated as frozen until it is reconciled with
`appendix-hashtrees.tex`.

## Hash rules

- Leaf: `H(0x00 || key || value)`
- Inner node, two children: `H(0x01 || depth_byte || region(key, depth) || left || right)`
- Inner node, one child: passthrough, child hash unchanged

`depth_byte` is the absolute branching depth as a single byte. `region(key, depth)`
is the 32-byte key prefix addressing the node: the first `depth` bits of the key,
with every bit at position ≥ `depth` cleared. At depth 0 it is 32 zero bytes; for
key `0xFFFF…` at depth 12 it is `fff00000…`.

**The region is not optional.** Omitting it reproduces the correct root only for a
tree with no binary inner node — that is, a proof with zero siblings. Any proof
carrying a sibling will verify against a different root. Inner nodes commit to
their absolute depth *and* to the region addressing them, which is what pins each
node to its position in the key space.

## Bit ordering

Big-endian (MSB-first) per the yellowpaper:

```
bit(key, d) = (key[d/8] >> (7 - d%8)) & 1
```

So bit 0 is the most significant bit of `key[0]`. Descent at depth `d` goes right
when `bit(key, d) == 1`, and the sibling supplied at that depth is then the left
child.

## Verification

`InclusionProofV2.Verify` performs, in order:

1. Non-nil proof, request, verifier context and trust base.
2. `certificationData != null`, else non-inclusion (unimplemented).
3. Request `transactionHash` present, and equal to the proof's.
4. `expiresAt` equal on both sides, treating absence as a value of its own.
5. `UC.IR.h` extractable and exactly 32 bytes.
6. `referenceTime` present.
7. If `expiresAt` is present, `referenceTime < expiresAt`. **Exclusive**: a leaf
   created at exactly the deadline is expired. When `expiresAt` is absent this
   check cannot run — the service-assigned deadline is not carried in the proof,
   is not signed, and is not checkable by any later verifier.
8. `InclusionCert.Verify(key, LeafValue(txhash, referenceTime), UC.IR.h)`.
9. Unicity Certificate verification against the trust base.

The nil-guard error strings are part of the public contract so reference
verifiers in other languages can pin them.

## Known divergence from the yellowpaper: the shard binding is not checked

**This is a soundness gap, not a caller responsibility.** An earlier revision of
this document told integrators to "derive `ExpectedShardID` from configuration".
That is not the specified check and does not close the hole.

`platform.tex` `VerifyInclusionProof` takes the partition description `CD_β` as
an input and mandates, before any tree check:

```
ensure(UC.C^r.α = T.α)
σ ← UC.C^shard.σ
ensure(σ ∈ CD_β.SH)
ensure(f_{CD_β.SH}(sid) = σ)      // Proof comes from the right shard
```

`f_SH` derives the expected shard **from the key itself**. `platform.tex`
explicitly forecloses delegating this to certificate verification:
`VerifyUnicityCert` "does not, by itself, prove that a particular state
identifier belongs to the shard named in `C^shard`; that binding is checked by
the proof verification functions below."

`InclusionProofV2.Verify` implements the tree and certificate steps but not the
binding: it compares the UC's shard against a caller-supplied
`VerifierContext.ExpectedShardID` rather than computing `f_SH(sid)`. In a
multi-shard deployment a leaf whose key routes to shard A, committed in shard
B's SMT under shard B's validly signed UC, therefore verifies — reproduced in
testing. That is cross-shard double-spend exposure. The network id
`UC.C^r.α = T.α` is likewise unchecked, so a certificate sealed for one network
verifies against another network's trust base.

Until `VerifierContext` carries the sharding scheme and `Verify` derives the
expected shard from `sid`, do not rely on this function alone for cross-shard
safety. `api.MatchesShardPrefix` implements `f_SH` and the admission path
applies it correctly.
