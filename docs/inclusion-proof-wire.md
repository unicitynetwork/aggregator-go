# Inclusion proof wire specification (v2)

Frozen wire format for `get_inclusion_proof.v2`. Three source comments cite this
document as normative: `pkg/api/types.go` (`InclusionProofV2`), and
`pkg/api/inclusion_cert.go` (`InclusionCert`, `ExclusionCert`).

Corresponds to the Unicity yellowpaper's inclusion proof
$\pi^{\mathsf{inc}} = (\mathsf{sid}, v, C^{\mathsf{inc}}, UC)$. Where this
document and the yellowpaper disagree about intent, the yellowpaper wins; where
they disagree about bytes, this document describes what the Go implementation
actually emits.

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
non-inclusion, and `certificateBytes` is an `ExclusionCert`. Non-inclusion
verification is not implemented in Go; the codec is frozen so clients can decode
today.

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

## `ExclusionCert`

```
k_l[32] || h_l[32] || bitmap[32] || s_1[32] || ... || s_n[32]
```

`(k_l, h_l)` is the witness leaf present in the tree at the position reached when
routing the query key. `bitmap` and siblings describe the path from the root to
that position, under the same root-to-leaf ordering as `InclusionCert`.

## Hash rules

- Leaf: `H(0x00 || key || value)`
- Inner node, two children: `H(0x01 || depth_byte || left || right)`
- Inner node, one child: passthrough, child hash unchanged

Bit ordering is big-endian per the yellowpaper.

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

`Verify` does **not** check that `sid` routes to the expected shard, though
`api.MatchesShardPrefix` exists and the admission path applies it. A caller that
derives `ExpectedShardID` from the proof's own UC would accept a leaf certified
by the wrong shard; derive it from configuration instead.
