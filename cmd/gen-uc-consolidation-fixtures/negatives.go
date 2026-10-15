package main

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/consolidationcorpus"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// negative is a tampered variant of a positive case. The corpus stores the
// complete mutated files (the other files are those of the base case) and the
// expected stage and typed outcome; the generator refuses to write a negative
// whose observed behavior differs from the intended one below.
type negative struct {
	name    string
	desc    string
	stage   string
	outcome string
	// pipeline is the outcome of the full pipeline for codec stage cases.
	pipeline string
	mutate   func(b *builtCase, f consolidationcorpus.Files) error
}

const (
	stView = consolidationcorpus.StageViewCodec
	stEv   = consolidationcorpus.StageEvidence
	stCons = consolidationcorpus.StageConsolidate
	rej    = consolidationcorpus.OutcomeRejected

	invalid  = "invalid_input"
	unavail  = "unavailable"
	unauth   = "unauthenticated"
	binding  = "binding_mismatch"
	stale    = "stale"
	expired  = "expired"
	refAfter = "reference_time_after_target"
	lineage  = "incompatible_lineage"
)

type rawView struct {
	_           struct{} `cbor:",toarray"`
	Version     uint32
	PartitionID types.PartitionID
	Leaves      []types.PartitionViewLeafV1
}

func decodeView(f consolidationcorpus.Files) (*types.PartitionViewV1, error) {
	return types.DecodePartitionViewV1(f[consolidationcorpus.FilePartitionView])
}

// setView replaces the view in both files carrying it.
func setView(f consolidationcorpus.Files, view []byte) error {
	f[consolidationcorpus.FilePartitionView] = view
	var r consolidationcorpus.Responses
	if err := f.Decode(consolidationcorpus.FileResponses, &r); err != nil {
		return err
	}
	r.Master.Result.PartitionView = consolidationcorpus.Hex(view)
	return f.Encode(consolidationcorpus.FileResponses, r)
}

// mutateView changes the decoded view and encodes it without validation.
func mutateView(fn func(v *rawView)) func(b *builtCase, f consolidationcorpus.Files) error {
	return func(b *builtCase, f consolidationcorpus.Files) error {
		v, err := decodeView(f)
		if err != nil {
			return err
		}
		rv := &rawView{Version: v.Version, PartitionID: v.PartitionID, Leaves: v.Clone().Leaves}
		fn(rv)
		out, err := types.Cbor.Marshal(rv)
		if err != nil {
			return err
		}
		return setView(f, out)
	}
}

func mutateViewBytes(fn func(b []byte) []byte) func(b *builtCase, f consolidationcorpus.Files) error {
	return func(_ *builtCase, f consolidationcorpus.Files) error {
		return setView(f, fn(bytes.Clone(f[consolidationcorpus.FilePartitionView])))
	}
}

func flipFirst(b []byte) []byte {
	b[0] ^= 1
	return b
}

// leaf flips: the peer leaf (a leaf whose record changed, valid after the edit).
func leafFlip(leaf int, fn func(l *types.PartitionViewLeafV1)) func(b *builtCase, f consolidationcorpus.Files) error {
	return mutateView(func(v *rawView) { fn(&v.Leaves[leaf]) })
}

func mutResponses(f consolidationcorpus.Files, fn func(r *consolidationcorpus.Responses)) error {
	var r consolidationcorpus.Responses
	if err := f.Decode(consolidationcorpus.FileResponses, &r); err != nil {
		return err
	}
	fn(&r)
	return f.Encode(consolidationcorpus.FileResponses, r)
}

func mutRequests(f consolidationcorpus.Files, fn func(r *consolidationcorpus.Requests)) error {
	var r consolidationcorpus.Requests
	if err := f.Decode(consolidationcorpus.FileRequests, &r); err != nil {
		return err
	}
	fn(&r)
	return f.Encode(consolidationcorpus.FileRequests, r)
}

func mutContext(f consolidationcorpus.Files, fn func(c *consolidationcorpus.Context)) error {
	var c consolidationcorpus.Context
	if err := f.Decode(consolidationcorpus.FileContext, &c); err != nil {
		return err
	}
	fn(&c)
	return f.Encode(consolidationcorpus.FileContext, c)
}

// editProofs decodes the proofs at the hex slice, applies fn to each and re-encodes.
func editProofs(hexes []string, fn func(i int, p *api.GetInclusionProofResponseV2) error) error {
	for i, h := range hexes {
		p, err := consolidationcorpus.DecodeProof(h)
		if err != nil {
			return err
		}
		if err := fn(i, p); err != nil {
			return err
		}
		if hexes[i], err = consolidationcorpus.EncodeProof(p); err != nil {
			return err
		}
	}
	return nil
}

// editUC edits the UC of the master proofs.
func editMasterUC(fn func(uc *types.UnicityCertificate)) func(b *builtCase, f consolidationcorpus.Files) error {
	return func(_ *builtCase, f consolidationcorpus.Files) error {
		var ferr error
		err := mutResponses(f, func(r *consolidationcorpus.Responses) {
			ferr = editProofs(r.Master.Result.Proofs, func(_ int, p *api.GetInclusionProofResponseV2) error {
				var uc types.UnicityCertificate
				if err := types.Cbor.Unmarshal(p.InclusionProof.UnicityCertificate, &uc); err != nil {
					return err
				}
				fn(&uc)
				b, err := uc.MarshalCBOR()
				p.InclusionProof.UnicityCertificate = b
				return err
			})
		})
		if err != nil {
			return err
		}
		return ferr
	}
}

func editMasterProof(fn func(i int, p *api.GetInclusionProofResponseV2)) func(b *builtCase, f consolidationcorpus.Files) error {
	return func(_ *builtCase, f consolidationcorpus.Files) error {
		var ferr error
		err := mutResponses(f, func(r *consolidationcorpus.Responses) {
			ferr = editProofs(r.Master.Result.Proofs, func(i int, p *api.GetInclusionProofResponseV2) error { fn(i, p); return nil })
		})
		if err != nil {
			return err
		}
		return ferr
	}
}

func chain(fns ...func(b *builtCase, f consolidationcorpus.Files) error) func(b *builtCase, f consolidationcorpus.Files) error {
	return func(b *builtCase, f consolidationcorpus.Files) error {
		for _, fn := range fns {
			if err := fn(b, f); err != nil {
				return err
			}
		}
		return nil
	}
}

func editMasterRequest(fn func(i int, r *api.CertificationRequest)) func(b *builtCase, f consolidationcorpus.Files) error {
	return func(_ *builtCase, f consolidationcorpus.Files) error {
		var ferr error
		err := mutRequests(f, func(r *consolidationcorpus.Requests) {
			for i, h := range r.Master.Requests {
				req, err := consolidationcorpus.DecodeRequest(h)
				if err != nil {
					ferr = err
					return
				}
				fn(i, req)
				if r.Master.Requests[i], ferr = consolidationcorpus.EncodeRequest(req); ferr != nil {
					return
				}
			}
		})
		if err != nil {
			return err
		}
		return ferr
	}
}

func flipBytes(b []byte) []byte { c := bytes.Clone(b); c[0] ^= 0x01; return c }

// ucFieldNames of the committed input record, for the flip cases.
func irFlips() []struct {
	name string
	fn   func(ir *types.InputRecord)
} {
	return []struct {
		name string
		fn   func(ir *types.InputRecord)
	}{
		{"round", func(ir *types.InputRecord) { ir.RoundNumber++ }},
		{"epoch", func(ir *types.InputRecord) { ir.Epoch++ }},
		{"previous-hash", func(ir *types.InputRecord) { ir.PreviousHash = flipBytes(ir.PreviousHash) }},
		{"state-hash", func(ir *types.InputRecord) { ir.Hash = flipBytes(ir.Hash) }},
		{"summary-value", func(ir *types.InputRecord) { ir.SummaryValue = []byte{1} }},
		{"timestamp", func(ir *types.InputRecord) { ir.Timestamp++ }},
		{"block-hash", func(ir *types.InputRecord) { ir.BlockHash = flipBytes(ir.BlockHash) }},
		{"fees", func(ir *types.InputRecord) { ir.SumOfEarnedFees++ }},
		{"et-hash", func(ir *types.InputRecord) { ir.ETHash = bytes.Repeat([]byte{7}, 32) }},
	}
}

// negatives returns the tampered variants of the base case.
func negatives(b *builtCase) []negative {
	var out []negative
	add := func(n negative) { out = append(out, n) }
	// the leaf (canonical index = shard index) of the last peer, whose record changed in block P
	peerLeaf := b.spec.peers[len(b.spec.peers)-1]

	// partition view structure and codec
	add(negative{name: "view-missing-shard", desc: "a shard of the partition is missing from the view", stage: stCons, outcome: invalid,
		mutate: mutateView(func(v *rawView) { v.Leaves = v.Leaves[:len(v.Leaves)-1] })})
	add(negative{name: "view-duplicate-shard", desc: "a shard appears twice, another is missing", stage: stCons, outcome: invalid,
		mutate: mutateView(func(v *rawView) { v.Leaves[1] = v.Leaves[0] })})
	add(negative{name: "view-extra-shard", desc: "a shard outside the admitted topology is added", stage: stCons, outcome: invalid,
		mutate: mutateView(func(v *rawView) { v.Leaves = append(v.Leaves, v.Leaves[0]) })})
	add(negative{name: "view-reordered", desc: "leaves are not in canonical order", stage: stCons, outcome: invalid,
		mutate: mutateView(func(v *rawView) { v.Leaves[0], v.Leaves[1] = v.Leaves[1], v.Leaves[0] })})
	add(negative{name: "view-wrong-partition", desc: "view of another partition", stage: stCons, outcome: invalid,
		mutate: mutateView(func(v *rawView) { v.PartitionID++ })})
	add(negative{name: "view-wrong-version", desc: "unsupported view version", stage: stView, outcome: rej, pipeline: invalid,
		mutate: mutateView(func(v *rawView) { v.Version = 2 })})
	add(negative{name: "view-version-zero", desc: "version zero is not defaulted", stage: stView, outcome: rej, pipeline: invalid,
		mutate: mutateView(func(v *rawView) { v.Version = 0 })})
	add(negative{name: "view-truncated", desc: "truncated CBOR", stage: stView, outcome: rej, pipeline: invalid,
		mutate: mutateViewBytes(func(b []byte) []byte { return b[:len(b)-1] })})
	add(negative{name: "view-trailing-bytes", desc: "bytes after the view", stage: stView, outcome: rej, pipeline: invalid,
		mutate: mutateViewBytes(func(b []byte) []byte { return append(b, 0) })})
	add(negative{name: "view-malformed-end-marker", desc: "shard ID bit string without end marker", stage: stView, outcome: rej, pipeline: invalid,
		mutate: mutateViewBytes(func(b []byte) []byte {
			i := bytes.Index(b, []byte{0x84, 0x41})
			b[i+2] = 0
			return b
		})})
	add(negative{name: "view-over-limit-leaves", desc: "more leaves than the admission limit", stage: stView, outcome: rej, pipeline: invalid,
		mutate: mutateView(func(v *rawView) {
			for len(v.Leaves) <= types.MaxPartitionViewLeaves {
				v.Leaves = append(v.Leaves, v.Leaves[0])
			}
		})})
	add(negative{name: "view-over-limit-size", desc: "view larger than the admission limit", stage: stView, outcome: rej, pipeline: invalid,
		mutate: mutateViewBytes(func(b []byte) []byte { return append(b, make([]byte, types.MaxPartitionViewBytes)...) })})

	// every committed field of a leaf: the view no longer hashes into the seal
	for _, fl := range irFlips() {
		fl := fl
		add(negative{name: "view-flip-" + fl.name, desc: "committed input record field changed in the view leaf", stage: stCons, outcome: invalid,
			mutate: leafFlip(peerLeaf, func(l *types.PartitionViewLeafV1) { fl.fn(l.InputRecord) })})
	}
	add(negative{name: "view-flip-tr-hash", desc: "TRHash of a leaf changed", stage: stCons, outcome: invalid,
		mutate: leafFlip(peerLeaf, func(l *types.PartitionViewLeafV1) { l.TRHash = flipBytes(l.TRHash) })})
	add(negative{name: "view-flip-shard-conf-hash", desc: "ShardConfHash of a leaf differs from the admitted configuration", stage: stCons, outcome: invalid,
		mutate: leafFlip(peerLeaf, func(l *types.PartitionViewLeafV1) { l.ShardConfHash = flipBytes(l.ShardConfHash) })})

	// the enclosing UC
	add(negative{name: "uc-flip-partition-path-hash", desc: "sibling hash of the partition authentication path changed", stage: stCons, outcome: invalid,
		mutate: editMasterUC(func(uc *types.UnicityCertificate) {
			uc.UnicityTreeCertificate.HashSteps[0].Hash = flipBytes(uc.UnicityTreeCertificate.HashSteps[0].Hash)
		})})
	add(negative{name: "uc-swap-partition-certificate", desc: "partition certificate of another partition", stage: stCons, outcome: invalid,
		mutate: editMasterUC(func(uc *types.UnicityCertificate) { uc.UnicityTreeCertificate.Partition = 0x01010101 })})
	add(negative{name: "uc-flip-shard-sibling", desc: "sibling hash of the shard authentication path changed", stage: stCons, outcome: invalid,
		mutate: editMasterUC(func(uc *types.UnicityCertificate) {
			if n := len(uc.ShardTreeCertificate.SiblingHashes); n > 0 {
				uc.ShardTreeCertificate.SiblingHashes[0] = flipBytes(uc.ShardTreeCertificate.SiblingHashes[0])
			} else {
				uc.TRHash = flipBytes(uc.TRHash)
			}
		})})
	for _, s := range []struct {
		name    string
		outcome string
		fn      func(s *types.UnicitySeal)
	}{
		{"hash", invalid, func(s *types.UnicitySeal) { s.Hash = flipBytes(s.Hash) }},
		{"round", unauth, func(s *types.UnicitySeal) { s.RootChainRoundNumber++ }},
		{"network", unauth, func(s *types.UnicitySeal) { s.NetworkID++ }},
		{"epoch", unauth, func(s *types.UnicitySeal) { s.Epoch++ }},
		{"timestamp", unauth, func(s *types.UnicitySeal) { s.Timestamp++ }},
		{"previous-hash", unauth, func(s *types.UnicitySeal) { s.PreviousHash = flipBytes(s.PreviousHash) }},
		{"signature-bytes", unauth, func(s *types.UnicitySeal) {
			for k, v := range s.Signatures {
				s.Signatures[k] = flipBytes(v)
			}
		}},
		{"signatures-missing", invalid, func(s *types.UnicitySeal) { s.Signatures = nil }},
		{"signatures-below-quorum", unauth, func(s *types.UnicitySeal) {
			keys := make([]string, 0, len(s.Signatures))
			for k := range s.Signatures {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, k := range keys[1:] {
				delete(s.Signatures, k)
			}
		}},
		{"signature-alien-key", unauth, func(s *types.UnicitySeal) {
			alien := types.SignatureMap{}
			for k, v := range s.Signatures {
				alien[k+"x"] = v
			}
			s.Signatures = alien
		}},
	} {
		s := s
		add(negative{name: "seal-" + s.name, desc: "seal field changed without re-signing: " + s.name, stage: stCons, outcome: s.outcome,
			mutate: editMasterUC(func(uc *types.UnicityCertificate) {
				uc.UnicitySeal.Signatures = cloneSigs(uc.UnicitySeal.Signatures)
				s.fn(uc.UnicitySeal)
			})})
	}

	// trusted context
	add(negative{name: "context-wrong-config-participant", desc: "admitted configuration hash of a participant differs", stage: stEv, outcome: lineage,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			return mutContext(f, func(c *consolidationcorpus.Context) {
				for i := range c.ShardConfigs {
					if c.ShardConfigs[i].Shard == c.Master {
						h, _ := consolidationcorpus.UnHex(c.ShardConfigs[i].Hash)
						c.ShardConfigs[i].Hash = consolidationcorpus.Hex(flipBytes(h))
					}
				}
			})
		}})
	add(negative{name: "context-wrong-partition", desc: "policy partition differs from the evidence", stage: stEv, outcome: lineage,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			return mutContext(f, func(c *consolidationcorpus.Context) { c.PartitionID++ })
		}})
	add(negative{name: "trust-base-other-authority", desc: "trusted authority of other keys", stage: stEv, outcome: unauth,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			return replaceTrustBaseKeys(f)
		}})
	add(negative{name: "trust-base-other-network", desc: "same keys under another network", stage: stEv, outcome: unauth,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			tb := &types.RootTrustBaseV1{}
			if err := tb.UnmarshalCBOR(f[consolidationcorpus.FileTrustBase]); err != nil {
				return err
			}
			tb.NetworkID++
			bs, err := tb.MarshalCBOR()
			f[consolidationcorpus.FileTrustBase] = bs
			return err
		}})
	add(negative{name: "trust-base-other-epoch", desc: "same keys in another epoch", stage: stEv, outcome: unauth,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			tb := &types.RootTrustBaseV1{}
			if err := tb.UnmarshalCBOR(f[consolidationcorpus.FileTrustBase]); err != nil {
				return err
			}
			tb.Epoch++
			bs, err := tb.MarshalCBOR()
			f[consolidationcorpus.FileTrustBase] = bs
			return err
		}})

	// evidence and R_min
	add(negative{name: "rmin-unknown", desc: "no original evidence: R_min is unknown", stage: stEv, outcome: unavail,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			return mutRequests(f, func(r *consolidationcorpus.Requests) { r.Evidence = nil })
		}})
	add(negative{name: "rmin-master-stale", desc: "evidence newer than the master seal", stage: stCons, outcome: stale,
		mutate: func(bc *builtCase, f consolidationcorpus.Files) error {
			return mutRequests(f, func(r *consolidationcorpus.Requests) { r.Evidence = append(r.Evidence, bc.lateEvidence) })
		}})
	add(negative{name: "evidence-reference-time", desc: "original proof with a lowered reference time", stage: stEv, outcome: binding,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			var ferr error
			err := mutRequests(f, func(r *consolidationcorpus.Requests) {
				ps := evidenceProofs(r)
				ferr = editProofs(ps, func(i int, p *api.GetInclusionProofResponseV2) error {
					if i == 0 {
						*p.InclusionProof.ReferenceTime--
					}
					return nil
				})
				applyEvidenceProofs(r, ps)
			})
			if err != nil {
				return err
			}
			return ferr
		}})
	add(negative{name: "evidence-reference-time-after-target", desc: "original proof with a reference time later than its own record timestamp", stage: stEv, outcome: refAfter,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			var ferr error
			err := mutRequests(f, func(r *consolidationcorpus.Requests) {
				ps := evidenceProofs(r)
				ferr = editProofs(ps, func(i int, p *api.GetInclusionProofResponseV2) error {
					if i == 0 {
						*p.InclusionProof.ReferenceTime++
					}
					return nil
				})
				applyEvidenceProofs(r, ps)
			})
			if err != nil {
				return err
			}
			return ferr
		}})
	add(negative{name: "evidence-missing-for-request", desc: "a consolidated request has no authenticated original evidence", stage: stCons, outcome: lineage,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			return mutRequests(f, func(r *consolidationcorpus.Requests) { r.Evidence = r.Evidence[1:] })
		}})
	add(negative{name: "evidence-duplicate", desc: "the same original evidence twice", stage: stEv, outcome: invalid,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			return mutRequests(f, func(r *consolidationcorpus.Requests) { r.Evidence = append(r.Evidence, r.Evidence[0]) })
		}})
	add(negative{name: "evidence-null-path-step", desc: "null entry in the partition path of an original proof", stage: stEv, outcome: invalid,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			var ferr error
			err := mutRequests(f, func(r *consolidationcorpus.Requests) {
				p, err := consolidationcorpus.DecodeProof(r.Evidence[0].Proof)
				if err != nil {
					ferr = err
					return
				}
				var uc types.UnicityCertificate
				if ferr = types.Cbor.Unmarshal(p.InclusionProof.UnicityCertificate, &uc); ferr != nil {
					return
				}
				uc.UnicityTreeCertificate.HashSteps = []*types.PathItem{nil}
				if p.InclusionProof.UnicityCertificate, ferr = uc.MarshalCBOR(); ferr != nil {
					return
				}
				r.Evidence[0].Proof, ferr = consolidationcorpus.EncodeProof(p)
			})
			if err != nil {
				return err
			}
			return ferr
		}})
	add(negative{name: "uc-null-partition-path-step", desc: "null entry in the partition path of the enclosing UC", stage: stCons, outcome: invalid,
		mutate: editMasterUC(func(uc *types.UnicityCertificate) { uc.UnicityTreeCertificate.HashSteps = []*types.PathItem{nil} })})
	add(negative{name: "view-absent", desc: "master response without partition view", stage: stCons, outcome: unavail,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			return mutResponses(f, func(r *consolidationcorpus.Responses) { r.Master.Result.PartitionView = "" })
		}})

	// proofs
	add(negative{name: "proof-missing", desc: "a requested proof is missing", stage: stCons, outcome: invalid,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			return mutResponses(f, func(r *consolidationcorpus.Responses) {
				r.Master.Result.Proofs = r.Master.Result.Proofs[:len(r.Master.Result.Proofs)-1]
			})
		}})
	add(negative{name: "proof-unrequested", desc: "a proof nobody requested", stage: stCons, outcome: invalid,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			return mutResponses(f, func(r *consolidationcorpus.Responses) {
				r.Master.Result.Proofs = append(r.Master.Result.Proofs, r.Master.Result.Proofs[0])
			})
		}})
	add(negative{name: "proof-duplicate", desc: "the same proof twice instead of two proofs", stage: stCons, outcome: binding,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			return mutResponses(f, func(r *consolidationcorpus.Responses) { r.Master.Result.Proofs[1] = r.Master.Result.Proofs[0] })
		}})
	add(negative{name: "proof-reordered", desc: "proofs not in request order", stage: stCons, outcome: binding,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			return mutResponses(f, func(r *consolidationcorpus.Responses) {
				p := r.Master.Result.Proofs
				p[0], p[1] = p[1], p[0]
			})
		}})
	add(negative{name: "proof-wrong-leaf-value", desc: "transaction hash of the proof's certification data changed (request unchanged)", stage: stCons, outcome: binding,
		mutate: editMasterProof(func(i int, p *api.GetInclusionProofResponseV2) {
			if i == 0 {
				p.InclusionProof.CertificationData.TransactionHash = api.RequireNewImprintV2(fmt.Sprintf("%064x", 0xabc))
			}
		})})
	add(negative{name: "request-differs-from-evidence", desc: "request whose certification data differs from the authenticated original", stage: stCons, outcome: lineage,
		mutate: editMasterRequest(func(i int, r *api.CertificationRequest) {
			if i == 0 {
				r.CertificationData.TransactionHash = api.RequireNewImprintV2(fmt.Sprintf("%064x", 0xabc))
			}
		})})
	add(negative{name: "proof-wrong-reference-time", desc: "reference time lowered by one", stage: stCons, outcome: binding,
		mutate: editMasterProof(func(i int, p *api.GetInclusionProofResponseV2) {
			if i == 0 {
				*p.InclusionProof.ReferenceTime--
			}
		})})
	add(negative{name: "proof-reference-time-after-target", desc: "reference time later than the target timestamp", stage: stCons, outcome: refAfter,
		mutate: editMasterProof(func(i int, p *api.GetInclusionProofResponseV2) {
			if i == 0 {
				*p.InclusionProof.ReferenceTime = ^uint64(0) >> 1
			}
		})})
	add(negative{name: "proof-wrong-expiry", desc: "expiry of the proof differs from the request", stage: stCons, outcome: binding,
		mutate: editMasterProof(func(i int, p *api.GetInclusionProofResponseV2) {
			if i == 0 {
				e := uint64(1) << 62
				p.InclusionProof.CertificationData.ExpiresAt = &e
			}
		})})
	add(negative{name: "evidence-expired", desc: "original request expired at its reference time", stage: stEv, outcome: expired,
		mutate: func(_ *builtCase, f consolidationcorpus.Files) error {
			var ferr error
			err := mutRequests(f, func(r *consolidationcorpus.Requests) {
				ev := &r.Evidence[0]
				req, err := consolidationcorpus.DecodeRequest(ev.Request)
				if err != nil {
					ferr = err
					return
				}
				p, err := consolidationcorpus.DecodeProof(ev.Proof)
				if err != nil {
					ferr = err
					return
				}
				e := *p.InclusionProof.ReferenceTime
				req.CertificationData.ExpiresAt = &e
				p.InclusionProof.CertificationData.ExpiresAt = &e
				if ev.Request, ferr = consolidationcorpus.EncodeRequest(req); ferr != nil {
					return
				}
				ev.Proof, ferr = consolidationcorpus.EncodeProof(p)
			})
			if err != nil {
				return err
			}
			return ferr
		}})
	add(negative{name: "request-wrong-state-id", desc: "request with another state ID, not covered by the evidence", stage: stCons, outcome: lineage,
		mutate: editMasterRequest(func(i int, r *api.CertificationRequest) {
			if i == 0 {
				r.StateID = api.RequireNewImprintV2(fmt.Sprintf("%064x", 0xdef))
			}
		})})
	add(negative{name: "proof-wrong-certificate", desc: "certificate of another request (path does not fold to the target root)", stage: stCons, outcome: binding,
		mutate: func(bc *builtCase, f consolidationcorpus.Files) error {
			other, _ := consolidationcorpus.UnHex(bc.otherCert)
			return editMasterProof(func(i int, p *api.GetInclusionProofResponseV2) {
				if i == 0 {
					p.InclusionProof.CertificateBytes = other
				}
			})(bc, f)
		}})
	add(negative{name: "peer-root-substitution", desc: "peer proofs of an older state root instead of the root of the view", stage: stCons, outcome: binding,
		mutate: func(bc *builtCase, f consolidationcorpus.Files) error {
			return mutResponses(f, func(r *consolidationcorpus.Responses) {
				old := bc.oldPeerProofs
				n := len(r.Peers[0].Response.Result.Proofs)
				var ps []string
				for len(ps) < n {
					ps = append(ps, old[len(ps)%len(old)])
				}
				r.Peers[0].Response.Result.Proofs = ps
			})
		}})
	add(negative{name: "peer-proof-of-other-shard", desc: "peer response carries the master's proofs", stage: stCons, outcome: binding,
		mutate: func(bc *builtCase, f consolidationcorpus.Files) error {
			return mutResponses(f, func(r *consolidationcorpus.Responses) {
				m := r.Master.Result.Proofs
				n := len(r.Peers[0].Response.Result.Proofs)
				for len(m) < n {
					m = append(m, m[0])
				}
				r.Peers[0].Response.Result.Proofs = slices.Clone(m[:n])
			})
		}})
	return out
}

func cloneSigs(s types.SignatureMap) types.SignatureMap {
	c := types.SignatureMap{}
	for k, v := range s {
		c[k] = bytes.Clone(v)
	}
	return c
}

func evidenceProofs(r *consolidationcorpus.Requests) []string {
	out := make([]string, len(r.Evidence))
	for i, e := range r.Evidence {
		out[i] = e.Proof
	}
	return out
}

func applyEvidenceProofs(r *consolidationcorpus.Requests, ps []string) {
	for i := range r.Evidence {
		r.Evidence[i].Proof = ps[i]
	}
}

// replaceTrustBaseKeys installs an authority of other (public, deterministic) keys.
func replaceTrustBaseKeys(f consolidationcorpus.Files) error {
	tb, err := otherAuthority()
	if err != nil {
		return err
	}
	bs, err := tb.MarshalCBOR()
	f[consolidationcorpus.FileTrustBase] = bs
	return err
}
