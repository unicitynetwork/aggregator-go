package api

// ProofStatus is an unauthenticated hint about a state ID which has no inclusion proof
// (yet). It is for control flow and UX only: never evidence that a state is unspent.
type ProofStatus string

const (
	// ProofStatusIncluded: the state is in the certified state served by the response.
	ProofStatusIncluded ProofStatus = "included"
	// ProofStatusPending: accepted by this aggregator and not yet in a published certified state.
	ProofStatusPending ProofStatus = "pending"
	// ProofStatusExpired: admitted but dropped because its effective timeout passed before
	// inclusion (bounded, best-effort memory of recent drops).
	ProofStatusExpired ProofStatus = "expired"
	// ProofStatusUnknown: none of the above at this node (never seen, evicted from the recent
	// memory, admitted by another instance, or lost across a restart).
	ProofStatusUnknown ProofStatus = "unknown"
)

// IncludedProofStatusResponse is the get_inclusion_proof.v2 result with includeStatus=true:
// {"proof": "<unchanged v2 hex-CBOR response>", "status": "<value>"}. Proof is
// byte-identical to the result without the parameter in every case, including the
// empty-proof shape of states which are not included.
type IncludedProofStatusResponse struct {
	Proof  *GetInclusionProofResponseV2 `json:"proof"`
	Status ProofStatus                  `json:"status"`
}

// WaitedProofResponse is the get_inclusion_proof.v2 result of a request which carried waitMs:
// {"proof": <unchanged hex>, "status"?: ..., "waitedMs": n}. Status is present iff
// includeStatus was requested. waitedMs is unauthenticated timing metadata.
type WaitedProofResponse struct {
	Proof    *GetInclusionProofResponseV2 `json:"proof"`
	Status   *ProofStatus                 `json:"status,omitempty"`
	WaitedMs uint64                       `json:"waitedMs"`
}
