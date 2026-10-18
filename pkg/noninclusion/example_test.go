package noninclusion_test

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/noninclusion"
)

// Verification returns the authenticated snapshot; whether it is recent enough is the caller's decision, made with
// a checkpoint which does not come from the proof or the server.
//
// Non-inclusion proves absence only at the authenticated snapshot returned by verification. It does not prove absence now, continuous absence, or that a token can be spent. Compare the snapshot’s network and root round (or its certified seal timestamp under your application’s time policy) with an independently trusted requirement. The seal timestamp is a consensus-certified time, not proof of real-time freshness. Do not use unverified response metadata for this check.
func ExampleVerifier_Verify() {
	var vec struct {
		Query   string `json:"query"`
		Wrapper string `json:"wrapper"`
	}
	readExampleJSON("cases/shared-prefix.json", &vec)
	tbBytes, _ := os.ReadFile(filepath.Join(corpus, "authority/trust-base.cbor"))
	tb := &types.RootTrustBaseV1{}
	if err := tb.UnmarshalCBOR(tbBytes); err != nil {
		panic(err)
	}
	v, err := noninclusion.NewVerifier(noninclusion.Context{
		Resolver: func(*types.UnicitySeal) (types.RootTrustBase, error) { return tb, nil },
	})
	if err != nil {
		panic(err)
	}

	var resp api.GetNonInclusionProofResponseV1
	wrapper, _ := hex.DecodeString(vec.Wrapper)
	if err := resp.UnmarshalCBOR(wrapper); err != nil {
		panic(err)
	}
	query, _ := hex.DecodeString(vec.Query)
	stateID := api.StateID(query)
	proof, err := resp.Proof.ForState(stateID)
	if err != nil {
		panic(err)
	}

	res, err := v.Verify(stateID, proof)
	if err != nil {
		fmt.Println("rejected:", noninclusion.RejectionOf(err)) // no snapshot
		return
	}
	if res.Outcome() != noninclusion.OutcomeAbsentAtSnapshot {
		fmt.Println("the state is included at the snapshot")
		return
	}
	snap := res.Snapshot()
	checkpoint := struct {
		NetworkID         types.NetworkID
		RequiredRootRound uint64
	}{NetworkID: snap.NetworkID(), RequiredRootRound: 3} // obtained independently of the proof
	if snap.NetworkID() != checkpoint.NetworkID || snap.RootRound() < checkpoint.RequiredRootRound {
		fmt.Println("snapshot does not meet the application policy")
		return
	}
	fmt.Printf("absent at root round %d (partition %d, shard %q)\n", snap.RootRound(), snap.PartitionID(), snap.ShardID().String())
	// Output: absent at root round 6 (partition 7, shard "")
}

// Verification success is not absence: err == nil also covers included_at_snapshot. Switch on the outcome,
// exhaustively, before looking at the snapshot; a rejection has no result and no snapshot.
//
// Non-inclusion proves absence only at the authenticated snapshot returned by verification. It does not prove absence now, continuous absence, or that a token can be spent. Compare the snapshot’s network and root round (or its certified seal timestamp under your application’s time policy) with an independently trusted requirement. The seal timestamp is a consensus-certified time, not proof of real-time freshness. Do not use unverified response metadata for this check.
func ExampleVerifier_Verify_outcome() {
	tbBytes, _ := os.ReadFile(filepath.Join(corpus, "authority/trust-base.cbor"))
	tb := &types.RootTrustBaseV1{}
	if err := tb.UnmarshalCBOR(tbBytes); err != nil {
		panic(err)
	}
	v, err := noninclusion.NewVerifier(noninclusion.Context{
		Resolver: func(*types.UnicitySeal) (types.RootTrustBase, error) { return tb, nil },
	})
	if err != nil {
		panic(err)
	}
	for _, name := range []string{"cases/shared-prefix.json", "cases/singleton-included.json", "negative/cert-sibling-flipped.json"} {
		var vec struct {
			Query   string `json:"query"`
			Wrapper string `json:"wrapper"`
		}
		readExampleJSON(name, &vec)
		var resp api.GetNonInclusionProofResponseV1
		wrapper, _ := hex.DecodeString(vec.Wrapper)
		if err := resp.UnmarshalCBOR(wrapper); err != nil {
			panic(err)
		}
		query, _ := hex.DecodeString(vec.Query)
		proof, err := resp.Proof.ForState(api.StateID(query))
		if err != nil {
			panic(err)
		}

		res, err := v.Verify(api.StateID(query), proof)
		if err != nil { // rejected: no result, no snapshot
			fmt.Printf("%s: rejected (%s)\n", name, noninclusion.RejectionOf(err))
			continue
		}
		switch res.Outcome() { // err == nil does NOT mean absent
		case noninclusion.OutcomeAbsentAtSnapshot:
			fmt.Printf("%s: absent at root round %d\n", name, res.Snapshot().RootRound())
		case noninclusion.OutcomeIncludedAtSnapshot:
			fmt.Printf("%s: INCLUDED at root round %d (verified, but not an absence)\n", name, res.Snapshot().RootRound())
		default:
			panic("unknown outcome: " + string(res.Outcome()))
		}
	}
	// Output:
	// cases/shared-prefix.json: absent at root round 6
	// cases/singleton-included.json: INCLUDED at root round 5 (verified, but not an absence)
	// negative/cert-sibling-flipped.json: rejected (certificate_invalid)
}
