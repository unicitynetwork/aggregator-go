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
