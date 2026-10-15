// Package consolidationsim is an in-process partition of N shard aggregators
// (real sparse Merkle trees, a real signed unicity tree and seal, real
// get_inclusion_proofs.v1 semantics over httptest servers). It exists for the
// client/CLI tests and the runnable example; it contains no production logic.
package consolidationsim

import (
	"bytes"
	"crypto"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation/client"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// PartitionID of the simulated partition.
const PartitionID types.PartitionID = 0x0a0b0c0d

// snapshot is one finalized state of a shard.
type snapshot struct {
	view  *smt.ReadView
	root  []byte
	block uint64
	// finalize is the index of the global finalization that produced it.
	finalize int
}

// Shard is one simulated shard aggregator.
type Shard struct {
	ID     types.ShardID
	URL    string
	Server *httptest.Server

	sim     *Sim
	tree    *smt.ThreadSafeSMT
	prevSeq uint64
	records map[string]*record
	snaps   []*snapshot

	mu    sync.Mutex
	calls []Call
	// Hook, when set, may handle a request itself (fault injection); return true if handled.
	Hook func(w http.ResponseWriter, body []byte) bool
	// Delay is added before answering.
	Delay time.Duration
}

// Call is one received request.
type Call struct {
	Method    string
	StateRoot string
	IDs       int
}

type record struct {
	req   *api.CertificationRequest
	ref   uint64
	final int // finalization index it appears in (>= 1), 0 while pending
}

// finalization is one global certification round of the partition.
type finalization struct {
	index     int
	seal      *types.UnicitySeal // the master seal
	localSeal *types.UnicitySeal // another genuine seal over the same unicity tree
	utc       *types.UnicityTreeCertificate
	tree      types.ShardTree
	inputs    []types.ShardTreeInput
	viewBytes []byte
}

// Sim is a simulated partition.
type Sim struct {
	Shards []*Shard
	TB     types.RootTrustBase
	Policy consolidation.Policy
	// RefTime is the reference time given to newly certified states.
	RefTime uint64
	// Retain bounds how many finalizations exact-root requests can still address (0 = all).
	Retain int
	// latestOverride makes "latest" requests serve this finalization (1-based; 0 = newest).
	latestOverride int

	mu     sync.RWMutex // Finalize against serving
	signer abcrypto.Signer
	fins   []*finalization
	round  uint64
	clock  uint64
	nreq   int
}

// New creates a simulation with n (1, 2, 4 or 8) shards and the default authority.
func New(n int) (*Sim, error) { return NewWithSeed(n, "consolidationsim") }

// NewWithSeed derives the root authority key from seed.
func NewWithSeed(n int, seed string) (*Sim, error) {
	scheme := types.ShardingScheme{}
	switch n {
	case 1:
	case 2, 4, 8:
		ids := []types.ShardID{{}}
		for range map[int]int{2: 1, 4: 2, 8: 3}[n] {
			var next []types.ShardID
			for _, id := range ids {
				a, b := id.Split()
				next = append(next, a, b)
			}
			ids = next
		}
		scheme = types.ShardingScheme(ids)
	default:
		return nil, fmt.Errorf("unsupported shard count %d", n)
	}
	key := sha256.Sum256([]byte(seed))
	signer, err := abcrypto.NewInMemorySecp256K1SignerFromKey(key[:])
	if err != nil {
		return nil, err
	}
	verifier, err := signer.Verifier()
	if err != nil {
		return nil, err
	}
	pub, err := verifier.MarshalPublicKey()
	if err != nil {
		return nil, err
	}
	tb, err := types.NewTrustBase(types.NetworkLocal, []*types.NodeInfo{{NodeID: "root-1", SigKey: pub, Stake: 1}},
		types.WithEpoch(1), types.WithEpochStart(1))
	if err != nil {
		return nil, err
	}
	s := &Sim{TB: tb, signer: signer, RefTime: types.GenesisTime + 100, clock: types.GenesisTime + 1000, round: tb.GetEpochStart() + 100}
	order, err := types.CanonicalShardOrder(scheme)
	if err != nil {
		return nil, err
	}
	s.Policy = consolidation.Policy{PartitionID: PartitionID, Topology: types.PartitionViewTopology{Scheme: scheme, ShardConfHashes: map[string][]byte{}}}
	for _, id := range order {
		h := sha256.Sum256([]byte("shard-conf-" + id.String()))
		s.Policy.Topology.ShardConfHashes[id.Key()] = h[:]
		sh := &Shard{ID: id, sim: s, records: map[string]*record{},
			tree: smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits))}
		sh.Server = httptest.NewServer(http.HandlerFunc(sh.serve))
		sh.URL = sh.Server.URL
		s.Shards = append(s.Shards, sh)
	}
	return s, nil
}

// Close stops the servers.
func (s *Sim) Close() {
	for _, sh := range s.Shards {
		sh.Server.Close()
	}
}

// Resolver is the trusted authority resolver.
func (s *Sim) Resolver() consolidation.TrustBaseResolver {
	return func(*types.UnicitySeal) (types.RootTrustBase, error) { return s.TB, nil }
}

// ClientConfig is a ready client configuration with one endpoint per shard.
func (s *Sim) ClientConfig() client.Config {
	cfg := client.Config{Policy: s.Policy, Resolver: s.Resolver()}
	for _, sh := range s.Shards {
		cfg.Endpoints = append(cfg.Endpoints, client.Endpoint{ShardID: sh.ID, URL: sh.URL})
	}
	return cfg
}

// Add creates count pending certification requests that route to the shard.
func (s *Sim) Add(shard, count int) []*api.CertificationRequest {
	sh := s.Shards[shard]
	var out []*api.CertificationRequest
	for len(out) < count {
		s.nreq++
		src := sha256.Sum256([]byte(fmt.Sprintf("source-%d", s.nreq)))
		tx := sha256.Sum256([]byte(fmt.Sprintf("tx-%d", s.nreq)))
		cd := api.CertificationData{
			Version:         api.CertificationDataVersion,
			OwnerPredicate:  api.Predicate{Engine: 1, Code: []byte{1}, Params: bytes.Repeat([]byte{2}, 33)},
			SourceStateHash: src[:],
			TransactionHash: tx[:],
			Witness:         bytes.Repeat([]byte{4}, 65),
		}
		id, err := cd.CreateStateID()
		if err != nil {
			panic(err)
		}
		key, _ := id.GetTreeKey()
		if !sh.ID.Comparator()(key) {
			continue
		}
		path, _ := id.GetPath()
		if err := sh.tree.AddLeaf(path, api.LeafValue(tx[:], s.RefTime)); err != nil {
			panic(err)
		}
		req := &api.CertificationRequest{StateID: id, CertificationData: cd}
		sh.records[string(id)] = &record{req: req, ref: s.RefTime}
		out = append(out, req)
	}
	return out
}

// Finalize certifies the current state of every shard under a new master seal
// and returns the finalization index.
func (s *Sim) Finalize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := len(s.fins) + 1
	s.round++
	s.clock += 10
	order, _ := types.CanonicalShardOrder(s.Policy.Topology.Scheme)
	view := &types.PartitionViewV1{Version: 1, PartitionID: PartitionID}
	var inputs []types.ShardTreeInput
	for i, id := range order {
		sh := s.Shards[i]
		rv := sh.tree.FreezeReadView(sh.prevSeq)
		sh.prevSeq = rv.Seq()
		sn := &snapshot{view: rv, root: rv.RootHashRaw(), block: uint64(idx), finalize: idx}
		sh.snaps = append(sh.snaps, sn)
		for _, r := range sh.records {
			if r.final == 0 {
				r.final = idx
			}
		}
		h := func(l string) []byte { x := sha256.Sum256([]byte(l + id.String())); return x[:] }
		ir := &types.InputRecord{Version: 1, RoundNumber: uint64(idx), PreviousHash: h("prev"), Hash: sn.root,
			SummaryValue: []byte{}, Timestamp: s.clock, BlockHash: h("block")}
		leaf := types.PartitionViewLeafV1{ShardID: id, InputRecord: ir, TRHash: h("tr"), ShardConfHash: s.Policy.Topology.ShardConfHashes[id.Key()]}
		view.Leaves = append(view.Leaves, leaf)
		inputs = append(inputs, types.ShardTreeInput{Shard: id, IR: ir, TRHash: leaf.TRHash, ShardConfHash: leaf.ShardConfHash})
	}
	viewBytes, err := view.MarshalCBOR()
	if err != nil {
		panic(err)
	}
	tree, err := types.CreateShardTree(s.Policy.Topology.Scheme, inputs, crypto.SHA256)
	if err != nil {
		panic(err)
	}
	other := sha256.Sum256([]byte("other partition"))
	ut, err := types.NewUnicityTree(crypto.SHA256, []*types.UnicityTreeData{
		{Partition: PartitionID, ShardTreeRoot: tree.RootHash()}, {Partition: 0x01010101, ShardTreeRoot: other[:]}})
	if err != nil {
		panic(err)
	}
	utc, err := ut.Certificate(PartitionID)
	if err != nil {
		panic(err)
	}
	mk := func(round uint64) *types.UnicitySeal {
		seal := &types.UnicitySeal{Version: 1, NetworkID: s.TB.GetNetworkID(), Epoch: s.TB.GetEpoch(), RootChainRoundNumber: round,
			Timestamp: s.clock, PreviousHash: bytes.Repeat([]byte{byte(round)}, 32), Hash: ut.RootHash()}
		if err := seal.Sign("root-1", s.signer); err != nil {
			panic(err)
		}
		return seal
	}
	s.fins = append(s.fins, &finalization{index: idx, seal: mk(s.round), localSeal: mk(s.round - 1), utc: utc, tree: tree, inputs: inputs, viewBytes: viewBytes})
	return idx
}

func (s *Sim) uc(f *finalization, shard int, local bool) []byte {
	stc, err := f.tree.Certificate(f.inputs[shard].Shard)
	if err != nil {
		panic(err)
	}
	seal := f.seal
	if local {
		seal = f.localSeal
	}
	uc := &types.UnicityCertificate{Version: 1, InputRecord: f.inputs[shard].IR, TRHash: f.inputs[shard].TRHash,
		ShardConfHash: f.inputs[shard].ShardConfHash, ShardTreeCertificate: stc, UnicityTreeCertificate: f.utc, UnicitySeal: seal}
	b, err := uc.MarshalCBOR()
	if err != nil {
		panic(err)
	}
	return b
}

// Item is the original certification of req (certified in finalization fin)
// as the shard's aggregator served it: a proof under that finalization's master seal.
func (s *Sim) Item(shard int, req *api.CertificationRequest, fin int) client.Item {
	proof, err := s.Shards[shard].proof(req.StateID, s.fins[fin-1], false)
	if err != nil {
		panic(err)
	}
	return client.Item{Request: req, Proof: proof.InclusionProof}
}

// UseFinalization makes "latest" requests serve finalization n (1-based) instead of the newest.
func (s *Sim) UseFinalization(n int) {
	s.mu.Lock()
	s.latestOverride = n
	s.mu.Unlock()
}

// Latest returns the index of the newest finalization.
func (s *Sim) Latest() int { return len(s.fins) }

// Calls returns the requests the shard has received.
func (sh *Shard) Calls() []Call {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return append([]Call(nil), sh.calls...)
}

func (sh *Shard) proof(id api.StateID, f *finalization, local bool) (*api.GetInclusionProofResponseV2, error) {
	r := sh.records[string(id)]
	if r == nil || r.final == 0 || r.final > f.index {
		return nil, errNotIncluded
	}
	var sn *snapshot
	for _, x := range sh.snaps {
		if x.finalize == f.index {
			sn = x
		}
	}
	key, _ := id.GetTreeKey()
	cert, err := sn.view.GetInclusionCert(key)
	if err != nil {
		return nil, err
	}
	cb, _ := cert.MarshalBinary()
	cd := r.req.CertificationData
	rt := r.ref
	pos := 0
	for i, x := range sh.sim.Shards {
		if x == sh {
			pos = i
		}
	}
	return &api.GetInclusionProofResponseV2{BlockNumber: sn.block, InclusionProof: &api.InclusionProofV2{
		CertificationData: &cd, ReferenceTime: &rt, CertificateBytes: cb, UnicityCertificate: sh.sim.uc(f, pos, local)}}, nil
}

var errNotIncluded = fmt.Errorf("state not included")

func rpcErr(w http.ResponseWriter, id json.RawMessage, e *jsonrpc.Error) {
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "error": e, "id": id})
}

func (sh *Shard) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	sh.mu.Lock()
	hook, delay := sh.Hook, sh.Delay
	sh.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
	}
	if hook != nil && hook(w, body) {
		return
	}
	var req struct {
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
		ID     json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	sh.sim.mu.RLock()
	defer sh.sim.mu.RUnlock()
	call := Call{Method: req.Method}
	pr, perr := api.ParseGetInclusionProofsRequestV1(req.Params)
	if perr == nil {
		call.StateRoot, call.IDs = pr.StateRoot, len(pr.StateIDs)
	}
	sh.mu.Lock()
	sh.calls = append(sh.calls, call)
	sh.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	if req.Method != client.Method {
		rpcErr(w, req.ID, jsonrpc.ErrMethodNotFound)
		return
	}
	if perr != nil {
		rpcErr(w, req.ID, jsonrpc.NewDetailedError(jsonrpc.InvalidParamsCode, "Invalid params", "invalid_request", api.DetailMalformedParams, false, nil))
		return
	}
	fins := sh.sim.fins
	if len(fins) == 0 {
		rpcErr(w, req.ID, jsonrpc.NewDetailedError(jsonrpc.ProofUnavailableCode, "Proof unavailable", "proof_unavailable", "no_finalized_view", true, nil))
		return
	}
	var f *finalization
	if pr.Root == nil {
		f = fins[len(fins)-1]
		if o := sh.sim.latestOverride; o > 0 {
			f = fins[o-1]
		}
	} else {
		for i := len(fins) - 1; i >= 0; i-- {
			if sh.sim.Retain > 0 && len(fins)-i > sh.sim.Retain {
				break
			}
			for _, sn := range sh.snaps {
				if sn.finalize == fins[i].index && bytes.Equal(sn.root, pr.Root) {
					f = fins[i]
				}
			}
			if f != nil {
				break
			}
		}
		if f == nil {
			rpcErr(w, req.ID, jsonrpc.NewDetailedError(jsonrpc.ProofUnavailableCode, "Proof unavailable", "proof_unavailable", "view_not_retained", true, nil))
			return
		}
	}
	res := struct {
		Proofs        []*api.GetInclusionProofResponseV2 `json:"proofs"`
		PartitionView api.HexBytes                       `json:"partitionView,omitempty"`
	}{}
	for _, id := range pr.StateIDs {
		p, err := sh.proof(id, f, pr.Root != nil)
		if err != nil {
			rpcErr(w, req.ID, jsonrpc.NewDetailedError(jsonrpc.ProofUnavailableCode, "Proof unavailable", "proof_unavailable", "state_not_included", true, nil))
			return
		}
		res.Proofs = append(res.Proofs, p)
	}
	if pr.Root == nil {
		res.PartitionView = f.viewBytes
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "result": res, "id": req.ID})
}

// RootHex returns the hex root of the shard's finalization fin.
func (sh *Shard) RootHex(fin int) string {
	for _, sn := range sh.snaps {
		if sn.finalize == fin {
			return hex.EncodeToString(sn.root)
		}
	}
	return ""
}
