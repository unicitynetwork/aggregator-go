package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/unicitynetwork/bft-core/rootchain/testutils/ucfixture"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/consolidationcorpus"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const fixturePartition types.PartitionID = api.AggregationPartitionID

// Root chain rounds of the case timeline.
const (
	roundE = 5  // first batch: every participant certifies state S1
	roundP = 9  // second batch: every participant certifies state S2
	roundF = 12 // master empty block record: the master's latest UC (the view's enclosing UC)
	roundL = 20 // a later block, source of evidence newer than the master seal
)

// caseSpec describes one positive case.
type caseSpec struct {
	name string
	// salt seeds the transactions (default: the name); cases with the same
	// salt and content differ only in what the spec varies.
	salt   string
	desc   string
	bits   int
	master int
	peers  []int
	// txs per participant: requests of the first and the second batch.
	txs map[int][2]int
	// baseTime is the base of the seal timestamps of the case.
	baseTime uint64
	fees     uint64
	// fSigners signs the master block F (default: first quorum).
	fSigners []int
	// fExtra are changes of other shards in block F (timeouts, empty records, writes).
	fExtra []ucfixture.Change
	// etHash per participant shard, nil keeps the field absent.
	etHash map[int][]byte
	// partition overrides the fixture partition: only for the validly signed wrong-partition negative.
	partition types.PartitionID
}

func (s caseSpec) participants() []int { return append([]int{s.master}, s.peers...) }

type tx struct {
	req   api.CertificationRequest
	key   []byte
	batch int // 0 first, 1 second, 2 late
	ref   uint64
}

// builtCase is a positive case with the material the negatives are derived from.
type builtCase struct {
	spec  caseSpec
	files consolidationcorpus.Files
	// lateEvidence is an original proof newer than the master seal (root round roundL).
	lateEvidence consolidationcorpus.Evidence
	// firstBatch are original (round E) proofs of the peers, used to substitute roots.
	oldPeerProofs []string
	otherCert     string // hex of a certificate of another request
}

func shardFile(s string) string {
	if s == "" {
		return "root"
	}
	return s
}

func hash32(parts ...any) []byte {
	h := sha256.New()
	for _, p := range parts {
		switch v := p.(type) {
		case string:
			h.Write([]byte(v))
		case int:
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(v))
			h.Write(b[:])
		case []byte:
			h.Write(v)
		}
	}
	return h.Sum(nil)
}

func genRequests(shard types.ShardID, n int, salt string, expiresAt *uint64, taken map[string]bool) ([]api.CertificationRequest, error) {
	var out []api.CertificationRequest
	for c := 0; len(out) < n; c++ {
		cd := api.CertificationData{
			Version: api.CertificationDataVersion,
			OwnerPredicate: api.Predicate{
				Engine: 1, Code: []byte{1}, Params: append([]byte{0x02}, hash32("owner", salt)...),
			},
			SourceStateHash: hash32("source", salt, c),
			TransactionHash: api.RequireNewImprintV2(hex.EncodeToString(hash32("tx", salt, c))),
			Witness:         append(append(hash32("witness", salt, c), hash32("witness2", salt, c)...), 0x1b),
			ExpiresAt:       expiresAt,
		}
		sid, err := cd.CreateStateID()
		if err != nil {
			return nil, err
		}
		key, err := sid.GetTreeKey()
		if err != nil {
			return nil, err
		}
		if shard.Length() > 0 && !shard.Comparator()(key) {
			continue
		}
		if taken[string(key)] {
			continue
		}
		taken[string(key)] = true
		out = append(out, api.CertificationRequest{Version: 1, StateID: sid, CertificationData: cd})
	}
	return out, nil
}

type shardSMT struct {
	be    *backend.MemoryBackend
	block uint64
	txs   []*tx
}

func newShardSMT() *shardSMT {
	return &shardSMT{be: backend.NewMemoryBackend(smt.NewThreadSafeSMT(smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)))}
}

// add commits the transactions as the next block with the reference time and returns the new root.
func (s *shardSMT) add(txs []*tx, ref uint64) ([]byte, error) {
	ctx := context.Background()
	snap, err := s.be.CreateSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	var in []backend.LeafInput
	for _, t := range txs {
		t.ref = ref
		in = append(in, backend.LeafInput{Key: t.key, Value: api.LeafValue(t.req.CertificationData.TransactionHash.DataBytes(), ref)})
	}
	res, err := snap.AddLeavesClassified(ctx, in)
	if err != nil {
		return nil, err
	}
	if len(res.Rejected) != 0 || len(res.AcceptedIndexes) != len(in) {
		return nil, fmt.Errorf("leaves not accepted: %+v", res)
	}
	s.block++
	if err := snap.Commit(ctx, backend.CommitMetadata{BlockNumber: api.NewBigIntFromUint64(s.block), RootHash: res.CandidateRoot}); err != nil {
		return nil, err
	}
	s.txs = append(s.txs, txs...)
	return res.CandidateRoot, nil
}

func (s *shardSMT) cert(t *tx) ([]byte, error) {
	c, err := s.be.GetInclusionCert(context.Background(), t.key)
	if err != nil {
		return nil, err
	}
	return c.MarshalBinary()
}

func proofOf(t *tx, cert []byte, uc []byte, block uint64) *api.GetInclusionProofResponseV2 {
	cd := t.req.CertificationData
	ref := t.ref
	return &api.GetInclusionProofResponseV2{BlockNumber: block, InclusionProof: &api.InclusionProofV2{
		Version: 1, CertificationData: cd.Clone(), ReferenceTime: &ref, CertificateBytes: cert, UnicityCertificate: uc,
	}}
}

// producerSpec is producer.json: the replayable description of the root chain
// blocks, so that bft-core can regenerate the producer material byte for byte
// without access to the aggregator state trees.
type producerSpec struct {
	Partition uint32          `json:"partitionId"`
	ShardBits int             `json:"shardBits"`
	Blocks    []producerBlock `json:"blocks"`
}

type producerBlock struct {
	Name      string           `json:"name"`
	Round     uint64           `json:"round,string"`
	Timestamp uint64           `json:"timestamp,string"`
	Signers   []int            `json:"signers,omitempty"`
	Changes   []producerChange `json:"changes"`
}

type producerChange struct {
	Shard   int    `json:"shard"`
	Timeout bool   `json:"timeout,omitempty"`
	Hash    string `json:"hash,omitempty"`
	Fees    uint64 `json:"fees,string,omitempty"`
	Summary string `json:"summary,omitempty"`
	// ETHash is "" (absent, nil) or "hex" (possibly the empty string "0x" marker for empty non-nil)
	ETHash *string `json:"etHash,omitempty"`
}

func toProducerChange(c ucfixture.Change) producerChange {
	pc := producerChange{Shard: c.Shard, Timeout: c.Timeout, Hash: hex.EncodeToString(c.Hash), Fees: c.Fees, Summary: hex.EncodeToString(c.SummaryValue)}
	if c.ETHash != nil {
		s := hex.EncodeToString(c.ETHash)
		pc.ETHash = &s
	}
	return pc
}

// buildCase runs the timeline of the spec and produces the case files.
func buildCase(spec caseSpec) (*builtCase, error) {
	ctx := context.Background()
	_ = ctx
	partition := fixturePartition
	if spec.partition != 0 {
		partition = spec.partition
	}
	prod, err := ucfixture.New(ucfixture.Config{Partition: partition, ShardBits: spec.bits})
	if err != nil {
		return nil, err
	}
	shards := prod.Shards()
	base := spec.baseTime
	if base == 0 {
		base = ucfixture.GenesisTimestamp
	}
	tsE, tsP, tsF, tsL := base+100, base+200, base+300, base+400

	salt := spec.salt
	if salt == "" {
		salt = spec.name
	}
	// transactions and state trees of the participants
	taken := map[string]bool{}
	smts := map[int]*shardSMT{}
	var first, second map[int][]*tx
	first, second = map[int][]*tx{}, map[int][]*tx{}
	late := map[int]*tx{}
	for _, i := range spec.participants() {
		n := spec.txs[i]
		reqs, err := genRequests(shards[i].ID, n[0]+n[1]+1, fmt.Sprintf("%s/%d", salt, i), nil, taken)
		if err != nil {
			return nil, err
		}
		for j := range reqs {
			key, _ := reqs[j].StateID.GetTreeKey()
			t := &tx{req: reqs[j], key: key}
			switch {
			case j < n[0]:
				t.batch = 0
				first[i] = append(first[i], t)
			case j < n[0]+n[1]:
				t.batch = 1
				second[i] = append(second[i], t)
			default:
				t.batch = 2
				late[i] = t
			}
		}
		smts[i] = newShardSMT()
	}

	// timeline
	e := ucfixture.BlockSpec{Round: roundE, Timestamp: tsE}
	p := ucfixture.BlockSpec{Round: roundP, Timestamp: tsP}
	f := ucfixture.BlockSpec{Round: roundF, Timestamp: tsF, Signers: spec.fSigners}
	l := ucfixture.BlockSpec{Round: roundL, Timestamp: tsL}
	roots1, roots2 := map[int][]byte{}, map[int][]byte{}
	type evidenceCert struct {
		t    *tx
		cert []byte
		blk  int // 0 = E, 1 = P
	}
	var evidenceCerts []evidenceCert
	for _, i := range spec.participants() {
		r1, err := smts[i].add(first[i], ucfixture.GenesisTimestamp)
		if err != nil {
			return nil, err
		}
		roots1[i] = r1
		for _, t := range first[i] {
			c, err := smts[i].cert(t)
			if err != nil {
				return nil, err
			}
			evidenceCerts = append(evidenceCerts, evidenceCert{t, c, 0})
		}
		e.Changes = append(e.Changes, ucfixture.Change{Shard: i, Hash: r1, Fees: spec.fees + uint64(i)})
	}
	blkE, err := prod.Next(e)
	if err != nil {
		return nil, err
	}
	for _, i := range spec.participants() {
		r2, err := smts[i].add(second[i], tsE)
		if err != nil {
			return nil, err
		}
		roots2[i] = r2
		for _, t := range second[i] {
			c, err := smts[i].cert(t)
			if err != nil {
				return nil, err
			}
			evidenceCerts = append(evidenceCerts, evidenceCert{t, c, 1})
		}
		p.Changes = append(p.Changes, ucfixture.Change{Shard: i, Hash: r2, Fees: spec.fees + uint64(i), ETHash: spec.etHash[i]})
	}
	blkP, err := prod.Next(p)
	if err != nil {
		return nil, err
	}
	// the master's empty block record produces its latest UC
	f.Changes = append([]ucfixture.Change{{Shard: spec.master, Hash: roots2[spec.master], ETHash: spec.etHash[spec.master]}}, spec.fExtra...)
	blkF, err := prod.Next(f)
	if err != nil {
		return nil, err
	}
	// target proofs at the state S2 of every participant
	type shardProofs struct {
		shard  int
		reqs   []string
		proofs []string
	}
	targets := map[int]*shardProofs{}
	otherCert := ""
	for _, i := range spec.participants() {
		sp := &shardProofs{shard: i}
		uc, block := blkP.Shards[i].UCBytes, uint64(2)
		if i == spec.master {
			uc, block = blkF.Shards[i].UCBytes, 3
		}
		for _, t := range append(append([]*tx{}, first[i]...), second[i]...) {
			c, err := smts[i].cert(t)
			if err != nil {
				return nil, err
			}
			if otherCert == "" || i != spec.master {
				otherCert = hex.EncodeToString(c)
			}
			rh, err := consolidationcorpus.EncodeRequest(&t.req)
			if err != nil {
				return nil, err
			}
			ph, err := consolidationcorpus.EncodeProof(proofOf(t, c, uc, block))
			if err != nil {
				return nil, err
			}
			sp.reqs, sp.proofs = append(sp.reqs, rh), append(sp.proofs, ph)
		}
		targets[i] = sp
	}
	// late block: a new transaction on a participant (original proof newer than the master seal)
	lateShard := spec.master
	if len(spec.peers) > 0 {
		lateShard = spec.peers[0]
	}
	r3, err := smts[lateShard].add([]*tx{late[lateShard]}, tsF)
	if err != nil {
		return nil, err
	}
	l.Changes = []ucfixture.Change{{Shard: lateShard, Hash: r3}}
	blkL, err := prod.Next(l)
	if err != nil {
		return nil, err
	}
	lc, err := smts[lateShard].cert(late[lateShard])
	if err != nil {
		return nil, err
	}
	var lateEv consolidationcorpus.Evidence
	if lateEv.Request, err = consolidationcorpus.EncodeRequest(&late[lateShard].req); err != nil {
		return nil, err
	}
	if lateEv.Proof, err = consolidationcorpus.EncodeProof(proofOf(late[lateShard], lc, blkL.Shards[lateShard].UCBytes, 4)); err != nil {
		return nil, err
	}

	// files
	files := consolidationcorpus.Files{}
	ctxFile := consolidationcorpus.Context{
		CorpusVersion: consolidationcorpus.Version, NetworkID: uint32(ucfixture.NetworkID), PartitionID: uint32(partition),
		Master: consolidationcorpus.ShardName(shards[spec.master].ID), TrustBase: consolidationcorpus.FileTrustBase,
	}
	for _, s := range shards {
		name := consolidationcorpus.ShardName(s.ID)
		ctxFile.Shards = append(ctxFile.Shards, name)
		file := consolidationcorpus.DirShardConfigs + shardFile(name) + ".cbor"
		files[file] = s.ConfigBytes
		ctxFile.ShardConfigs = append(ctxFile.ShardConfigs, consolidationcorpus.ShardConfig{Shard: name, File: file, Hash: consolidationcorpus.Hex(s.ConfigHash)})
		files[consolidationcorpus.DirUCs+shardFile(name)+".cbor"] = blkF.Shards[indexOf(shards, s.ID)].UCBytes
	}
	for _, i := range spec.participants() {
		ctxFile.Participants = append(ctxFile.Participants, consolidationcorpus.ShardName(shards[i].ID))
	}
	if err := files.Encode(consolidationcorpus.FileContext, ctxFile); err != nil {
		return nil, err
	}
	if files[consolidationcorpus.FileTrustBase], err = prod.TrustBaseCBOR(); err != nil {
		return nil, err
	}
	pspec := producerSpec{Partition: uint32(partition), ShardBits: spec.bits}
	for _, b := range []struct {
		name string
		s    ucfixture.BlockSpec
	}{{"E", e}, {"P", p}, {"F", f}, {"L", l}} {
		pb := producerBlock{Name: b.name, Round: b.s.Round, Timestamp: b.s.Timestamp, Signers: b.s.Signers}
		for _, c := range b.s.Changes {
			pb.Changes = append(pb.Changes, toProducerChange(c))
		}
		pspec.Blocks = append(pspec.Blocks, pb)
	}
	if err := files.Encode(consolidationcorpus.FileProducer, pspec); err != nil {
		return nil, err
	}
	mf := blkF.Shards[spec.master]
	files[consolidationcorpus.FileResponseV1], files[consolidationcorpus.FileResponseV2] = mf.ResponseV1, mf.ResponseV2
	files[consolidationcorpus.FilePartitionView] = blkF.View

	var reqs consolidationcorpus.Requests
	var resps consolidationcorpus.Responses
	m := targets[spec.master]
	reqs.Master = consolidationcorpus.Batch{Shard: consolidationcorpus.ShardName(shards[spec.master].ID), Requests: m.reqs}
	resps.Master = consolidationcorpus.RPCResponse{JSONRPC: "2.0", ID: 1, Result: &consolidationcorpus.RPCResult{Proofs: m.proofs, PartitionView: consolidationcorpus.Hex(blkF.View)}}
	for k, i := range spec.peers {
		sp := targets[i]
		name := consolidationcorpus.ShardName(shards[i].ID)
		reqs.Peers = append(reqs.Peers, consolidationcorpus.Batch{Shard: name, Requests: sp.reqs})
		resps.Peers = append(resps.Peers, consolidationcorpus.PeerResponse{Shard: name,
			Response: consolidationcorpus.RPCResponse{JSONRPC: "2.0", ID: 2 + k, Result: &consolidationcorpus.RPCResult{Proofs: sp.proofs}}})
	}
	var oldPeer []string
	for _, ec := range evidenceCerts {
		uc := blkE.Shards
		_ = uc
		var ucb []byte
		var block uint64
		shardIdx := shardOfTx(spec, first, second, ec.t)
		if ec.blk == 0 {
			ucb, block = blkE.Shards[shardIdx].UCBytes, 1
		} else {
			ucb, block = blkP.Shards[shardIdx].UCBytes, 2
		}
		rh, err := consolidationcorpus.EncodeRequest(&ec.t.req)
		if err != nil {
			return nil, err
		}
		ph, err := consolidationcorpus.EncodeProof(proofOf(ec.t, ec.cert, ucb, block))
		if err != nil {
			return nil, err
		}
		reqs.Evidence = append(reqs.Evidence, consolidationcorpus.Evidence{Request: rh, Proof: ph})
		if ec.blk == 0 && shardIdx != spec.master {
			oldPeer = append(oldPeer, ph)
		}
	}
	if err := files.Encode(consolidationcorpus.FileRequests, reqs); err != nil {
		return nil, err
	}
	if err := files.Encode(consolidationcorpus.FileResponses, resps); err != nil {
		return nil, err
	}

	bc := &builtCase{spec: spec, files: files, lateEvidence: lateEv, oldPeerProofs: oldPeer, otherCert: otherCert}
	if spec.partition != 0 {
		// material of another partition is only a source of the wrong-partition negative: no positive expectation
		return bc, nil
	}
	if err := bc.writeExpected(); err != nil {
		return nil, fmt.Errorf("case %s: %w", spec.name, err)
	}
	// the independent producer UCs of every shard of block F, for the consumer to compare with
	_ = blkL
	return bc, nil
}

func indexOf(shards []ucfixture.Shard, id types.ShardID) int {
	for i, s := range shards {
		if s.ID.Equal(id) {
			return i
		}
	}
	return -1
}

func shardOfTx(spec caseSpec, first, second map[int][]*tx, t *tx) int {
	for _, i := range spec.participants() {
		for _, x := range append(append([]*tx{}, first[i]...), second[i]...) {
			if x == t {
				return i
			}
		}
	}
	return -1
}

// expected is expected.json of a positive case.
type expected struct {
	Description string                            `json:"description"`
	Stage       string                            `json:"stage"`
	Outcome     string                            `json:"outcome"`
	RMin        string                            `json:"rMin"`
	RootRound   string                            `json:"rootRound"`
	Master      string                            `json:"master"`
	Counters    consolidationcorpus.Counters      `json:"counters"`
	Shards      []expectedShard                   `json:"shards"`
	Bundle      []consolidationcorpus.ShardBundle `json:"bundle"`
}

type expectedShard struct {
	Shard string `json:"shard"`
	// StateRoot is the state tree root (IR hash) the consolidated proofs fold to.
	StateRoot string `json:"stateRoot"`
	// UCSha256 is the digest of the assembled UC, equal to ucs/<shard>.cbor of the producer.
	UCSha256     string   `json:"ucSha256"`
	ProofSha256s []string `json:"proofSha256"`
}

func (c *builtCase) writeExpected() error {
	res, err := consolidationcorpus.Evaluate(c.files)
	if err != nil {
		return err
	}
	if res.Stage != consolidationcorpus.StageOK {
		return fmt.Errorf("positive case refused at %s: %s", res.Stage, res.Outcome)
	}
	var reqs consolidationcorpus.Requests
	if err := c.files.Decode(consolidationcorpus.FileRequests, &reqs); err != nil {
		return err
	}
	total := len(reqs.Master.Requests)
	for _, p := range reqs.Peers {
		total += len(p.Requests)
	}
	want := consolidationcorpus.Counters{QuorumChecks: 1, BodyChecks: len(c.spec.participants()), LeafChecks: total, EvidenceQuorumChecks: 2}
	if *res.Counters != want {
		return fmt.Errorf("counters %+v, want %+v", *res.Counters, want)
	}
	exp := expected{Description: c.spec.desc, Stage: res.Stage, Outcome: res.Outcome, RMin: res.RMin, RootRound: res.RootRound, Master: res.Master, Counters: *res.Counters, Bundle: res.Bundle}
	for _, b := range res.Bundle {
		ucb, err := consolidationcorpus.UnHex(b.UC)
		if err != nil {
			return err
		}
		var uc types.UnicityCertificate
		if err := types.Cbor.Unmarshal(ucb, &uc); err != nil {
			return err
		}
		// the assembled UC is byte-identical to the one the producer derived
		file := consolidationcorpus.DirUCs + shardFile(b.Shard) + ".cbor"
		if !bytes.Equal(c.files[file], ucb) {
			return fmt.Errorf("assembled UC of shard %q differs from the producer UC", b.Shard)
		}
		es := expectedShard{Shard: b.Shard, StateRoot: consolidationcorpus.Hex(uc.InputRecord.Hash), UCSha256: consolidationcorpus.Digest(ucb)}
		for _, p := range b.Proofs {
			pb, _ := consolidationcorpus.UnHex(p)
			es.ProofSha256s = append(es.ProofSha256s, consolidationcorpus.Digest(pb))
		}
		exp.Shards = append(exp.Shards, es)
	}
	return c.files.Encode(consolidationcorpus.FileExpected, exp)
}

func u64(v uint64) string { return strconv.FormatUint(v, 10) }
