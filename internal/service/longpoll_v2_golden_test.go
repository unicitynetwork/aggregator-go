package service

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/longpoll"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/proofstatus"
	"github.com/unicitynetwork/aggregator-go/internal/signing"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	bfttypes "github.com/unicitynetwork/bft-go-base/types"
)

// No-wait golden responses of get_inclusion_proof.v2 (long-poll T3), recorded through the real
// AggregatorService read path (cached metadata, published-root/empty-proof shapes, the #220
// known-not-ready shortcut, the pre-first-block error) behind the production gateway: gin, the
// RequestID/Logging/Timeout middleware and the real handler. A1 (permit, middleware) and A3
// (re-runnable evaluator) change that machinery, so these were recorded before either.

const longPollGoldenV2 = "../../testdata/longpoll/golden/v2-nowait.json"

// goldenRequest is a deterministic signed request (fixed key and expiry).
func goldenRequest(t *testing.T, n int) *api.CertificationRequest {
	t.Helper()
	priv, _ := btcec.PrivKeyFromBytes(bytes.Repeat([]byte{byte(n + 1)}, 32))
	owner := api.NewPayToPublicKeyPredicate(priv.PubKey().SerializeCompressed())
	src := signing.CreateDataHash(bytes.Repeat([]byte{byte(0x10 + n)}, 32))
	id, err := api.CreateStateID(owner, src)
	require.NoError(t, err)
	exp := uint64(4102444800) // 2100-01-01
	cd := &api.CertificationData{OwnerPredicate: owner, SourceStateHash: src,
		TransactionHash: signing.CreateDataHash(bytes.Repeat([]byte{byte(0x20 + n)}, 32)), ExpiresAt: &exp}
	require.NoError(t, signing.NewSigningService().SignCertData(cd, priv.Serialize()))
	return &api.CertificationRequest{StateID: id, CertificationData: *cd}
}

// notReadyRoundManager adds the known-not-ready shortcut to the stub.
type notReadyRoundManager struct {
	*stubRoundManager
	block *models.Block
}

func (m notReadyRoundManager) GetKnownNotReadyBlock(id api.StateID) (*models.Block, bool) {
	if m.stubRoundManager.ProofStatus(id) == api.ProofStatusPending {
		return m.block, true
	}
	return nil, false
}

func TestLongPollGolden_V2NoWaitThroughRealService(t *testing.T) {
	t.Run("legacy", func(t *testing.T) { v2NoWaitGolden(t, false) })
	t.Run("long-poll wired", func(t *testing.T) { v2NoWaitGolden(t, true) })
}

func v2NoWaitGolden(t *testing.T, wired bool) {
	var lp *longpoll.Coordinator
	if wired {
		lp = goldenCoordinator(t)
	}
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	shardingCfg := config.ShardingConfig{Mode: config.ShardingModeBFTShard}

	incl, other, absent, absent2 := goldenRequest(t, 0), goldenRequest(t, 1), goldenRequest(t, 2), goldenRequest(t, 3)
	tree := smt.NewSparseMerkleTree(api.SHA256, api.StateTreeKeyLengthBits)
	const refTime uint64 = 1755000000
	for _, r := range []*api.CertificationRequest{incl, other} {
		path, err := r.StateID.GetPath()
		require.NoError(t, err)
		require.NoError(t, tree.AddLeaf(path, api.LeafValue(r.CertificationData.TransactionHash.DataBytes(), refTime)))
	}
	root := api.HexBytes(tree.GetRootHashRaw())
	block := models.NewBlock(api.NewBigIntFromUint64(9), "test-chain", 0, "v", "f", root, nil, testChildProofUC(t, 9, root), 1755000000)
	block.Finalized = true
	record := func(r *api.CertificationRequest) *models.AggregatorRecord {
		return &models.AggregatorRecord{Version: 2, StateID: r.StateID,
			CertificationData: models.CertificationData{OwnerPredicate: r.CertificationData.OwnerPredicate,
				SourceStateHash: r.CertificationData.SourceStateHash, TransactionHash: r.CertificationData.TransactionHash,
				ExpiresAt: r.CertificationData.ExpiresAt, Witness: r.CertificationData.Witness},
			ReferenceTime: refTime, BlockNumber: api.NewBigIntFromUint64(9), LeafIndex: api.NewBigIntFromUint64(0)}
	}
	build := func(withBlock bool, mutate func(*stubRoundManager)) (*stubRoundManager, *AggregatorService) {
		blocks := map[string]*models.Block{}
		if withBlock {
			blocks[root.String()] = block
		}
		recs := &testAggregatorRecordStorage{byStateID: map[string]*models.AggregatorRecord{incl.StateID.String(): record(incl)}}
		rm := &stubRoundManager{smt: smt.NewThreadSafeSMT(tree), status: proofstatus.New(proofstatus.Config{})}
		if withBlock {
			rm.cachedRoot, rm.cachedBlock, rm.cachedRecord = root, block, record(incl)
		}
		if mutate != nil {
			mutate(rm)
		}
		return rm, &AggregatorService{
			config:                        &config.Config{Sharding: shardingCfg},
			logger:                        log,
			storage:                       &testStorage{blockStorage: &testBlockStorage{latestByRoot: blocks}, recordStorage: recs},
			roundManager:                  rm,
			certificationRequestValidator: signing.NewCertificationRequestValidator(shardingCfg, bfttypes.ShardID{}),
		}
	}

	type spec struct {
		name string
		url  string
		body string
	}
	var specs []spec
	req := func(id int, params string) string {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"get_inclusion_proof.v2","params":%s}`, id, params)
	}
	sid := func(r *api.CertificationRequest) string { return `"stateId":"` + r.StateID.String() + `"` }

	// 1. a block exists and the state is included
	_, svcIncl := build(true, nil)
	u := goldenGateway(t, svcIncl, nil, lp)
	specs = append(specs,
		spec{"included-bare", u, req(1, `{`+sid(incl)+`}`)},
		spec{"included-status-false", u, req(2, `{`+sid(incl)+`,"includeStatus":false}`)},
		spec{"included-status", u, req(3, `{`+sid(incl)+`,"includeStatus":true}`)},
		spec{"included-unknown-extra-field-ignored", u, req(4, `{`+sid(incl)+`,"foo":1}`)},
		// not included (no record): the empty proof of the latest certified block, with each hint
		spec{"not-included-bare", u, req(5, `{`+sid(absent)+`}`)},
		spec{"not-included-status-unknown", u, req(6, `{`+sid(absent)+`,"includeStatus":true}`)},
	)

	// 2. pending (admitted) and expired hints, without and with the known-not-ready shortcut
	rmPend, svcPend := build(true, nil)
	rmPend.status.MarkPending(absent.StateID, 1<<62)
	rmPend.status.MarkExpired(absent2.StateID)
	uPend := goldenGateway(t, svcPend, nil, lp)
	specs = append(specs,
		spec{"pending-hint-without-shortcut-status", uPend, req(7, `{`+sid(absent)+`,"includeStatus":true}`)},
		spec{"expired-hint-status", uPend, req(8, `{`+sid(absent2)+`,"includeStatus":true}`)},
		spec{"expired-hint-bare", uPend, req(9, `{`+sid(absent2)+`}`)},
	)
	rmShort, svcShort := build(true, nil)
	rmShort.status.MarkPending(absent.StateID, 1<<62)
	svcShort.roundManager = notReadyRoundManager{stubRoundManager: rmShort, block: block}
	uShort := goldenGateway(t, svcShort, nil, lp)
	specs = append(specs,
		spec{"known-not-ready-bare", uShort, req(10, `{`+sid(absent)+`}`)},
		spec{"known-not-ready-status", uShort, req(11, `{`+sid(absent)+`,"includeStatus":true}`)},
		// a stale pending mark loses against an actual proof
		spec{"included-wins-over-stale-status", uPend, req(12, `{`+sid(incl)+`,"includeStatus":true}`)},
	)

	// 3. before the first block: the real service's typed error
	_, svcPre := build(false, nil)
	uPre := goldenGateway(t, svcPre, nil, lp)
	specs = append(specs,
		spec{"no-finalized-view", uPre, req(13, `{`+sid(incl)+`}`)},
		spec{"no-finalized-view-status", uPre, req(14, `{`+sid(incl)+`,"includeStatus":true}`)},
	)

	// 4. infrastructure and validation errors
	_, svcNoTree := build(true, func(rm *stubRoundManager) { rm.smt = nil })
	uNoTree := goldenGateway(t, svcNoTree, nil, lp)
	specs = append(specs,
		spec{"internal-error-no-tree", uNoTree, req(15, `{`+sid(incl)+`}`)},
		spec{"state-id-too-short", u, req(16, `{"stateId":"abcd"}`)},
		spec{"state-id-missing", u, req(17, `{}`)},
		spec{"params-not-an-object", u, req(18, `"x"`)},
		spec{"params-array", u, req(19, `[1]`)},
		spec{"include-status-wrong-type", u, req(20, `{`+sid(incl)+`,"includeStatus":"yes"}`)},
	)

	var got []goldenCase
	for _, s := range specs {
		got = append(got, goldenCase{Name: s.name, Request: s.body, Response: goldenPost(t, s.url, s.body)})
	}
	checkLongPollGolden(t, longPollGoldenV2, got)

	// sanity of the recorded shapes, so a wrong fixture is caught when it is recorded
	byName := map[string]string{}
	for _, c := range got {
		byName[c.Name] = c.Response
	}
	require.Contains(t, byName["pending-hint-without-shortcut-status"], `"status":"pending"`)
	require.Contains(t, byName["expired-hint-status"], `"status":"expired"`)
	require.Contains(t, byName["known-not-ready-status"], `"status":"pending"`)
	require.Contains(t, byName["no-finalized-view"], `"detail":"no_finalized_view"`)
	require.Contains(t, byName["included-status"], `"status":"included"`)
}
