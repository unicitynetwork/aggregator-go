package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/gateway"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/proofview"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// No-wait golden responses of get_inclusion_proofs.v1 (long-poll T3). They pin the raw HTTP
// bodies of requests which do not carry waitMs, so that the long-poll handlers (A3) must keep
// them byte-identical. They were recorded from the handlers before any long-poll change.
// Regenerate deliberately with LONGPOLL_GOLDEN_UPDATE=1.

const longPollGoldenBatch = "../../testdata/longpoll/golden/batch-nowait.json"

type goldenCase struct {
	Name     string `json:"name"`
	Request  string `json:"request"`
	Response string `json:"response"`
}

func batchIDsJSON(ids ...api.StateID) string {
	q := make([]string, len(ids))
	for i, id := range ids {
		q[i] = `"` + id.String() + `"`
	}
	return strings.Join(q, ",")
}

// goldenGateway starts the production gateway server (gin router, the RequestID/Logging/
// Timeout(30s) middleware stack, the real method registration and limits) on a loopback port and
// returns its URL. svc serves get_inclusion_proof.v2 (the batch method gets batch, when set).
func goldenGateway(t *testing.T, svc gateway.Service, batch jsonrpc.HandlerFunc) string {
	t.Helper()
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	cfg := &config.Config{}
	cfg.Server = config.ServerConfig{Host: "127.0.0.1", Port: strconv.Itoa(port), ConcurrencyLimit: 100,
		ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute}
	cfg.Sharding.Mode = config.ShardingModeStandalone
	srv := gateway.NewServer(cfg, log, svc)
	if batch != nil {
		srv.SetBatchProofHandler(batch)
	}
	go func() { _ = srv.Start() }()
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Stop(c)
	})
	url := fmt.Sprintf("http://127.0.0.1:%d/", port)
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
		}
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	return url
}

func goldenPost(t *testing.T, url, body string) string {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	return string(b)
}

func TestLongPollGolden_BatchNoWait(t *testing.T) {
	batchReq := func(id int, params string) string {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"get_inclusion_proofs.v1","params":%s}`, id, params)
	}

	// An environment with 6 finalized records, then 1 record newer than the pinned view.
	e := newBatchEnv(t, proofview.DefaultLimits())
	ids := e.addRecords(6, 1000)
	oldRoot := e.finalize([]byte("pv1"))
	more := e.addRecords(2, 1000)
	e.finalize([]byte("pv2"))
	later := e.addRecordNoLeaf(1000)
	gone := unseen(e, 4)
	st := mapStatus{m: map[string]api.ProofStatus{
		gone[0].String(): api.ProofStatusPending,
		gone[1].String(): api.ProofStatusExpired,
		more[0].String(): api.ProofStatusPending,
	}}
	e.svc.status = st
	h := goldenGateway(t, &AggregatorService{}, e.svc.HandleRPC)

	// A deployment before its first block, and one whose newest block has no partition view.
	empty := newBatchEnv(t, proofview.DefaultLimits())
	emptyIDs := empty.addRecords(1, 1000)
	hEmpty := goldenGateway(t, &AggregatorService{}, empty.svc.HandleRPC)
	noView := newBatchEnv(t, proofview.DefaultLimits())
	noViewIDs := noView.addRecords(1, 1000)
	noView.finalize(nil)
	hNoView := goldenGateway(t, &AggregatorService{}, noView.svc.HandleRPC)

	type spec struct {
		name string
		h    string
		body string
	}
	rootHex := fmt.Sprintf("%x", oldRoot)
	over := make([]api.StateID, 0, 257)
	for i := 0; i < 257; i++ {
		over = append(over, api.StateID(append([]byte{0xdd, byte(i >> 8), byte(i)}, make([]byte, 29)...)))
	}
	specs := []spec{
		{"latest-success-2", h, batchReq(1, `{"stateIds":[`+batchIDsJSON(ids[1], ids[0])+`]}`)},
		{"latest-success-explicit-latest", h, batchReq(2, `{"stateIds":[`+batchIDsJSON(ids[2])+`],"stateRoot":"latest"}`)},
		{"exact-root-success", h, batchReq(3, `{"stateIds":[`+batchIDsJSON(ids[0], ids[3])+`],"stateRoot":"`+rootHex+`"}`)},
		{"exact-root-not-in-root", h, batchReq(4, `{"stateIds":[`+batchIDsJSON(ids[0], more[0], gone[3])+`],"stateRoot":"`+rootHex+`"}`)},
		{"pending", h, batchReq(5, `{"stateIds":[`+batchIDsJSON(ids[0], gone[0], more[0])+`]}`)},
		{"pending-record-newer-than-view", h, batchReq(6, `{"stateIds":[`+batchIDsJSON(ids[0], later)+`]}`)},
		{"expired-over-pending", h, batchReq(7, `{"stateIds":[`+batchIDsJSON(gone[0], gone[1])+`]}`)},
		{"unknown-over-expired-over-pending", h, batchReq(8, `{"stateIds":[`+batchIDsJSON(gone[0], gone[1], gone[2])+`]}`)},
		{"no-finalized-view", hEmpty, batchReq(9, `{"stateIds":[`+batchIDsJSON(emptyIDs[0])+`]}`)},
		{"partition-view-unavailable", hNoView, batchReq(10, `{"stateIds":[`+batchIDsJSON(noViewIDs[0])+`]}`)},
		{"malformed-not-an-object", h, batchReq(11, `[1]`)},
		{"malformed-unknown-field", h, batchReq(12, `{"stateIds":[`+batchIDsJSON(ids[0])+`],"foo":1}`)},
		{"malformed-duplicate-state-ids-field", h, batchReq(13, `{"stateIds":[`+batchIDsJSON(ids[0])+`],"stateIds":[`+batchIDsJSON(ids[1])+`]}`)},
		{"malformed-empty-ids", h, batchReq(14, `{"stateIds":[]}`)},
		{"malformed-missing-ids", h, batchReq(15, `{}`)},
		{"malformed-null-root", h, batchReq(16, `{"stateIds":[`+batchIDsJSON(ids[0])+`],"stateRoot":null}`)},
		{"malformed-trailing-data", h, batchReq(17, `{"stateIds":[`+batchIDsJSON(ids[0])+`]} 1`)},
		{"invalid-state-id", h, batchReq(18, `{"stateIds":["abcd"]}`)},
		{"invalid-state-root", h, batchReq(19, `{"stateIds":[`+batchIDsJSON(ids[0])+`],"stateRoot":"LATEST"}`)},
		{"duplicate-state-id", h, batchReq(20, `{"stateIds":[`+batchIDsJSON(ids[0], ids[0])+`]}`)},
		{"state-count-257", h, batchReq(21, `{"stateIds":[`+batchIDsJSON(over...)+`]}`)},
	}

	var got []goldenCase
	for _, s := range specs {
		got = append(got, goldenCase{Name: s.name, Request: s.body, Response: goldenPost(t, s.h, s.body)})
	}
	checkLongPollGolden(t, longPollGoldenBatch, got)
}

// checkLongPollGolden compares the recorded cases with the file (or rewrites it on request).
func checkLongPollGolden(t *testing.T, path string, got []goldenCase) {
	t.Helper()
	enc, err := json.MarshalIndent(got, "", "  ")
	require.NoError(t, err)
	enc = append(enc, '\n')
	if os.Getenv("LONGPOLL_GOLDEN_UPDATE") != "" {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, enc, 0o644))
		return
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err)
	var wantCases []goldenCase
	require.NoError(t, json.Unmarshal(want, &wantCases))
	require.Len(t, got, len(wantCases))
	for i := range got {
		require.Equal(t, wantCases[i].Name, got[i].Name)
		require.Equal(t, wantCases[i].Request, got[i].Request, got[i].Name)
		require.Equal(t, wantCases[i].Response, got[i].Response, got[i].Name)
	}
}
