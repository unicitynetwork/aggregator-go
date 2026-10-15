// Package client is the HTTP orchestration of master-first proof consolidation.
// It performs no verification of its own: it fetches batches through
// get_inclusion_proofs.v1 and hands them to the pure pkg/consolidation library.
//
// One attempt = one master "latest" request, then one exact-root request per
// other participating shard (bounded parallel), all under one total deadline.
// There is no retry, no chunking and no chasing of changing latest seals, and
// the client only ever reads: it never submits a certification request.
package client

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/consolidation"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// Method is the only JSON-RPC method this client calls.
const Method = "get_inclusion_proofs.v1"

// requestID is the JSON-RPC id of every request (one request per connection use).
const requestID = 1

const (
	// DefaultTimeout is the default total deadline of one attempt.
	DefaultTimeout = 10 * time.Second
	// DefaultMaxParallel bounds the concurrent exact-root requests.
	DefaultMaxParallel = 4
	// maxBody bounds a response body (8 MiB response limit plus slack).
	maxBody = api.InclusionProofsMaxResponseBytes + 4096
)

// Endpoint is the configured native shard aggregator of one shard.
type Endpoint struct {
	ShardID types.ShardID
	URL     string
}

// Config configures a Client.
type Config struct {
	// Endpoints has one entry per shard that may participate. The shard IDs
	// must be shards of Policy.Topology.
	Endpoints []Endpoint
	// Policy and Resolver are the trusted verification context.
	Policy   consolidation.Policy
	Resolver consolidation.TrustBaseResolver
	// HTTPClient defaults to a client without its own timeout (the attempt deadline governs).
	HTTPClient *http.Client
	// Timeout is the total deadline of one attempt (default DefaultTimeout).
	Timeout time.Duration
	// MaxParallel bounds concurrent second-wave requests (default DefaultMaxParallel).
	MaxParallel int
	// Master optionally selects the master shard; it must participate. When
	// nil the shard of the newest authenticated evidence is used.
	Master *types.ShardID
}

// Item is one original certification request and the proof it was certified by.
type Item struct {
	Request *api.CertificationRequest
	Proof   *api.InclusionProofV2
}

// Stats describes the work of one attempt.
type Stats struct {
	// Requests is the number of HTTP requests made.
	Requests int
	// Waves is the number of sequential request waves (1 for one shard, else 2).
	Waves int
	// PartitionViewBytes is the size of the downloaded partition view.
	PartitionViewBytes int
	// ResponseBytes is the total size of the response bodies.
	ResponseBytes int
}

// Result is a consolidated bundle with provenance and instrumentation.
type Result struct {
	*consolidation.Output
	// Order maps every input item index to (shard index in Shards, index in that shard's Proofs).
	Order    []Position
	Counters consolidation.Counters
	Stats    Stats
}

// Position locates a consolidated proof of an input item.
type Position struct{ Shard, Proof int }

// Client runs consolidation attempts. It is safe for concurrent use.
type Client struct {
	cfg  Config
	http *http.Client
}

// New validates the configuration and creates a client.
func New(cfg Config) (*Client, error) {
	if cfg.Resolver == nil {
		return nil, &Error{Kind: KindInput, Err: errors.New("trust base resolver is required")}
	}
	order, err := types.CanonicalShardOrder(cfg.Policy.Topology.Scheme)
	if err != nil {
		return nil, &Error{Kind: KindInput, Err: err}
	}
	seen := map[string]bool{}
	for _, ep := range cfg.Endpoints {
		if !slices.ContainsFunc(order, ep.ShardID.Equal) {
			return nil, &Error{Kind: KindInput, Err: fmt.Errorf("endpoint shard %q is not in the policy topology", ep.ShardID)}
		}
		if seen[ep.ShardID.Key()] {
			return nil, &Error{Kind: KindInput, Err: fmt.Errorf("duplicate endpoint for shard %q", ep.ShardID)}
		}
		if ep.URL == "" {
			return nil, &Error{Kind: KindInput, Err: fmt.Errorf("endpoint of shard %q has no URL", ep.ShardID)}
		}
		seen[ep.ShardID.Key()] = true
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.MaxParallel <= 0 {
		cfg.MaxParallel = DefaultMaxParallel
	}
	h := cfg.HTTPClient
	if h == nil {
		h = &http.Client{}
	}
	return &Client{cfg: cfg, http: h}, nil
}

type shardGroup struct {
	ep    Endpoint
	items []int // indexes into the input
}

// Consolidate runs one master-first attempt. The inputs are never modified;
// on any failure nothing is returned but the error.
func (c *Client) Consolidate(ctx context.Context, items []Item) (*Result, error) {
	if len(items) == 0 {
		return nil, &Error{Kind: KindInput, Err: errors.New("no items")}
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	// trust resolution cooperates with the attempt context
	resolver := func(seal *types.UnicitySeal) (types.RootTrustBase, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return c.cfg.Resolver(seal)
	}
	session, err := consolidation.NewSession(c.cfg.Policy, resolver)
	if err != nil {
		return nil, libError(err)
	}
	// ctxFail reports a done attempt context as the typed unavailable outcome, also
	// when a local verification error was merely a consequence of it
	ctxFail := func() error {
		if ctx.Err() == nil {
			return nil
		}
		return transportError(ctx, types.ShardID{}, ctx.Err())
	}
	libFail := func(err error) error {
		if cerr := ctxFail(); cerr != nil {
			return cerr
		}
		return libError(err)
	}
	groups, err := c.group(items)
	if err != nil {
		return nil, err
	}

	// R_min from the authenticated original evidence, never from a guess
	ev := make([]consolidation.Evidence, len(items))
	for i, it := range items {
		ev[i] = consolidation.Evidence{Request: it.Request, Proof: it.Proof}
	}
	bound, err := session.AuthenticateEvidence(ev)
	if err != nil {
		return nil, libFail(err)
	}
	if err := ctxFail(); err != nil {
		return nil, err
	}

	master, err := c.pickMaster(groups, bound)
	if err != nil {
		return nil, err
	}
	var stats Stats

	// wave 1: the master's latest batch and the partition view
	mg := groups[master]
	mres, err := c.fetch(ctx, mg, items, nil, &stats)
	if err != nil {
		return nil, err
	}
	if len(mres.view) == 0 {
		return nil, &Error{Kind: KindUnavailable, Shard: mg.ep.ShardID, Detail: "partition_view_unavailable", Err: errors.New("master response carries no partition view")}
	}
	stats.PartitionViewBytes = len(mres.view)
	stats.Waves = 1
	if err := ctxFail(); err != nil {
		return nil, err
	}

	// authenticate the master (view/UC binding, trusted seal, R >= R_min) before any peer is contacted
	am, err := session.AuthenticateMaster(bound, shardResult(mg, items, mres), mres.view)
	if err != nil {
		return nil, libFail(err)
	}
	if err := ctxFail(); err != nil {
		return nil, err
	}

	// wave 2: exact roots of the other participating shards, bounded parallel
	peers := make([]*shardGroup, 0, len(groups)-1)
	roots := make([][]byte, 0, len(groups)-1)
	for gi, g := range groups {
		if gi == master {
			continue
		}
		root, err := am.PeerRoot(g.ep.ShardID)
		if err != nil {
			return nil, libFail(err)
		}
		peers = append(peers, g)
		roots = append(roots, root)
	}
	pres, err := c.fetchAll(ctx, peers, roots, items, &stats)
	if err != nil {
		return nil, err
	}
	if len(peers) > 0 {
		stats.Waves = 2
	}
	if err := ctxFail(); err != nil {
		return nil, err
	}

	var peerResults []consolidation.ShardResult
	for i, g := range peers {
		peerResults = append(peerResults, shardResult(g, items, pres[i]))
	}
	out, err := session.Complete(am, peerResults)
	if err != nil {
		return nil, libFail(err)
	}
	// never publish success for a done attempt
	if err := ctxFail(); err != nil {
		return nil, err
	}

	res := &Result{Output: out, Order: make([]Position, len(items)), Counters: session.Counters(), Stats: stats}
	all := append([]*shardGroup{mg}, peers...)
	for si, g := range all {
		for pi, idx := range g.items {
			res.Order[idx] = Position{Shard: si, Proof: pi}
		}
	}
	return res, nil
}

// group routes every item to its shard endpoint, in canonical shard order.
func (c *Client) group(items []Item) ([]*shardGroup, error) {
	byShard := map[string]*shardGroup{}
	for i, it := range items {
		if it.Request == nil || it.Proof == nil {
			return nil, &Error{Kind: KindInput, Err: fmt.Errorf("item %d is incomplete", i)}
		}
		key, err := it.Request.StateID.GetTreeKey()
		if err != nil {
			return nil, &Error{Kind: KindInput, Err: fmt.Errorf("item %d: %w", i, err)}
		}
		var g *shardGroup
		for _, ep := range c.cfg.Endpoints {
			if ep.ShardID.Length() <= uint(len(key)*8) && ep.ShardID.Comparator()(key) {
				if byShard[ep.ShardID.Key()] == nil {
					byShard[ep.ShardID.Key()] = &shardGroup{ep: ep}
				}
				g = byShard[ep.ShardID.Key()]
				break
			}
		}
		if g == nil {
			return nil, &Error{Kind: KindInput, Err: fmt.Errorf("item %d: no configured endpoint serves state %s", i, it.Request.StateID)}
		}
		g.items = append(g.items, i)
	}
	order, _ := types.CanonicalShardOrder(c.cfg.Policy.Topology.Scheme)
	var groups []*shardGroup
	for _, id := range order {
		if g := byShard[id.Key()]; g != nil {
			groups = append(groups, g)
		}
	}
	return groups, nil
}

func (c *Client) pickMaster(groups []*shardGroup, bound *consolidation.RootRoundBound) (int, error) {
	want := bound.Shard()
	if c.cfg.Master != nil {
		want = *c.cfg.Master
	}
	for i, g := range groups {
		if g.ep.ShardID.Equal(want) {
			return i, nil
		}
	}
	return 0, &Error{Kind: KindInput, Err: fmt.Errorf("master shard %q does not participate", want)}
}

func shardResult(g *shardGroup, items []Item, r *batchResponse) consolidation.ShardResult {
	sr := consolidation.ShardResult{ShardID: g.ep.ShardID, Proofs: r.proofs}
	for _, idx := range g.items {
		sr.Requests = append(sr.Requests, items[idx].Request)
	}
	return sr
}

type batchResponse struct {
	proofs []*api.GetInclusionProofResponseV2
	view   []byte
}

// fetchAll runs the exact-root requests with bounded parallelism. The first
// failure cancels the rest; the reported error is deterministic (shard order).
func (c *Client) fetchAll(ctx context.Context, groups []*shardGroup, roots [][]byte, items []Item, stats *Stats) ([]*batchResponse, error) {
	res := make([]*batchResponse, len(groups))
	errs := make([]error, len(groups))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, c.cfg.MaxParallel)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := range groups {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				errs[i] = transportError(ctx, groups[i].ep.ShardID, ctx.Err())
				return
			}
			var local Stats
			r, err := c.fetch(ctx, groups[i], items, roots[i], &local)
			mu.Lock()
			stats.Requests += local.Requests
			stats.ResponseBytes += local.ResponseBytes
			mu.Unlock()
			if err != nil {
				errs[i] = err
				cancel()
				return
			}
			res[i] = r
		}()
	}
	wg.Wait()
	// prefer the originating failure over the cancellations it caused
	var first error
	for _, err := range errs {
		if err == nil {
			continue
		}
		if !isContextError(err) {
			return nil, err
		}
		if first == nil {
			first = err
		}
	}
	if first != nil {
		return nil, first
	}
	return res, nil
}

// isContextError reports a typed error that only reflects cancellation or an expired deadline.
func isContextError(err error) bool {
	var ce *Error
	return errors.As(err, &ce) && ce.Kind == KindUnavailable && (ce.Detail == "cancelled" || ce.Detail == "deadline")
}

type rpcRequest struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      int       `json:"id"`
	Method  string    `json:"method"`
	Params  rpcParams `json:"params"`
}

type rpcParams struct {
	StateIDs  []string `json:"stateIds"`
	StateRoot string   `json:"stateRoot"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   json.RawMessage `json:"error"`
	ID      json.RawMessage `json:"id"`
}

type rpcResult struct {
	Proofs        []*api.GetInclusionProofResponseV2 `json:"proofs"`
	PartitionView api.HexBytes                       `json:"partitionView"`
}

type rpcErrorBody struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func present(raw json.RawMessage) bool {
	return len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// fetch makes exactly one request for the group's items; root nil means latest.
func (c *Client) fetch(ctx context.Context, g *shardGroup, items []Item, root []byte, stats *Stats) (*batchResponse, error) {
	ids := make([]string, len(g.items))
	for i, idx := range g.items {
		ids[i] = hex.EncodeToString(items[idx].Request.StateID)
	}
	stateRoot := api.InclusionProofsStateRootLatest
	if root != nil {
		stateRoot = hex.EncodeToString(root)
	}
	body, err := json.Marshal(rpcRequest{JSONRPC: jsonrpc.Version, ID: requestID, Method: Method, Params: rpcParams{StateIDs: ids, StateRoot: stateRoot}})
	if err != nil {
		return nil, &Error{Kind: KindInput, Shard: g.ep.ShardID, Err: err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.ep.URL, bytes.NewReader(body))
	if err != nil {
		return nil, &Error{Kind: KindInput, Shard: g.ep.ShardID, Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	stats.Requests++
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, transportError(ctx, g.ep.ShardID, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, transportError(ctx, g.ep.ShardID, err)
	}
	stats.ResponseBytes += len(raw)
	if len(raw) > maxBody {
		return nil, &Error{Kind: KindInvalidResponse, Shard: g.ep.ShardID, Err: errors.New("response exceeds the 8 MiB limit")}
	}
	var rr rpcResponse
	if err := json.Unmarshal(raw, &rr); err != nil {
		if resp.StatusCode != http.StatusOK {
			return nil, &Error{Kind: KindUnavailable, Shard: g.ep.ShardID, Detail: fmt.Sprintf("http_%d", resp.StatusCode), Err: errors.New(resp.Status)}
		}
		return nil, &Error{Kind: KindInvalidResponse, Shard: g.ep.ShardID, Err: err}
	}
	invalid := func(format string, a ...any) error {
		return &Error{Kind: KindInvalidResponse, Shard: g.ep.ShardID, Err: fmt.Errorf(format, a...)}
	}
	if rr.JSONRPC != jsonrpc.Version {
		return nil, invalid("response jsonrpc version %q, want %q", rr.JSONRPC, jsonrpc.Version)
	}
	if string(bytes.TrimSpace(rr.ID)) != strconv.Itoa(requestID) {
		return nil, invalid("response id %s does not match request id %d", rr.ID, requestID)
	}
	hasResult, hasError := present(rr.Result), present(rr.Error)
	if hasResult == hasError {
		return nil, invalid("response must carry exactly one of result and error")
	}
	if hasError {
		var eb rpcErrorBody
		if err := json.Unmarshal(rr.Error, &eb); err != nil {
			return nil, invalid("malformed error object: %v", err)
		}
		return nil, rpcError(g.ep.ShardID, eb.Code, eb.Message, eb.Data)
	}
	var res rpcResult
	if err := json.Unmarshal(rr.Result, &res); err != nil {
		return nil, &Error{Kind: KindInvalidResponse, Shard: g.ep.ShardID, Err: err}
	}
	if len(res.Proofs) != len(g.items) {
		return nil, &Error{Kind: KindInvalidResponse, Shard: g.ep.ShardID, Err: fmt.Errorf("got %d proofs for %d states", len(res.Proofs), len(g.items))}
	}
	for i, p := range res.Proofs {
		if p == nil || p.InclusionProof == nil {
			return nil, &Error{Kind: KindInvalidResponse, Shard: g.ep.ShardID, Err: fmt.Errorf("proof %d is empty", i)}
		}
	}
	return &batchResponse{proofs: res.Proofs, view: res.PartitionView}, nil
}
