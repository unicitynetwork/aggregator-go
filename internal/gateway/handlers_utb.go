package gateway

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/trustbase/utb"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

// maxTrustBaseQueryBytes bounds the query string of GET /api/v1/trustbases (five short fields).
const maxTrustBaseQueryBytes = 1 << 10

// SetTrustBaseHistory serves the Unicity Trust Base history (signed record version 2) on GET /api/v1/trustbases instead of the version 1 trust bases.
// PUT and the aggregator's own version 1 trust base are untouched. With CORS enabled the route and its preflight carry the JSON-RPC route's headers
// (a browser fetch loop needs them), advertising GET and OPTIONS only. Call it before Start.
func (s *Server) SetTrustBaseHistory(g *utb.Guard) {
	s.utb = g
	if s.config.Server.EnableCORS {
		s.router.OPTIONS("/api/v1/trustbases", s.trustBaseCORS, func(c *gin.Context) { c.Status(http.StatusOK) })
	}
}

// trustBaseCORS is the CORS middleware of the history route; unlike the JSON-RPC route it answers the preflight itself.
func (s *Server) trustBaseCORS(c *gin.Context) {
	jsonrpc.SetCORSHeaders(c.Writer.Header(), "GET, OPTIONS")
	if c.Request.Method == http.MethodOptions {
		c.AbortWithStatus(http.StatusOK)
		return
	}
	c.Next()
}

// handleGetTrustBases serves the V2 history when configured and the version 1 trust bases otherwise.
func (s *Server) handleGetTrustBases(v1 gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		if s.utb == nil {
			v1(c)
			return
		}
		if s.config.Server.EnableCORS {
			s.trustBaseCORS(c)
		}
		s.serveTrustBaseHistory(c)
	}
}

// serveTrustBaseHistory answers GET /api/v1/trustbases?from=E[&to=E][&snapshotEnd=E&snapshotEntryId=0x..][&maxEntries=N] with the GetTrustBase response
// codec (tag 39015, application/cbor). A malformed query is 400; every well-formed outcome, protocol failures included, is 200 with the status inside
// the response. The response is encoded and size-checked before anything is written.
func (s *Server) serveTrustBaseHistory(c *gin.Context) {
	ctx := c.Request.Context()
	h := s.utb.History()
	q, err := parseTrustBaseQuery(c.Request.URL, h.GenesisEpoch())
	if err != nil {
		http.Error(c.Writer, err.Error(), http.StatusBadRequest)
		return
	}
	var resp *types.TrustBaseResponse
	// Behind (the active epoch is not in the held history yet) is not a fork: the verified prefix is served and NotYetAvailable says where it ends.
	// Only a conflict with the aggregator's own authority, or a failure to tell, stops the answer.
	if err := s.utb.Check(ctx); err != nil && !errors.Is(err, utb.ErrBehind) {
		s.logger.WithContext(ctx).Error("GET trustbases: the V2 history is not served", "error", err.Error())
		resp = h.Unavailable()
	} else {
		resp = h.Respond(q)
	}
	body, err := resp.Encode()
	if err == nil && len(body) > types.DefaultTrustBaseMaxResponse {
		err = fmt.Errorf("response of %d bytes exceeds %d", len(body), types.DefaultTrustBaseMaxResponse)
	}
	if err != nil {
		s.logger.WithContext(ctx).Error("GET trustbases: encoding the response", "error", err.Error())
		http.Error(c.Writer, "failed to encode the response", http.StatusInternalServerError)
		return
	}
	s.logger.WithContext(ctx).Debug("served trust-base history", "from", q.From, "status", resp.Status.String(), "entries", len(resp.Entries),
		"effectiveEnd", resp.EffectiveEnd)
	c.Header("Cache-Control", "no-store")
	c.Data(http.StatusOK, "application/cbor", body)
}

var errQuery = errors.New("invalid query")

// parseTrustBaseQuery is strict: a repeated field, a value that is not a plain decimal that fits, a half snapshot, maxEntries outside 1..64 and an
// oversized query string are all rejected; unknown fields are ignored.
func parseTrustBaseQuery(u *url.URL, genesis uint64) (utb.Query, error) {
	q := utb.Query{From: genesis, MaxEntries: types.DefaultTrustBaseMaxEntries}
	if len(u.RawQuery) > maxTrustBaseQueryBytes {
		return q, fmt.Errorf("%w: query longer than %d bytes", errQuery, maxTrustBaseQueryBytes)
	}
	vals, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return q, fmt.Errorf("%w: %v", errQuery, err)
	}
	one := func(key string) (string, bool, error) {
		v := vals[key]
		switch {
		case len(v) == 0:
			return "", false, nil
		case len(v) > 1:
			return "", false, fmt.Errorf("%w: %s is repeated", errQuery, key)
		}
		return v[0], true, nil
	}
	num := func(key string) (*uint64, error) {
		raw, ok, err := one(key)
		if err != nil || !ok {
			return nil, err
		}
		n, perr := strconv.ParseUint(raw, 10, 64)
		if perr != nil || (len(raw) > 1 && raw[0] == '0') {
			return nil, fmt.Errorf("%w: %s must be a decimal number below 2^64", errQuery, key)
		}
		return &n, nil
	}
	if v, err := num("from"); err != nil {
		return q, err
	} else if v != nil {
		q.From = *v
	}
	if q.To, err = num("to"); err != nil {
		return q, err
	}
	if q.SnapshotEnd, err = num("snapshotEnd"); err != nil {
		return q, err
	}
	if raw, ok, err := one("snapshotEntryId"); err != nil {
		return q, err
	} else if ok {
		id, derr := hex.DecodeString(strings.TrimPrefix(raw, "0x"))
		if derr != nil || len(id) != types.TrustBaseV2IDLen {
			return q, fmt.Errorf("%w: snapshotEntryId must be 32 bytes of hex", errQuery)
		}
		q.SnapshotEntryID = id
	}
	if (q.SnapshotEnd == nil) != (q.SnapshotEntryID == nil) {
		return q, fmt.Errorf("%w: snapshotEnd and snapshotEntryId go together", errQuery)
	}
	if v, err := num("maxEntries"); err != nil {
		return q, err
	} else if v != nil {
		if *v < 1 || *v > types.DefaultTrustBaseMaxEntries {
			return q, fmt.Errorf("%w: maxEntries must be 1..%d", errQuery, types.DefaultTrustBaseMaxEntries)
		}
		q.MaxEntries = int(*v)
	}
	return q, nil
}

// trustBaseV2Health adds the served tip to the health status and degrades it while the history is not consistent with the aggregator's authority.
func (s *Server) trustBaseV2Health(c *gin.Context, status *api.HealthStatus) {
	if s.utb == nil {
		return
	}
	degrade := func(key, msg string) {
		status.AddDetail(key, msg)
		if status.Status == api.HealthStatusOk {
			status.Status = api.HealthStatusDegraded
		}
	}
	epoch, id := s.utb.History().Tip()
	status.AddDetail("trustBaseV2Tip", fmt.Sprintf("epoch %d entry 0x%s", epoch, hex.EncodeToString(id[:])))
	switch err := s.utb.Check(c.Request.Context()); {
	case err == nil:
	case errors.Is(err, utb.ErrBehind):
		degrade("trustBaseV2", "behind, serving the verified prefix: "+err.Error())
	default:
		degrade("trustBaseV2", "not served: "+err.Error())
	}
	if err, at := s.utb.History().Status(); err != nil {
		degrade("trustBaseV2Refresh", fmt.Sprintf("failed at %s: %v", at.UTC().Format(time.RFC3339), err))
	}
}
