package api

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// LongPollParamCase is one row of testdata/longpoll/params.json.
type LongPollParamCase struct {
	Name   string  `json:"name"`
	Method string  `json:"method"`
	Params string  `json:"params"`
	Valid  bool    `json:"valid"`
	WaitMs *uint64 `json:"waitMs"`
	Error  string  `json:"error"`
	Note   string  `json:"note"`
}

type longPollParamsFile struct {
	Cases []LongPollParamCase `json:"cases"`
}

func loadLongPollParams(t *testing.T) []LongPollParamCase {
	t.Helper()
	raw, err := os.ReadFile("../../testdata/longpoll/params.json")
	require.NoError(t, err)
	var f longPollParamsFile
	require.NoError(t, json.Unmarshal(raw, &f))
	require.NotEmpty(t, f.Cases)
	return f.Cases
}

// The shared fixtures decide the batch parser's behavior; rugregator's reservation parser and the
// SDKs consume the same file.
func TestLongPollParams_BatchParser(t *testing.T) {
	n := 0
	for _, c := range loadLongPollParams(t) {
		if c.Method != "get_inclusion_proofs.v1" {
			continue
		}
		n++
		t.Run(c.Name, func(t *testing.T) {
			req, err := ParseGetInclusionProofsRequestV1([]byte(c.Params))
			if !c.Valid {
				require.Equal(t, "batch_malformed_params", c.Error)
				var pe *InclusionProofsParamsError
				require.True(t, errors.As(err, &pe), "got %v", err)
				require.Equal(t, DetailMalformedParams, pe.Detail)
				require.False(t, pe.IsLimit())
				return
			}
			require.NoError(t, err)
			if c.WaitMs == nil {
				require.Equal(t, WaitMs{}, req.WaitMs)
			} else {
				require.Equal(t, WaitMs{Present: true, Value: *c.WaitMs}, req.WaitMs)
			}
		})
	}
	require.NotZero(t, n)
}

// The v2 handler decodes permissively with encoding/json, then validates waitMs with ScanWaitMs.
func TestLongPollParams_V2Scan(t *testing.T) {
	n := 0
	for _, c := range loadLongPollParams(t) {
		if c.Method != "get_inclusion_proof.v2" {
			continue
		}
		n++
		t.Run(c.Name, func(t *testing.T) {
			var legacy GetInclusionProofRequestV2
			if err := json.Unmarshal([]byte(c.Params), &legacy); err != nil {
				require.False(t, c.Valid, "legacy decode failed for a valid case: %v", err)
				return // rejected before waitMs is looked at, as for any malformed params
			}
			w, err := ScanWaitMs([]byte(c.Params))
			if !c.Valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if c.WaitMs == nil {
				require.Equal(t, WaitMs{}, w)
			} else {
				require.Equal(t, WaitMs{Present: true, Value: *c.WaitMs}, w)
			}
		})
	}
	require.NotZero(t, n)
}

func TestScanWaitMs_NeverFailsWithoutWaitMs(t *testing.T) {
	for _, p := range []string{``, `null`, `[]`, `"x"`, `1`, `{`, `{"a":`, `{"stateId":"ab","x":[1,{"waitMs":-1}]}`, `{"nested":{"waitMs":"bad"}}`} {
		w, err := ScanWaitMs([]byte(p))
		require.NoError(t, err, p)
		require.Equal(t, WaitMs{}, w, p)
	}
}

func TestParseWaitMs(t *testing.T) {
	for in, want := range map[string]uint64{"0": 0, "7": 7, "9007199254740991": WaitMsMax} {
		got, err := ParseWaitMs([]byte(in))
		require.NoError(t, err, in)
		require.Equal(t, want, got)
	}
	for _, in := range []string{"", "-0", "-1", "00", "01", "1.0", "1e3", " 1", "1 ", "9007199254740992", "10000000000000000", "٣", "null", `"1"`} {
		_, err := ParseWaitMs([]byte(in))
		require.ErrorIs(t, err, ErrInvalidWaitMs, in)
	}
}

func TestLongPollWindow(t *testing.T) {
	const s = time.Second
	for name, tc := range map[string]struct {
		p, r, tr, m, want time.Duration
	}{
		"preferred fits":        {20 * s, 40 * s, 30 * s, 2 * s, 20 * s},
		"remaining budget":      {20 * s, 10 * s, 30 * s, 2 * s, 8 * s},
		"transport maximum":     {20 * s, 40 * s, 15 * s, 2 * s, 13 * s}, // cmd/commitment's 15s HTTP timeout
		"both bind":             {20 * s, 12 * s, 15 * s, 2 * s, 10 * s},
		"unbounded remaining":   {20 * s, NoLimit, 30 * s, 2 * s, 20 * s},
		"unbounded transport":   {20 * s, 40 * s, NoLimit, 2 * s, 20 * s},
		"both unbounded":        {20 * s, NoLimit, NoLimit, 2 * s, 20 * s},
		"nothing fits":          {20 * s, 2 * s, 30 * s, 2 * s, 0},
		"negative after margin": {20 * s, s, 30 * s, 2 * s, 0},
		"expired":               {20 * s, 0, 30 * s, 2 * s, 0},
		"sub-millisecond floor": {20 * s, 2*s + 1500*time.Microsecond, 30 * s, 2 * s, time.Millisecond},
		"configured margin":     {20 * s, 10 * s, 30 * s, 5 * s, 5 * s},
	} {
		require.Equal(t, tc.want, LongPollWindow(tc.p, tc.r, tc.tr, tc.m), name)
	}
}

func TestParseEchoAndFindEcho(t *testing.T) {
	ms, ok, err := FindEchoMs([]byte(`{"proofs":[],"waitedMs":1200}`))
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 1200, ms)
	_, ok, err = FindEchoMs([]byte(`{"proofs":[]}`))
	require.NoError(t, err)
	require.False(t, ok)
	_, ok, err = FindEchoMs([]byte(`"a string"`))
	require.NoError(t, err)
	require.False(t, ok)
	for _, bad := range []string{`-1`, `1.5`, `"7"`, `null`, `9007199254740992`, `1e3`, `true`} {
		_, ok, err = FindEchoMs([]byte(`{"waitedMs":` + bad + `}`))
		require.True(t, ok, bad)
		require.Error(t, err, bad)
	}
}

// The shared capability table decides which rejections trigger the one parser-negotiation retry.
func TestIsParserNegotiation_FollowsTheCapabilityFixture(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/longpoll/capability.json")
	require.NoError(t, err)
	var f struct {
		Cases []struct {
			Name     string          `json:"name"`
			Response json.RawMessage `json:"response"`
			Action   string          `json:"action"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &f))
	n := 0
	for _, c := range f.Cases {
		var r struct {
			Error *struct {
				Code int             `json:"code"`
				Data json.RawMessage `json:"data"`
			} `json:"error"`
		}
		if json.Unmarshal(c.Response, &r) != nil || r.Error == nil {
			continue // not a JSON-RPC error row (success, transport, multi-step rows)
		}
		n++
		require.Equal(t, c.Action == "retry", IsParserNegotiation(r.Error.Code, r.Error.Data), c.Name)
	}
	require.Positive(t, n)
}
