package api

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

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
