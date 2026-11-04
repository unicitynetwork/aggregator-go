package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

// The README of the shared fixtures lists their checksums; B and C verify the copies they take.
func TestLongPollFixtures_ChecksumsMatchReadme(t *testing.T) {
	const dir = "../../testdata/longpoll/"
	readme, err := os.ReadFile(dir + "README.md")
	require.NoError(t, err)
	rows := regexp.MustCompile("(?m)^\\| `([^`]+)` \\| ([0-9a-f]{64}) \\|$").FindAllStringSubmatch(string(readme), -1)
	require.Len(t, rows, 5)
	for _, r := range rows {
		b, err := os.ReadFile(dir + r[1])
		require.NoError(t, err, r[1])
		sum := sha256.Sum256(b)
		require.Equal(t, r[2], hex.EncodeToString(sum[:]), r[1])
		require.True(t, json.Valid(b), r[1])
	}
}

// Every echo example is a well-formed envelope. An echo appears exactly on requests whose params
// object has a waitMs member (the decoded name, valid value or not) and which passed preflight (a
// string or number RPC ID); the exact text of requests which a parsed object cannot express
// (duplicates, escaped names) is in rawParams or rawRequest.
func TestLongPollFixtures_EchoExamplesAreConsistent(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/longpoll/echo.json")
	require.NoError(t, err)
	var f struct {
		Cases []struct {
			Name       string          `json:"name"`
			Request    json.RawMessage `json:"request"`
			RawParams  string          `json:"rawParams"`
			RawRequest string          `json:"rawRequest"`
			Response   json.RawMessage `json:"response"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &f))
	require.NotEmpty(t, f.Cases)
	for _, c := range f.Cases {
		body := c.Request
		if c.RawRequest != "" {
			body = json.RawMessage(c.RawRequest)
		}
		var req struct {
			ID     json.RawMessage `json:"id"`
			Params json.RawMessage `json:"params"`
		}
		require.NoError(t, json.Unmarshal(body, &req), c.Name)
		params := req.Params
		if c.RawParams != "" {
			params = json.RawMessage(c.RawParams)
		}
		require.True(t, json.Valid(params), c.Name)
		w, _ := ScanWaitMs(params)
		validID := len(req.ID) > 0 && (req.ID[0] == '"' || req.ID[0] == '-' || (req.ID[0] >= '0' && req.ID[0] <= '9'))
		hasEcho := regexp.MustCompile(`"waitedMs"`).Match(c.Response)
		require.Equal(t, w.Present && validID, hasEcho, c.Name)
		if !validID {
			require.Contains(t, c.Name, "preflight", c.Name)
		}
	}
}
