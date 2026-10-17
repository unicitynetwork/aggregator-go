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

// Every echo example is a well-formed envelope, and echoes appear only on opt-in requests.
func TestLongPollFixtures_EchoExamplesAreConsistent(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/longpoll/echo.json")
	require.NoError(t, err)
	var f struct {
		Cases []struct {
			Name     string          `json:"name"`
			Request  json.RawMessage `json:"request"`
			Response json.RawMessage `json:"response"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &f))
	require.NotEmpty(t, f.Cases)
	for _, c := range f.Cases {
		var req struct {
			Params map[string]json.RawMessage `json:"params"`
		}
		require.NoError(t, json.Unmarshal(c.Request, &req), c.Name)
		_, optIn := req.Params["waitMs"]
		hasEcho := regexp.MustCompile(`"waitedMs"`).Match(c.Response)
		recognized := optIn && string(req.Params["waitMs"]) != `"5"` && string(req.Params["waitMs"]) != "-1"
		require.Equal(t, recognized, hasEcho, c.Name)
	}
}
