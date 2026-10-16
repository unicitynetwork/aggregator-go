package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// No-wait golden responses of get_inclusion_proof.v2 (long-poll T3), recorded from the handler
// before any long-poll change. Regenerate deliberately with LONGPOLL_GOLDEN_UPDATE=1.

const longPollGoldenV2 = "../../testdata/longpoll/golden/v2-nowait.json"

type goldenCase struct {
	Name     string `json:"name"`
	Request  string `json:"request"`
	Response string `json:"response"`
}

type errV2Service struct {
	fixedV2Service
	err error
}

func (s errV2Service) GetInclusionProofV2(context.Context, *api.GetInclusionProofRequestV2) (*api.GetInclusionProofResponseV2, error) {
	return nil, s.err
}

func TestLongPollGolden_V2NoWait(t *testing.T) {
	req := func(id int, params string) string {
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"get_inclusion_proof.v2","params":%s}`, id, params)
	}
	sid := `"stateId":"` + stateIDHex + `"`
	included := fixedV2Service{resp: v2Fixture()}
	empty := fixedV2Service{resp: emptyV2Fixture()}
	status := func(s api.ProofStatus, resp *api.GetInclusionProofResponseV2) Service {
		return &statusV2Service{fixedV2Service: fixedV2Service{resp: resp}, hint: s}
	}
	type spec struct {
		name string
		svc  Service
		body string
	}
	specs := []spec{
		{"included-bare", included, req(1, `{`+sid+`}`)},
		{"empty-proof-bare", empty, req(2, `{`+sid+`}`)},
		{"included-status-false", included, req(3, `{`+sid+`,"includeStatus":false}`)},
		{"included-status", status(api.ProofStatusPending, v2Fixture()), req(4, `{`+sid+`,"includeStatus":true}`)},
		{"pending-status", status(api.ProofStatusPending, emptyV2Fixture()), req(5, `{`+sid+`,"includeStatus":true}`)},
		{"expired-status", status(api.ProofStatusExpired, emptyV2Fixture()), req(6, `{`+sid+`,"includeStatus":true}`)},
		{"unknown-status", status(api.ProofStatusUnknown, emptyV2Fixture()), req(7, `{`+sid+`,"includeStatus":true}`)},
		{"status-without-provider", empty, req(8, `{`+sid+`,"includeStatus":true}`)},
		{"unknown-extra-field-ignored", included, req(9, `{`+sid+`,"foo":1}`)},
		{"no-finalized-view", errV2Service{err: api.ErrProofNotYetAvailable}, req(10, `{`+sid+`}`)},
		{"internal-error", errV2Service{err: errors.New("db down")}, req(11, `{`+sid+`}`)},
		{"state-id-too-short", included, req(12, `{"stateId":"abcd"}`)},
		{"state-id-missing", included, req(13, `{}`)},
		{"params-not-an-object", included, req(14, `"x"`)},
		{"params-array", included, req(15, `[1]`)},
		{"include-status-wrong-type", included, req(16, `{`+sid+`,"includeStatus":"yes"}`)},
	}
	var got []goldenCase
	for _, s := range specs {
		_, body := postRPC(t, newRPCTestServer(t, s.svc, nil), s.body)
		got = append(got, goldenCase{Name: s.name, Request: s.body, Response: body})
	}

	enc, err := json.MarshalIndent(got, "", "  ")
	require.NoError(t, err)
	enc = append(enc, '\n')
	if os.Getenv("LONGPOLL_GOLDEN_UPDATE") != "" {
		require.NoError(t, os.MkdirAll(filepath.Dir(longPollGoldenV2), 0o755))
		require.NoError(t, os.WriteFile(longPollGoldenV2, enc, 0o644))
		return
	}
	want, err := os.ReadFile(longPollGoldenV2)
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
