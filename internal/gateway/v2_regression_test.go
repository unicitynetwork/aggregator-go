package gateway

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	"github.com/unicitynetwork/aggregator-go/pkg/jsonrpc"
)

type fixedV2Service struct {
	Service
	resp *api.GetInclusionProofResponseV2
}

func (f fixedV2Service) GetInclusionProofV2(context.Context, *api.GetInclusionProofRequestV2) (*api.GetInclusionProofResponseV2, error) {
	return f.resp, nil
}

func v2Fixture() *api.GetInclusionProofResponseV2 {
	rt := uint64(1755000000)
	return &api.GetInclusionProofResponseV2{
		BlockNumber: 42,
		InclusionProof: &api.InclusionProofV2{
			CertificationData: &api.CertificationData{
				Version:         api.CertificationDataVersion,
				OwnerPredicate:  api.Predicate{Engine: 1, Code: []byte{1}, Params: bytes.Repeat([]byte{2}, 33)},
				SourceStateHash: bytes.Repeat([]byte{3}, 32),
				TransactionHash: api.RequireNewImprintV2(strings.Repeat("22", 32)),
				Witness:         bytes.Repeat([]byte{4}, 65),
			},
			ReferenceTime:      &rt,
			CertificateBytes:   make([]byte, api.BitmapSize),
			UnicityCertificate: types.RawCBOR{0x01},
		},
	}
}

// newRPCTestServer is a gateway JSON-RPC server with the v2 handler (and
// anything the caller registers) on top of the given service.
func newRPCTestServer(t *testing.T, svc Service, register func(s *Server)) *jsonrpc.Server {
	t.Helper()
	log, err := logger.New("error", "text", "stdout", false)
	require.NoError(t, err)
	rpc := jsonrpc.NewServer(log, 8)
	s := &Server{logger: log, rpcServer: rpc, service: svc}
	rpc.RegisterMethod("get_inclusion_proof.v2", s.handleGetInclusionProofV2)
	if register != nil {
		register(s)
	}
	return rpc
}

func postRPC(t *testing.T, h http.Handler, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

// get_inclusion_proof.v2 must stay byte-identical: the full HTTP body of a
// success and of a validation failure are pinned.
func TestGetInclusionProofV2_ByteIdentical(t *testing.T) {
	rpc := newRPCTestServer(t, fixedV2Service{resp: v2Fixture()}, nil)

	stateID := strings.Repeat("ab", 32)
	code, body := postRPC(t, rpc, `{"jsonrpc":"2.0","id":7,"method":"get_inclusion_proof.v2","params":{"stateId":"`+stateID+`"}}`)
	t.Logf("BODY1 %q", body)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, goldenV2Success, body)

	t.Logf("BODY2 %q", body)
	code, body = postRPC(t, rpc, `{"jsonrpc":"2.0","id":8,"method":"get_inclusion_proof.v2","params":{"stateId":"abcd"}}`)
	require.Equal(t, http.StatusOK, code)
	t.Logf("BODY3 %q", body)
	require.Equal(t, goldenV2BadID, body)
}
