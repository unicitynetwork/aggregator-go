package gateway

import (
	"context"
	"crypto"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	abcrypto "github.com/unicitynetwork/bft-go-base/crypto"
	testsig "github.com/unicitynetwork/bft-go-base/testutils/sig"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
)

func Test_GetTrustBases(t *testing.T) {
	testLogger, err := logger.New("info", "text", "stdout", false)
	require.NoError(t, err)

	s1, v1 := testsig.CreateSignerAndVerifier(t)
	trustBase1, err := types.NewTrustBase(types.NetworkLocal, []*types.NodeInfo{newNodeInfoFromVerifier(t, "1", v1)},
		types.WithEpoch(1),
		types.WithEpochStart(1),
		types.WithQuorumThreshold(1),
	)
	require.NoError(t, err)
	require.NoError(t, trustBase1.Sign("1", s1))

	_, v2 := testsig.CreateSignerAndVerifier(t)
	trustBase1Hash, err := trustBase1.Hash(crypto.SHA256)
	require.NoError(t, err)
	trustBase2, err := types.NewTrustBase(types.NetworkLocal, []*types.NodeInfo{newNodeInfoFromVerifier(t, "2", v2)},
		types.WithEpoch(2),
		types.WithEpochStart(2),
		types.WithQuorumThreshold(1),
		types.WithPreviousTrustBaseHash(trustBase1Hash),
	)
	require.NoError(t, err)
	require.NoError(t, trustBase2.Sign("1", s1)) // sign by previous validator

	service := mockTrustBaseService{
		tbs: []*types.RootTrustBaseV1{trustBase1, trustBase2},
	}
	hf := getTrustBaseHandler(testLogger, service)

	t.Run("ok with default values", func(t *testing.T) {
		res, body := doRequest(t, hf, http.MethodGet, "/api/v1/trustbases")
		require.Equal(t, http.StatusOK, res.StatusCode)
		require.Equal(t, "application/cbor", res.Header.Get("Content-Type"))

		var response TrustBasesResponse
		require.NoError(t, types.Cbor.Unmarshal(body, &response))
		require.Len(t, response.TrustBases, 1)

		require.EqualValues(t, 2, response.TrustBases[0].GetEpoch())
	})

	t.Run("ok with multiple trust bases", func(t *testing.T) {
		res, body := doRequest(t, hf, http.MethodGet, "/api/v1/trustbases?from=1&to=2")
		require.Equal(t, http.StatusOK, res.StatusCode)

		var response TrustBasesResponse
		require.NoError(t, types.Cbor.Unmarshal(body, &response))
		require.Len(t, response.TrustBases, 2)

		require.EqualValues(t, 1, response.TrustBases[0].GetEpoch())
		require.EqualValues(t, 2, response.TrustBases[1].GetEpoch())
	})

	t.Run("ok with explicit params", func(t *testing.T) {
		res, _ := doRequest(t, hf, http.MethodGet, "/api/v1/trustbases?from=2&to=2")
		require.Equal(t, http.StatusOK, res.StatusCode)
	})

	t.Run("bad_request_invalid_epoch", func(t *testing.T) {
		res, _ := doRequest(t, hf, http.MethodGet, "/api/v1/trustbases?from=0")
		require.Equal(t, http.StatusBadRequest, res.StatusCode)
	})

	t.Run("bad_request_from_gt_to", func(t *testing.T) {
		res, _ := doRequest(t, hf, http.MethodGet, "/api/v1/trustbases?from=5&to=3")
		require.Equal(t, http.StatusBadRequest, res.StatusCode)
	})

	t.Run("bad_request_epoch_gt_latest", func(t *testing.T) {
		res, _ := doRequest(t, hf, http.MethodGet, "/api/v1/trustbases?to=999")
		require.Equal(t, http.StatusBadRequest, res.StatusCode)
	})
}

func doRequest(t *testing.T, hf gin.HandlerFunc, method, path string) (*http.Response, []byte) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, nil)
	ginCtx := testGinContext(rec, req)
	hf(ginCtx)
	res := rec.Result()
	defer res.Body.Close()
	responseBody, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return res, responseBody
}

func testGinContext(rec *httptest.ResponseRecorder, req *http.Request) *gin.Context {
	gin.SetMode(gin.TestMode)
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = req
	return ctx
}

func newNodeInfoFromVerifier(t *testing.T, nodeID string, sigVerifier abcrypto.Verifier) *types.NodeInfo {
	sigKey, err := sigVerifier.MarshalPublicKey()
	require.NoError(t, err)
	return &types.NodeInfo{
		NodeID: nodeID,
		SigKey: sigKey,
		Stake:  1,
	}
}

type mockTrustBaseService struct {
	tbs []*types.RootTrustBaseV1
}

func (mockTrustBaseService) PutTrustBase(ctx context.Context, req *types.RootTrustBaseV1) error {
	panic("implement me")
}

func (m mockTrustBaseService) GetTrustBases(ctx context.Context, from, to uint64) ([]*types.RootTrustBaseV1, error) {
	var result []*types.RootTrustBaseV1
	for _, tb := range m.tbs {
		epoch := tb.GetEpoch()
		if epoch >= from && epoch <= to {
			result = append(result, tb)
		}
	}
	return result, nil
}

func (m mockTrustBaseService) GetLatestTrustBase(ctx context.Context) (*types.RootTrustBaseV1, error) {
	if len(m.tbs) == 0 {
		return nil, errors.New("no trust bases found")
	}
	return m.tbs[len(m.tbs)-1], nil
}
