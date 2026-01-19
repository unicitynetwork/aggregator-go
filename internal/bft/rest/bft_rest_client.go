package rest

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/unicitynetwork/bft-go-base/types"
)

type BFTRestClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewBFTRestClient(baseURL string) *BFTRestClient {
	return &BFTRestClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

type GetTrustBasesResponse struct {
	_          struct{}                 `cbor:",toarray"`
	TrustBases []*types.RootTrustBaseV1 `cbor:"trustBases"`
}

func (c *BFTRestClient) GetTrustBases(ctx context.Context, epoch1, epoch2 uint64) ([]*types.RootTrustBaseV1, error) {
	u, err := url.Parse(c.baseURL + "/api/v1/trustbases")
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}

	q := u.Query()
	q.Set("epoch1", strconv.FormatUint(epoch1, 10))
	q.Set("epoch2", strconv.FormatUint(epoch2, 10))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/cbor")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status: %s", resp.Status)
	}

	var response GetTrustBasesResponse
	if err := types.Cbor.GetDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode CBOR response: %w", err)
	}
	return response.TrustBases, nil
}
