package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// handleHealth handles the health endpoint
func (s *Server) handleHealth(c *gin.Context) {
	ctx := c.Request.Context()

	status, err := s.service.GetHealthStatus(ctx)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to get health status", "error", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		return
	}

	// Return 503 if unhealthy so load balancers can remove from rotation
	if status.Status == api.HealthStatusUnhealthy {
		c.JSON(http.StatusServiceUnavailable, status)
		return
	}

	c.JSON(http.StatusOK, status)
}

// handleDocs handles the API documentation endpoint
func (s *Server) handleDocs(c *gin.Context) {
	html := GenerateDocsHTML()
	c.Header("Content-Type", "text/html")
	c.String(http.StatusOK, html)
}

func (s *Server) handlePutTrustBase(c *gin.Context) {
	ctx := c.Request.Context()

	jsonData, err := io.ReadAll(c.Request.Body)
	if err != nil {
		s.logger.WithContext(ctx).Error("Failed to read request body", "error", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		return
	}

	var trustBase *types.RootTrustBaseV1
	if err := json.Unmarshal(jsonData, &trustBase); err != nil {
		s.logger.WithContext(ctx).Warn("Failed to parse trust base from request body", "error", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to parse trust base from request body"})
		return
	}

	if err := s.service.PutTrustBase(ctx, trustBase); err != nil {
		s.logger.WithContext(ctx).Warn("Failed to store trust base", "error", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()}) // make the actual error visible to client
		return
	}

	c.JSON(http.StatusOK, gin.H{})
}

type TrustBasesResponse struct {
	_          struct{} `cbor:",toarray"`
	TrustBases []*types.RootTrustBaseV1
}

func getTrustBaseHandler(logger *logger.Logger, service TrustBaseService) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		// return trust base records for the epochs from <epoch1> to <epoch2>, inclusively.
		// Both parameters are optional and default to the latest epoch.
		// parameters are uint64 (0 value is not allowed by business rules)
		latestTrustBase, err := service.GetLatestTrustBase(ctx)
		if err != nil {
			logger.WithContext(ctx).Error("GET trustbases request: failed to load latest active epoch", "error", err.Error())
			http.Error(c.Writer, "failed to load latest trust base", http.StatusInternalServerError)
			return
		}

		from, to, err := validateQueryParams(c.Request, latestTrustBase.GetEpoch())
		if err != nil {
			logger.WithContext(ctx).Warn("GET trustbases request: %v", "error", err.Error())
			http.Error(c.Writer, err.Error(), http.StatusBadRequest)
			return
		}

		trustBases, err := service.GetTrustBases(ctx, from, to)
		if err != nil {
			logger.WithContext(ctx).Warn("GET trustbases request", "error", err.Error())
			http.Error(c.Writer, "failed to load trust base", http.StatusInternalServerError)
			return
		}

		result := &TrustBasesResponse{TrustBases: trustBases}
		if err := writeCborResponse(c.Writer, result); err != nil {
			logger.WithContext(ctx).Error("failed to write response", "error", err.Error())
		}
	}
}

func writeCborResponse(w http.ResponseWriter, response any) error {
	w.Header().Set("Content-Type", "application/cbor")
	encoder, err := types.Cbor.GetEncoder(w)
	if err != nil {
		return fmt.Errorf("failed to create cbor encoder: %w", err)
	}
	if err := encoder.Encode(response); err != nil {
		return fmt.Errorf("failed to encode response: %w", err)
	}
	return nil
}

func validateQueryParams(r *http.Request, lastEpoch uint64) (uint64, uint64, error) {
	q := r.URL.Query()

	from, err := parseUint64WithDefault(q, "from", lastEpoch)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse from query param: %v", err)
	}
	to, err := parseUint64WithDefault(q, "to", lastEpoch)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse to query param: %v", err)
	}

	if from == 0 || to == 0 {
		return 0, 0, fmt.Errorf("from and to should be greater than 0")
	}
	if from > to {
		return 0, 0, fmt.Errorf("to must be greater than or equal to from")
	}
	if from > lastEpoch || to > lastEpoch {
		return 0, 0, fmt.Errorf("from and to cannot be greater than latest epoch")
	}
	return from, to, nil
}

func parseUint64WithDefault(q url.Values, key string, defaultValue uint64) (uint64, error) {
	valStr := q.Get(key)
	if valStr == "" {
		return defaultValue, nil
	}

	v, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}

	return v, nil
}
