package models

import "github.com/unicitynetwork/aggregator-go/pkg/api"

// HealthStatus represents the health status of the service
type HealthStatus struct {
	Status   string            `json:"status"`
	Role     string            `json:"role"`
	ServerID string            `json:"serverId"`
	Sharding api.Sharding      `json:"sharding"`
	Details  map[string]string `json:"details,omitempty"`
}

// NewHealthStatus creates a new health status
func NewHealthStatus(role, serverID string, sharding api.Sharding) *HealthStatus {
	return &HealthStatus{
		Status:   "ok",
		Role:     role,
		ServerID: serverID,
		Sharding: sharding,
		Details:  make(map[string]string),
	}
}

// AddDetail adds a detail to the health status
func (h *HealthStatus) AddDetail(key, value string) {
	if h.Details == nil {
		h.Details = make(map[string]string)
	}
	h.Details[key] = value
}
