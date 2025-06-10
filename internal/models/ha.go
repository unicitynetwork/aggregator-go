package models

import (
	"time"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// LeadershipStatus represents the current leadership status
type LeadershipStatus struct {
	IsLeader bool   `json:"isLeader"`
	ServerID string `json:"serverId"`
	Role     string `json:"role"` // "leader", "follower", "standalone"
}

// NewLeadershipStatus creates a new leadership status
func NewLeadershipStatus(isLeader bool, serverID string, haEnabled bool) *LeadershipStatus {
	role := "follower"
	if !haEnabled {
		role = "standalone"
	} else if isLeader {
		role = "leader"
	}

	return &LeadershipStatus{
		IsLeader: isLeader,
		ServerID: serverID,
		Role:     role,
	}
}

// LeadershipLock represents the leadership lock document in MongoDB
type LeadershipLock struct {
	ID        string         `json:"id" bson:"_id"`
	ServerID  string         `json:"serverId" bson:"serverId"`
	ExpiresAt *api.Timestamp `json:"expiresAt" bson:"expiresAt"`
	UpdatedAt *api.Timestamp `json:"updatedAt" bson:"updatedAt"`
}

// NewLeadershipLock creates a new leadership lock
func NewLeadershipLock(serverID string, ttlSeconds int) *LeadershipLock {
	now := api.Now()
	expiresAt := api.NewTimestamp(now.Time.Add(time.Second * time.Duration(ttlSeconds)))

	return &LeadershipLock{
		ID:        "leadership",
		ServerID:  serverID,
		ExpiresAt: expiresAt,
		UpdatedAt: now,
	}
}

// IsExpired checks if the lock has expired
func (l *LeadershipLock) IsExpired() bool {
	return l.ExpiresAt.Time.Before(time.Now())
}

// HealthStatus represents the health status of the service
type HealthStatus struct {
	Status   string            `json:"status"`
	Role     string            `json:"role"`
	ServerID string            `json:"serverId"`
	Details  map[string]string `json:"details,omitempty"`
}

// NewHealthStatus creates a new health status
func NewHealthStatus(role, serverID string) *HealthStatus {
	return &HealthStatus{
		Status:   "ok",
		Role:     role,
		ServerID: serverID,
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
