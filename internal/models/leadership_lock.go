package models

import (
	"time"
)

// LeadershipLock represents the leadership lock document in MongoDB
type LeadershipLock struct {
	LockID        string    `json:"lockId" bson:"lockId"`
	ServerID      string    `json:"serverId" bson:"serverId"`
	LastHeartbeat time.Time `json:"lastHeartbeat" bson:"lastHeartbeat"`
}

func (l *LeadershipLock) IsExpired(ttlSeconds int) bool {
	return l.LastHeartbeat.Add(time.Duration(ttlSeconds) * time.Second).Before(time.Now())
}

// IsActive returns true if the lock is owned by the given server and is not expired
func (l *LeadershipLock) IsActive(serverID string, ttlSeconds int) bool {
	return l.ServerID == serverID && !l.IsExpired(ttlSeconds)
}
