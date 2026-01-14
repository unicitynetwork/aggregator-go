package events

const (
	TopicLeaderChanged Topic = "leaderChanged"
)

// LeaderChangedEvent is published when the node becomes a leader
// or when the node loses leader status and becomes a follower.
type LeaderChangedEvent struct {
	IsLeader bool
}
