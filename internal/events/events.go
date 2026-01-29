package events

const (
	TopicLeaderChanged Topic = "leaderChanged"
	TopicFatalError    Topic = "fatalError"
)

// LeaderChangedEvent is published when the node becomes a leader
// or when the node loses leader status and becomes a follower.
type LeaderChangedEvent struct {
	IsLeader bool
}

// FatalErrorEvent signals an unrecoverable error that should trigger shutdown.
type FatalErrorEvent struct {
	Source string
	Error  string
}
