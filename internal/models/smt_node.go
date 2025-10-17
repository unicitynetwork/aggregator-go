package models

import (
	"time"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// SmtNode represents a Sparse Merkle Tree node
type SmtNode struct {
	Key       api.HexBytes   `json:"key"`
	Value     api.HexBytes   `json:"value"`
	CreatedAt *api.Timestamp `json:"createdAt"`
}

type SmtNodeBSON struct {
	Key       api.HexBytes `bson:"key"`
	Value     api.HexBytes `bson:"value"`
	CreatedAt time.Time    `bson:"createdAt"`
}

func NewSmtNode(key, value api.HexBytes) *SmtNode {
	return &SmtNode{
		Key:       key,
		Value:     value,
		CreatedAt: api.Now(),
	}
}

func (n *SmtNode) ToBSON() *SmtNodeBSON {
	return &SmtNodeBSON{
		Key:       n.Key,
		Value:     n.Value,
		CreatedAt: n.CreatedAt.Time,
	}
}

func (nb *SmtNodeBSON) FromBSON() *SmtNode {
	return &SmtNode{
		Key:       nb.Key,
		Value:     nb.Value,
		CreatedAt: api.NewTimestamp(nb.CreatedAt),
	}
}
