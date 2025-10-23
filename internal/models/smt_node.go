package models

import (
	"fmt"
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
	Key       string    `bson:"key"`
	Value     string    `bson:"value"`
	CreatedAt time.Time `bson:"createdAt"`
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
		Key:       n.Key.String(),
		Value:     n.Value.String(),
		CreatedAt: n.CreatedAt.Time,
	}
}

func (nb *SmtNodeBSON) FromBSON() (*SmtNode, error) {
	key, err := api.NewHexBytesFromString(nb.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key: %w", err)
	}

	val, err := api.NewHexBytesFromString(nb.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse value: %w", err)
	}
	return &SmtNode{
		Key:       key,
		Value:     val,
		CreatedAt: api.NewTimestamp(nb.CreatedAt),
	}, nil
}
