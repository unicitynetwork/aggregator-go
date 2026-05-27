package store

import (
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/smt/disk"
	"github.com/unicitynetwork/aggregator-go/internal/smt/disk/storage"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const (
	SchemaVersion = "1"
	TreeLayout    = "yellowpaper-rsmt-sha256-v1"
	KeyBits       = "256"
)

const (
	metaPrefix = "m/"
	nodePrefix = "n/"

	metaRoot          = "root"
	metaRootBlock     = "root_block"
	metaSchemaVersion = "schema_version"
	metaTreeLayout    = "tree_layout"
	metaKeyBits       = "key_bits"
)

type CommittedState = storage.CommittedState

func metaKey(name string) []byte {
	out := make([]byte, 0, len(metaPrefix)+len(name))
	out = append(out, metaPrefix...)
	out = append(out, name...)
	return out
}

func nodeKey(key disk.NodeKey) []byte {
	return appendNodeKey(nil, key)
}

func appendNodeKey(dst []byte, key disk.NodeKey) []byte {
	dst = append(dst, nodePrefix...)
	return key.AppendBytes(dst)
}

func encodeBlockNumber(blockNumber *api.BigInt) []byte {
	if blockNumber == nil || blockNumber.Int == nil {
		return []byte{}
	}
	return []byte(blockNumber.String())
}

func decodeBlockNumber(data []byte) (*api.BigInt, error) {
	if len(data) == 0 {
		return nil, nil
	}
	blockNumber, err := api.NewBigIntFromString(string(data))
	if err != nil {
		return nil, fmt.Errorf("decode root block metadata: %w", err)
	}
	return blockNumber, nil
}
