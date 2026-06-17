package smt

import "github.com/unicitynetwork/aggregator-go/pkg/api"

func legacyPathRootHex(tree *SparseMerkleTree) string {
	return api.NewDataHash(tree.algorithm, tree.GetRootHashRaw()).ToHex()
}
