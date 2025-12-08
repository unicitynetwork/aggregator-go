package bft

import (
	gocrypto "crypto"
	"fmt"
	"math/big"
	"math/rand"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/unicitynetwork/bft-go-base/types"
)

const defaultHandshakeNodes = 3
const defaultNofRootNodes = 2

func randomNodeSelector(tb types.RootTrustBase, upToNodes int) (peer.IDSlice, error) {
	nodes, err := rootNodesIDSlice(tb)
	if err != nil {
		return nil, fmt.Errorf("failed to get peer.IDSlice from trust base: %w", err)
	}
	nodeCnt := len(nodes)
	if nodeCnt < 1 {
		return nil, fmt.Errorf("node list is empty")
	}
	if upToNodes == 0 {
		return nil, fmt.Errorf("invalid parameter, number of nodes to select is 0")
	}
	// optimization for wanting more nodes than in the node list - there is not enough to choose from
	if upToNodes >= nodeCnt {
		return nodes, nil
	}
	rNodes := make(peer.IDSlice, len(nodes))
	// make a copy of available nodes
	copy(rNodes, nodes)
	// randomize
	rand.Shuffle(len(rNodes), func(i, j int) {
		rNodes[i], rNodes[j] = rNodes[j], rNodes[i]
	})
	return rNodes[:upToNodes], nil
}

func rootNodesSelector(luc *types.UnicityCertificate, tb types.RootTrustBase, upToNodes int) (peer.IDSlice, error) {
	if luc == nil {
		return nil, fmt.Errorf("UC is nil")
	}
	nodes, err := rootNodesIDSlice(tb)
	if err != nil {
		return nil, fmt.Errorf("failed to get peer.IDSlice from trust base: %w", err)
	}
	nodeCnt := len(nodes)
	if nodeCnt < 1 {
		return nil, fmt.Errorf("root node list is empty")
	}
	if upToNodes == 0 {
		return nil, fmt.Errorf("invalid parameter, number of nodes to select is 0")
	}
	// optimization for wanting more nodes than in the root node list - there is not enough to choose from
	if upToNodes >= nodeCnt {
		return nodes, nil
	}
	chosen := make(peer.IDSlice, 0, upToNodes)
	lucHash, err := luc.Hash(gocrypto.SHA256)
	if err != nil {
		return nil, fmt.Errorf("failed to hash UC: %w", err)
	}
	index := int(big.NewInt(0).Mod(big.NewInt(0).SetBytes(lucHash), big.NewInt(int64(nodeCnt))).Int64())
	// choose upToNodes from index
	idx := index
	for {
		chosen = append(chosen, nodes[idx])
		idx++
		// wrap around and choose from node 0
		if idx >= nodeCnt {
			idx = 0
		}
		// break loop if either again at start index or enough validators have been found
		if idx == index || len(chosen) == upToNodes {
			break
		}
	}
	return chosen, nil
}

func rootNodesIDSlice(tb types.RootTrustBase) (peer.IDSlice, error) {
	var idSlice peer.IDSlice
	for _, node := range tb.GetRootNodes() {
		id, err := peer.Decode(node.NodeID)
		if err != nil {
			return nil, fmt.Errorf("invalid root node id in trust base: %w", err)
		}
		idSlice = append(idSlice, id)
	}
	return idSlice, nil
}
