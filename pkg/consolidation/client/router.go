package client

import "fmt"

// router maps a state key to the endpoint of the shard that owns it with a binary
// trie over the shard prefixes (MSB first, as ShardID.Comparator): O(shard depth)
// per lookup instead of a scan over all endpoints.
type router struct {
	nodes []routeNode
}

type routeNode struct {
	child [2]int32 // 0 = none (the root is never a child)
	ep    int32    // endpoint index + 1 at a shard leaf, 0 otherwise
}

// newRouter builds the trie once. The endpoint shards must be prefix-free.
func newRouter(eps []Endpoint) (*router, error) {
	r := &router{nodes: make([]routeNode, 1, 2*len(eps)+1)}
	for i, ep := range eps {
		bits := ep.ShardID.String()
		n := int32(0)
		for _, c := range bits {
			if r.nodes[n].ep != 0 {
				return nil, fmt.Errorf("shard %q overlaps another endpoint shard", ep.ShardID)
			}
			b := c - '0'
			if r.nodes[n].child[b] == 0 {
				r.nodes = append(r.nodes, routeNode{})
				r.nodes[n].child[b] = int32(len(r.nodes) - 1)
			}
			n = r.nodes[n].child[b]
		}
		if r.nodes[n].ep != 0 || r.nodes[n].child != [2]int32{} {
			return nil, fmt.Errorf("shard %q overlaps another endpoint shard", ep.ShardID)
		}
		r.nodes[n].ep = int32(i) + 1
	}
	return r, nil
}

// lookup returns the endpoint index owning the key (MSB-first bit order), or -1, and
// the number of trie nodes visited (the hardware-independent work metric).
func (r *router) lookup(key []byte) (ep, visits int) {
	n := int32(0)
	visits = 1
	for depth := 0; ; depth++ {
		if e := r.nodes[n].ep; e != 0 {
			return int(e) - 1, visits
		}
		if depth/8 >= len(key) {
			return -1, visits
		}
		b := (key[depth/8] >> (7 - depth%8)) & 1
		n = r.nodes[n].child[b]
		if n == 0 {
			return -1, visits
		}
		visits++
	}
}
