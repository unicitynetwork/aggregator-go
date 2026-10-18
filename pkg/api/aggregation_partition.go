package api

import "github.com/unicitynetwork/bft-go-base/types"

// AggregationPartitionID is the BFT partition of every Unicity aggregation layer. It is a protocol constant, not
// configuration: all mainnet and testnet shards run in partition 7 (mainnet is network 1, testnet2 is network 4), and
// a proof never selects the partition it is verified against. The networks differ by network ID and authority only.
//
// Every application verifier (inclusion, non-inclusion, consolidation, partition views) requires the authenticated
// unicity certificate to be of this partition. The generic BFT codecs and path primitives stay partition-agnostic.
const AggregationPartitionID types.PartitionID = 7
