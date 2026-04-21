package api

import "github.com/unicitynetwork/bft-go-base/types"

// CBOR tag registry for aggregator-go types. Source of truth:
// https://github.com/unicitynetwork/unicity-ids/blob/main/cbor-tags.json
const (
	CertificationRequestTag types.CborTag = 39030
	CertificationDataTag    types.CborTag = 39031
	PredicateTag            types.CborTag = 39032
	InclusionProofTag       types.CborTag = 39033
)
