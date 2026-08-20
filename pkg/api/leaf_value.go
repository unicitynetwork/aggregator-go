package api

// LeafValue returns the sparse Merkle tree leaf value the Unicity Service
// records for an accepted certification request:
//
//	SHA-256( CBOR([transactionHash, referenceTime]) )
//
// The value binds the reference time the request was validated under, not the
// transaction hash alone. The tree is append-only, so a leaf can be certified
// afresh against any later root and a later inclusion proof carries a later
// round's reference time. Binding the reference time into the leaf value fixes
// the value the transition was validated under, for any proof of that leaf.
//
// transactionHash is the raw 32-byte digest (no algorithm-id prefix); the
// returned value is raw 32 bytes, matching the v2 SMT profile.
func LeafValue(transactionHash []byte, referenceTime uint64) []byte {
	return NewDataHasher(SHA256).
		AddData(CborArray(2)).
		AddCborBytes(transactionHash).
		AddData(CborUint(referenceTime)).
		GetHash().
		RawHash
}
