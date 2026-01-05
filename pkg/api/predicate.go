package api

import "github.com/unicitynetwork/bft-go-base/types"

type Predicate struct {
	_      struct{} `cbor:",toarray"`
	Engine uint
	Code   []byte
	Params []byte
}

func NewPayToPublicKeyPredicate(publicKey []byte) Predicate {
	return Predicate{
		Engine: 1,
		Code:   []byte{1},
		Params: publicKey,
	}
}

func NewPayToPublicKeyPredicateBytes(publicKey []byte) ([]byte, error) {
	return types.Cbor.Marshal(NewPayToPublicKeyPredicate(publicKey))
}
