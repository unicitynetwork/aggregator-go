package api

import "github.com/unicitynetwork/bft-go-base/types"

type Predicate struct {
	_      struct{} `cbor:",toarray"`
	Engine uint     `json:"engine"`
	Code   []byte   `json:"code"`
	Params []byte   `json:"params"`
}

func NewPayToPublicKeyPredicate(publicKey []byte) Predicate {
	return Predicate{
		Engine: 1,
		Code:   []byte{1},
		Params: publicKey,
	}
}

// Predicate is tag-wrapped but intentionally carries no Version field — the
// Engine field is already the shape discriminator.
func (p Predicate) MarshalCBOR() ([]byte, error) {
	type alias Predicate
	return types.Cbor.MarshalTaggedValue(PredicateTag, alias(p))
}

func (p *Predicate) UnmarshalCBOR(data []byte) error {
	type alias Predicate
	return types.Cbor.UnmarshalTaggedValue(PredicateTag, data, (*alias)(p))
}
