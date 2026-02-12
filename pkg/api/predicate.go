package api

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
