package v1

import (
	"fmt"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Authenticator represents the authentication data for a commitment
type Authenticator struct {
	Algorithm string              `json:"algorithm" bson:"algorithm"`
	PublicKey api.HexBytes        `json:"publicKey" bson:"publicKey"`
	Signature api.HexBytes        `json:"signature" bson:"signature"`
	StateHash api.SourceStateHash `json:"stateHash" bson:"stateHash"`
}

type AuthenticatorBSON struct {
	Algorithm string `bson:"algorithm"`
	PublicKey string `bson:"publicKey"`
	Signature string `bson:"signature"`
	StateHash string `bson:"stateHash"`
}

func (a *Authenticator) ToAPI() *api.Authenticator {
	return &api.Authenticator{
		Algorithm: a.Algorithm,
		PublicKey: a.PublicKey,
		Signature: a.Signature,
		StateHash: a.StateHash,
	}
}

func (a *Authenticator) ToBSON() AuthenticatorBSON {
	return AuthenticatorBSON{
		Algorithm: a.Algorithm,
		PublicKey: a.PublicKey.String(),
		Signature: a.Signature.String(),
		StateHash: a.StateHash.String(),
	}
}

func (ab *AuthenticatorBSON) FromBSON() (*Authenticator, error) {
	publicKey, err := api.NewHexBytesFromString(ab.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse publicKey: %w", err)
	}

	signature, err := api.NewHexBytesFromString(ab.Signature)
	if err != nil {
		return nil, fmt.Errorf("failed to parse signature: %w", err)
	}

	return &Authenticator{
		Algorithm: ab.Algorithm,
		PublicKey: publicKey,
		Signature: signature,
		StateHash: api.SourceStateHash(ab.StateHash),
	}, nil
}
