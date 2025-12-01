package models

import (
	"fmt"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// CertificationData represents the certification data of a state transition certification request
type CertificationData struct {
	_               struct{}            `cbor:",toarray"`
	PublicKey       api.HexBytes        `json:"publicKey" bson:"publicKey"`
	SourceStateHash api.SourceStateHash `json:"sourceStateHash" bson:"sourceStateHash"`
	TransactionHash api.TransactionHash `json:"transactionHash" bson:"transactionHash"`
	Signature       api.HexBytes        `json:"signature" bson:"signature"`
}

type CertificationDataBSON struct {
	PublicKey       string `bson:"publicKey"`
	SourceStateHash string `bson:"sourceStateHash"`
	TransactionHash string `bson:"transactionHash"`
	Signature       string `bson:"signature"`
}

func (a *CertificationData) ToAPI() *api.CertificationData {
	return &api.CertificationData{
		PublicKey:       a.PublicKey,
		SourceStateHash: a.SourceStateHash,
		TransactionHash: a.TransactionHash,
		Signature:       a.Signature,
	}
}

func (a *CertificationData) ToBSON() CertificationDataBSON {
	return CertificationDataBSON{
		PublicKey:       a.PublicKey.String(),
		SourceStateHash: a.SourceStateHash.String(),
		TransactionHash: a.TransactionHash.String(),
		Signature:       a.Signature.String(),
	}
}

func (ab *CertificationDataBSON) FromBSON() (*CertificationData, error) {
	publicKey, err := api.NewHexBytesFromString(ab.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse publicKey: %w", err)
	}

	signature, err := api.NewHexBytesFromString(ab.Signature)
	if err != nil {
		return nil, fmt.Errorf("failed to parse signature: %w", err)
	}

	return &CertificationData{
		PublicKey:       publicKey,
		SourceStateHash: api.SourceStateHash(ab.SourceStateHash),
		TransactionHash: api.TransactionHash(ab.TransactionHash),
		Signature:       signature,
	}, nil
}
