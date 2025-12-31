package models

import (
	"fmt"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// CertificationData represents the certification data of a state transition certification request
type CertificationData struct {
	_               struct{}            `cbor:",toarray"`
	OwnerPredicate  api.HexBytes        `json:"ownerPredicate"`
	SourceStateHash api.SourceStateHash `json:"sourceStateHash"`
	TransactionHash api.TransactionHash `json:"transactionHash"`
	Witness         api.HexBytes        `json:"witness"`
}

type CertificationDataBSON struct {
	OwnerPredicate  string `bson:"ownerPredicate"`
	SourceStateHash string `bson:"sourceStateHash"`
	TransactionHash string `bson:"transactionHash"`
	Witness         string `bson:"witness"`
}

func (a *CertificationData) ToAPI() *api.CertificationData {
	return &api.CertificationData{
		OwnerPredicate:  a.OwnerPredicate,
		SourceStateHash: a.SourceStateHash,
		TransactionHash: a.TransactionHash,
		Witness:         a.Witness,
	}
}

func (a *CertificationData) ToBSON() CertificationDataBSON {
	return CertificationDataBSON{
		OwnerPredicate:  a.OwnerPredicate.String(),
		SourceStateHash: a.SourceStateHash.String(),
		TransactionHash: a.TransactionHash.String(),
		Witness:         a.Witness.String(),
	}
}

func (ab *CertificationDataBSON) FromBSON() (*CertificationData, error) {
	ownerPredicate, err := api.NewHexBytesFromString(ab.OwnerPredicate)
	if err != nil {
		return nil, fmt.Errorf("failed to parse owner predicate: %w", err)
	}

	signature, err := api.NewHexBytesFromString(ab.Witness)
	if err != nil {
		return nil, fmt.Errorf("failed to parse signature: %w", err)
	}

	return &CertificationData{
		OwnerPredicate:  ownerPredicate,
		SourceStateHash: api.SourceStateHash(ab.SourceStateHash),
		TransactionHash: api.TransactionHash(ab.TransactionHash),
		Witness:         signature,
	}, nil
}
