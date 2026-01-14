package models

import (
	"fmt"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// CertificationData represents the certification data of a state transition certification request
type CertificationData struct {
	_               struct{}            `cbor:",toarray"`
	OwnerPredicate  api.Predicate       `json:"ownerPredicate"`
	SourceStateHash api.SourceStateHash `json:"sourceStateHash"`
	TransactionHash api.TransactionHash `json:"transactionHash"`
	Witness         api.HexBytes        `json:"witness"`
}

type CertificationDataBSON struct {
	OwnerPredicate  PredicateBSON `bson:"ownerPredicate"`
	SourceStateHash string        `bson:"sourceStateHash"`
	TransactionHash string        `bson:"transactionHash"`
	Witness         string        `bson:"witness"`
}

type PredicateBSON struct {
	Engine uint   `bson:"engine"`
	Code   []byte `bson:"code"`
	Params []byte `bson:"params"`
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
		OwnerPredicate: PredicateBSON{
			Engine: a.OwnerPredicate.Engine,
			Code:   a.OwnerPredicate.Code,
			Params: a.OwnerPredicate.Params,
		},
		SourceStateHash: a.SourceStateHash.String(),
		TransactionHash: a.TransactionHash.String(),
		Witness:         a.Witness.String(),
	}
}

func (ab *CertificationDataBSON) FromBSON() (*CertificationData, error) {
	signature, err := api.NewHexBytesFromString(ab.Witness)
	if err != nil {
		return nil, fmt.Errorf("failed to parse signature: %w", err)
	}

	return &CertificationData{
		OwnerPredicate: api.Predicate{
			Engine: ab.OwnerPredicate.Engine,
			Code:   ab.OwnerPredicate.Code,
			Params: ab.OwnerPredicate.Params,
		},
		SourceStateHash: api.SourceStateHash(ab.SourceStateHash),
		TransactionHash: api.TransactionHash(ab.TransactionHash),
		Witness:         signature,
	}, nil
}
