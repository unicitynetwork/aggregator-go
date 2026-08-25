package signing

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/stretchr/testify/require"
	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Demonstrates: a well-formed request whose current-owner predicate is one of
// the yellowpaper's built-in codes other than 0x01 is rejected outright by the
// Unicity Service, with a status string clients see as "UNKNOWN".
func TestLens_NonSigBuiltinPredicatesRejected(t *testing.T) {
	validator := newDefaultCertificationRequestValidator()

	priv, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	pk := priv.PubKey().SerializeCompressed()

	sourceStateHash := CreateDataHash([]byte("state"))
	txDataHash := CreateDataHash([]byte("tx"))
	txHash := txDataHash.Imprint()

	// params encodings per appendix-token.tex:53-63
	tlockParams, err := types.Cbor.Marshal([]interface{}{pk, uint64(1000)})
	require.NoError(t, err)
	pkh := sha256.Sum256(pk)
	msigParams, err := types.Cbor.Marshal([]interface{}{pk})
	require.NoError(t, err)
	tsigParams, err := types.Cbor.Marshal([]interface{}{uint64(1), []interface{}{pk}})
	require.NoError(t, err)
	y := sha256.Sum256([]byte("preimage"))
	htlcParams, err := types.Cbor.Marshal([]interface{}{pk, pk, y[:], uint64(2000)})
	require.NoError(t, err)

	cases := []struct {
		name   string
		code   byte
		params []byte
	}{
		{"0x02 burn", 0x02, pkh[:]},
		{"0x03 tlock", 0x03, tlockParams},
		{"0x04 p2pkh", 0x04, pkh[:]},
		{"0x05 p2sh", 0x05, pkh[:]},
		{"0x06 msig", 0x06, msigParams},
		{"0x07 tsig", 0x07, tsigParams},
		{"0x08 htlc", 0x08, htlcParams},
	}

	for _, c := range cases {
		pred := api.Predicate{Engine: 1, Code: []byte{c.code}, Params: c.params}
		stateID, err := api.CreateStateID(pred, sourceStateHash)
		require.NoError(t, err)

		// A genuinely satisfying unlocking argument for the sig-shaped paths:
		// signature over m = H(sthash, txhash).
		sigDataHash := api.SigDataHash(sourceStateHash, txHash)
		sig, err := NewSigningService().SignDataHash(sigDataHash, priv.Serialize())
		require.NoError(t, err)

		req := &models.CertificationRequest{
			StateID: stateID,
			CertificationData: models.CertificationData{
				OwnerPredicate:  pred,
				SourceStateHash: sourceStateHash,
				TransactionHash: txDataHash,
				Witness:         api.HexBytes(sig),
			},
		}
		res := validator.Validate(req)
		fmt.Printf("%-12s -> status=%d string=%q err=%v\n", c.name, res.Status, res.Status.String(), res.Error)
		require.Equal(t, ValidationStatusInvalidOwnerPredicate, res.Status)
		require.Equal(t, "UNKNOWN", res.Status.String())
	}
}

// Demonstrates the exact bytes hashed for sid and for m.
func TestLens_SidAndMPreimages(t *testing.T) {
	pk, err := hex.DecodeString("02" + "11"+"22"+"33"+"44"+"55"+"66"+"77"+"88"+"99"+"aa"+"bb"+"cc"+"dd"+"ee"+"ff"+"00"+"11"+"22"+"33"+"44"+"55"+"66"+"77"+"88"+"99"+"aa"+"bb"+"cc"+"dd"+"ee"+"ff"+"00")
	require.NoError(t, err)
	pred := api.NewPayToPublicKeyPredicate(pk)

	sth := make([]byte, 32)
	for i := range sth {
		sth[i] = byte(i)
	}
	txh := make([]byte, 32)
	for i := range txh {
		txh[i] = byte(0x80 + i)
	}

	type stateIDInput struct {
		_               struct{} `cbor:",toarray"`
		OwnerPredicate  api.Predicate
		SourceStateHash []byte
	}
	b, err := types.Cbor.Marshal(stateIDInput{OwnerPredicate: pred, SourceStateHash: sth})
	require.NoError(t, err)
	fmt.Printf("sid preimage = %x\n", b)
	sid, err := api.CreateStateID(pred, sth)
	require.NoError(t, err)
	fmt.Printf("sid          = %x\n", []byte(sid))
	h := sha256.Sum256(b)
	require.Equal(t, h[:], []byte(sid))

	mPre := append([]byte{0x82, 0x58, 0x20}, sth...)
	mPre = append(mPre, 0x58, 0x20)
	mPre = append(mPre, txh...)
	mh := sha256.Sum256(mPre)
	fmt.Printf("m preimage   = %x\nm            = %x\n", mPre, mh[:])
	require.Equal(t, mh[:], api.SigDataHash(sth, txh).RawHash)

	// Leaf value lambda(Q,tau) = H(CBOR([txhash, tau]))
	lv := api.LeafValue(txh, 1700000000)
	lvPre := append([]byte{0x82, 0x58, 0x20}, txh...)
	lvPre = append(lvPre, 0x1a, 0x65, 0x53, 0xf1, 0x00)
	lvh := sha256.Sum256(lvPre)
	fmt.Printf("leaf preimage= %x\n", lvPre)
	require.Equal(t, lvh[:], lv)
}
