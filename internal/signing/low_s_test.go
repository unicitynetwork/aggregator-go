package signing

import (
	"math/big"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// highSTwin returns the malleated twin of a 65-byte r||s||v signature: s' = n - s, v' = v ^ 1.
// If flipV is false the original v is kept.
func highSTwin(sig []byte, flipV bool) []byte {
	n := btcec.S256().N
	s := new(big.Int).SetBytes(sig[32:64])
	twin := make([]byte, 65)
	copy(twin, sig[:32])
	new(big.Int).Sub(n, s).FillBytes(twin[32:64])
	twin[64] = sig[64]
	if flipV {
		twin[64] ^= 1
	}
	return twin
}

func lowSFixture(t *testing.T) (svc *SigningService, hash, sig, pub []byte) {
	t.Helper()
	svc = NewSigningService()
	priv, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	pub = priv.PubKey().SerializeCompressed()
	hash = make([]byte, 32)
	for i := range hash {
		hash[i] = byte(i + 1)
	}
	sig, err = svc.SignHash(hash, priv.Serialize())
	require.NoError(t, err)
	return
}

func TestVerifyHash_LowSAccepted(t *testing.T) {
	svc, hash, sig, pub := lowSFixture(t)
	ok, err := svc.VerifyHashWithPublicKey(hash, sig, pub)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestVerifyHash_highSTwinRejected(t *testing.T) {
	svc, hash, sig, pub := lowSFixture(t)
	for name, flip := range map[string]bool{"flipped v": true, "original v": false} {
		t.Run(name, func(t *testing.T) {
			ok, err := svc.VerifyHashWithPublicKey(hash, highSTwin(sig, flip), pub)
			require.False(t, ok)
			require.Error(t, err)
		})
	}
}

func TestVerifyHash_OutOfRangeScalarsRejected(t *testing.T) {
	svc, hash, sig, pub := lowSFixture(t)
	n := btcec.S256().N
	cases := map[string]func(s []byte){
		"s = 0": func(s []byte) { clear(s[32:64]) },
		"r = 0": func(s []byte) { clear(s[0:32]) },
		"s = n": func(s []byte) { n.FillBytes(s[32:64]) },
		"s > n": func(s []byte) { new(big.Int).Add(n, big.NewInt(1)).FillBytes(s[32:64]) },
		"r = n": func(s []byte) { n.FillBytes(s[0:32]) },
		"s max": func(s []byte) {
			copy(s[32:64], make([]byte, 32))
			for i := 32; i < 64; i++ {
				s[i] = 0xff
			}
		},
		"v = 4":  func(s []byte) { s[64] = 4 },
		"v = 31": func(s []byte) { s[64] = 31 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			bad := append([]byte(nil), sig...)
			mutate(bad)
			ok, err := svc.VerifyHashWithPublicKey(hash, bad, pub)
			require.False(t, ok)
			require.Error(t, err)
		})
	}
}

func TestValidator_RejectshighSTwin(t *testing.T) {
	validator := newDefaultCertificationRequestValidator()
	priv, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	pred := api.NewPayToPublicKeyPredicate(priv.PubKey().SerializeCompressed())
	src := CreateDataHash([]byte("state"))
	stateID, err := api.CreateStateID(pred, src)
	require.NoError(t, err)
	txHash := CreateDataHash([]byte("tx"))
	sig, err := NewSigningService().SignDataHash(api.SigDataHash(src, txHash.Imprint()), priv.Serialize())
	require.NoError(t, err)

	mk := func(w []byte) *models.CertificationRequest {
		return &models.CertificationRequest{StateID: stateID, CertificationData: models.CertificationData{
			OwnerPredicate: pred, SourceStateHash: src, TransactionHash: txHash, Witness: api.HexBytes(w),
		}}
	}
	for _, flip := range []bool{true, false} {
		res := validator.Validate(mk(highSTwin(sig, flip)))
		require.Equal(t, ValidationStatusSignatureVerificationFailed, res.Status)
	}
	require.Equal(t, ValidationStatusSuccess, validator.Validate(mk(sig)).Status)
}
