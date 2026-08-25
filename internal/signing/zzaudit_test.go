package signing

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/stretchr/testify/require"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
	bfttypes "github.com/unicitynetwork/bft-go-base/types"
)

// AUDIT: byte-level verification of m, sid, lambda and predicate acceptance.
func TestAudit_Bytes(t *testing.T) {
	priv, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	pk := priv.PubKey().SerializeCompressed()

	sth := sha256.Sum256([]byte("sthash"))
	txh := sha256.Sum256([]byte("txhash"))

	// ---- m = H(sthash, txhash)
	m := api.SigDataHash(sth[:], txh[:])
	preM := append([]byte{0x82, 0x58, 0x20}, sth[:]...)
	preM = append(preM, 0x58, 0x20)
	preM = append(preM, txh[:]...)
	wantM := sha256.Sum256(preM)
	fmt.Printf("m preimage      = %x\n", preM)
	fmt.Printf("m               = %x\n", m.RawHash)
	fmt.Printf("m expected      = %x\n", wantM)
	require.Equal(t, wantM[:], m.RawHash)

	// ---- sid = H(pred, sthash)
	pred := api.NewPayToPublicKeyPredicate(pk)
	sid, err := api.CreateStateID(pred, sth[:])
	require.NoError(t, err)
	// expected preimage: 82 d9 98 78 83 01 41 01 58 21 <pk> 58 20 <sth>
	preS := []byte{0x82, 0xd9, 0x98, 0x78, 0x83, 0x01, 0x41, 0x01, 0x58, 0x21}
	preS = append(preS, pk...)
	preS = append(preS, 0x58, 0x20)
	preS = append(preS, sth[:]...)
	wantS := sha256.Sum256(preS)
	fmt.Printf("sid preimage    = %x\n", preS)
	fmt.Printf("sid             = %x\n", sid)
	fmt.Printf("sid expected    = %x\n", wantS)
	require.Equal(t, wantS[:], []byte(sid))

	// ---- lambda(Q, tau) = H(txhash, tau)
	lv := api.LeafValue(txh[:], 1755000000)
	preL := append([]byte{0x82, 0x58, 0x20}, txh[:]...)
	preL = append(preL, 0x1a, 0x68, 0x9b, 0x2c, 0xc0) // uint32 1755000000
	wantL := sha256.Sum256(preL)
	fmt.Printf("leaf preimage   = %x\n", preL)
	fmt.Printf("leaf            = %x\n", lv)
	fmt.Printf("leaf expected   = %x\n", wantL)
	require.Equal(t, wantL[:], lv)
}

func auditCommitment(t *testing.T, pred api.Predicate, priv *btcec.PrivateKey) *models.CertificationRequest {
	t.Helper()
	sth := sha256.Sum256([]byte("sthash"))
	txh := sha256.Sum256([]byte("txhash"))
	sid, err := api.CreateStateID(pred, sth[:])
	require.NoError(t, err)
	sig, err := NewSigningService().SignDataHash(api.SigDataHash(sth[:], txh[:]), priv.Serialize())
	require.NoError(t, err)
	return &models.CertificationRequest{
		StateID: sid,
		CertificationData: models.CertificationData{
			OwnerPredicate:  pred,
			SourceStateHash: sth[:],
			TransactionHash: txh[:],
			Witness:         api.HexBytes(sig),
		},
	}
}

// AUDIT: every non-0x01 predicate type code is rejected outright.
func TestAudit_PredicateTypeCodes(t *testing.T) {
	v := NewCertificationRequestValidator(config.ShardingConfig{Mode: config.ShardingModeStandalone}, bfttypes.ShardID{})
	priv, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	pk := priv.PubKey().SerializeCompressed()

	for code := 0x01; code <= 0x08; code++ {
		pred := api.Predicate{Engine: 1, Code: []byte{byte(code)}, Params: pk}
		res := v.Validate(auditCommitment(t, pred, priv))
		fmt.Printf("code 0x%02x -> status=%d string=%q err=%v\n", code, res.Status, res.Status.String(), res.Error)
	}
	// engine variations
	for _, eng := range []uint{0, 2, 7} {
		pred := api.Predicate{Engine: eng, Code: []byte{1}, Params: pk}
		res := v.Validate(auditCommitment(t, pred, priv))
		fmt.Printf("engine %d -> status=%d string=%q err=%v\n", eng, res.Status, res.Status.String(), res.Error)
	}
	fmt.Printf("InvalidOwnerPredicate iota=%d String()=%q\n",
		ValidationStatusInvalidOwnerPredicate, ValidationStatusInvalidOwnerPredicate.String())
	require.Equal(t, "UNKNOWN", ValidationStatusInvalidOwnerPredicate.String())
}

// AUDIT: does the validator take tau anywhere?
func TestAudit_NoTau(t *testing.T) {
	// compile-time proof that Validate has exactly one parameter and no tau
	var f func(*models.CertificationRequest) ValidationResult = (&CertificationRequestValidator{}).Validate
	_ = f
	fmt.Println("Validate signature: func(*models.CertificationRequest) ValidationResult -- no tau")
}

// AUDIT: shard comparator is MSB-first prefix match on the sid bytes.
func TestAudit_ShardComparator(t *testing.T) {
	// build shard id "0" and "1" (1-bit split)
	id0, id1 := bfttypes.ShardID{}.Split()
	for _, id := range []bfttypes.ShardID{id0, id1} {
		cmp := id.Comparator()
		key0 := make([]byte, 32) // 0x00.. -> top bit 0
		key1 := make([]byte, 32)
		key1[0] = 0x80 // top bit 1
		fmt.Printf("shard %v (len=%d): key 0x00..=%v  key 0x80..=%v\n",
			id.String(), id.Length(), cmp(key0), cmp(key1))
	}
	_ = hex.EncodeToString
}
