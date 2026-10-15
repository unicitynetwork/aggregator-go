package bft

import (
	"context"
	"crypto"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"

	"github.com/unicitynetwork/bft-go-base/types"
)

// ucAuthenticator is the single boundary where unicity certificates received from
// the root chain become trusted. It binds what native UC.Verify does not:
//
//   - the configured network (the seal and the resolved trust base);
//   - the EXACT root epoch of the seal (the resolved trust base must be of the epoch the
//     seal names: a store/REST answer for another epoch is refused);
//   - a seal round at or after the start of that epoch;
//
// before the quorum signatures count, and rejects malformed nested shapes (null
// partition path elements) before any native path evaluation. Successful complete-seal
// authentications are memoized per (seal bytes, authority content) so the UC itself and
// its partition view share one quorum check.
type ucAuthenticator struct {
	network types.NetworkID
	store   TrustBaseStore

	mu    sync.Mutex
	known map[[sha256.Size]byte]struct{}
}

const maxAuthenticatedSeals = 256

func newUCAuthenticator(network types.NetworkID, store TrustBaseStore) *ucAuthenticator {
	return &ucAuthenticator{network: network, store: store, known: map[[sha256.Size]byte]struct{}{}}
}

// trustBase resolves the trust base of exactly the requested epoch of the configured network.
func (a *ucAuthenticator) trustBase(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	tb, err := a.store.GetByEpoch(ctx, epoch)
	if err != nil {
		return nil, fmt.Errorf("failed to load trust base for epoch %d: %w", epoch, err)
	}
	if tb == nil {
		return nil, fmt.Errorf("no trust base for epoch %d", epoch)
	}
	if tb.GetEpoch() != epoch {
		return nil, fmt.Errorf("trust base of epoch %d returned for requested epoch %d", tb.GetEpoch(), epoch)
	}
	if tb.GetNetworkID() != a.network {
		return nil, fmt.Errorf("trust base of epoch %d is of network %d, configured network is %d", epoch, tb.GetNetworkID(), a.network)
	}
	return tb, nil
}

// validateUCShape rejects certificates with missing parts or null path elements, which the
// native verification would dereference.
func validateUCShape(uc *types.UnicityCertificate) error {
	switch {
	case uc == nil:
		return types.ErrUnicityCertificateIsNil
	case uc.InputRecord == nil:
		return types.ErrInputRecordIsNil
	case uc.UnicitySeal == nil:
		return types.ErrUnicitySealIsNil
	case uc.UnicityTreeCertificate == nil:
		return types.ErrUnicityTreeCertificateIsNil
	}
	for i, step := range uc.UnicityTreeCertificate.HashSteps {
		if step == nil {
			return fmt.Errorf("unicity tree certificate hash step %d is null", i)
		}
	}
	return nil
}

// verifyUC authenticates the certificate of the shard.
func (a *ucAuthenticator) verifyUC(ctx context.Context, uc *types.UnicityCertificate, partition types.PartitionID, shard types.ShardID) error {
	if err := validateUCShape(uc); err != nil {
		return fmt.Errorf("malformed unicity certificate: %w", err)
	}
	if uc.UnicitySeal.NetworkID != a.network {
		return fmt.Errorf("unicity seal is of network %d, configured network is %d", uc.UnicitySeal.NetworkID, a.network)
	}
	if err := uc.IsValid(partition, shard, nil); err != nil {
		return fmt.Errorf("invalid unicity certificate: %w", err)
	}
	shardRoot, err := uc.ShardTreeCertificate.ComputeCertificateHash(uc.InputRecord, uc.TRHash, uc.ShardConfHash, crypto.SHA256)
	if err != nil {
		return err
	}
	root, err := uc.UnicityTreeCertificate.EvalAuthPath(shardRoot, crypto.SHA256)
	if err != nil {
		return fmt.Errorf("evaluating unicity tree certificate: %w", err)
	}
	if string(root) != string(uc.UnicitySeal.Hash) {
		return fmt.Errorf("unicity seal hash %X does not match with the root hash of the unicity tree %X", uc.UnicitySeal.Hash, root)
	}
	return a.verifySeal(ctx, uc.UnicitySeal)
}

// AdmittedTrustBases is the optional lookup of trust bases already admitted to the local
// history. It must never fetch: an unknown epoch is (nil, false, nil).
type AdmittedTrustBases interface {
	GetAdmitted(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, bool, error)
}

// nextEpochStart returns the start round of the epoch following "epoch" if that trust base
// is already admitted locally. Retired authority may not certify at or after it. An unknown
// future boundary is not an error (nothing proves freshness without it, as for ordinary UCs).
func (a *ucAuthenticator) nextEpochStart(ctx context.Context, epoch uint64) (uint64, bool, error) {
	var next *types.RootTrustBaseV1
	if h, ok := a.store.(AdmittedTrustBases); ok {
		tb, known, err := h.GetAdmitted(ctx, epoch+1)
		if err != nil {
			return 0, false, err
		}
		if !known {
			return 0, false, nil
		}
		next = tb
	} else {
		tb, err := a.store.GetByEpoch(ctx, epoch+1)
		if err != nil {
			if errors.Is(err, interfaces.ErrTrustBaseNotFound) {
				return 0, false, nil
			}
			return 0, false, fmt.Errorf("failed to load trust base for epoch %d: %w", epoch+1, err)
		}
		next = tb
	}
	if next == nil {
		return 0, false, nil
	}
	if next.GetEpoch() != epoch+1 || next.GetNetworkID() != a.network {
		return 0, false, fmt.Errorf("history entry for epoch %d is of epoch %d, network %d", epoch+1, next.GetEpoch(), next.GetNetworkID())
	}
	return next.GetEpochStart(), true, nil
}

// verifySeal authenticates the complete seal under the trust base of its own epoch, inside
// the interval of that epoch: the known end boundary is checked on EVERY call, before any
// memoized verdict is used, so history learned later invalidates cached seals.
func (a *ucAuthenticator) verifySeal(ctx context.Context, seal *types.UnicitySeal) error {
	if seal == nil {
		return types.ErrUnicitySealIsNil
	}
	tb, err := a.trustBase(ctx, seal.Epoch)
	if err != nil {
		return err
	}
	next, known, err := a.nextEpochStart(ctx, seal.Epoch)
	if err != nil {
		return err
	}
	if known && seal.RootChainRoundNumber >= next {
		return fmt.Errorf("seal round %d is at or after the start of the next epoch %d (round %d): epoch %d authority has retired",
			seal.RootChainRoundNumber, seal.Epoch+1, next, seal.Epoch)
	}
	tbHash, err := tb.Hash(crypto.SHA256)
	if err != nil {
		return fmt.Errorf("hashing trust base: %w", err)
	}
	sealBytes, err := seal.MarshalCBOR()
	if err != nil {
		return fmt.Errorf("encoding unicity seal: %w", err)
	}
	h := sha256.New()
	h.Write(tbHash)
	h.Write(sealBytes)
	var key [sha256.Size]byte
	copy(key[:], h.Sum(nil))
	a.mu.Lock()
	_, ok := a.known[key]
	a.mu.Unlock()
	if ok {
		return nil
	}
	// network, exact epoch, round >= epoch start, quorum
	if err := types.TrustBaseSealVerifier(tb).VerifySeal(seal); err != nil {
		return err
	}
	a.mu.Lock()
	if len(a.known) >= maxAuthenticatedSeals {
		clear(a.known)
	}
	a.known[key] = struct{}{}
	a.mu.Unlock()
	return nil
}

// sealVerifier is the types.SealVerifier of the authenticator bound to a request context.
func (a *ucAuthenticator) sealVerifier(ctx context.Context) types.SealVerifier {
	return sealVerifierFunc(func(seal *types.UnicitySeal) error { return a.verifySeal(ctx, seal) })
}

type sealVerifierFunc func(*types.UnicitySeal) error

func (f sealVerifierFunc) VerifySeal(s *types.UnicitySeal) error { return f(s) }

var errNoAuthenticator = errors.New("unicity certificate authenticator is not configured")
