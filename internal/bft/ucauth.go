package bft

import (
	"context"
	"crypto"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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
	// authorities caches the admitted trust base of an epoch with its content digest. Admitted
	// trust bases are immutable (the store refuses to replace them), so a validated entry never
	// goes stale; only positive results are cached.
	authorities map[uint64]*authorityEntry
	// ends caches KNOWN epoch ends (epoch -> start round of the next epoch). A known boundary
	// never changes either. An unknown boundary is NEVER cached beyond one operation: history
	// may be admitted at any time (also by another node sharing the store), and the retirement
	// check must see it on the next certificate.
	ends map[uint64]uint64

	// counters of the work done, the hardware-independent metrics of the cost model
	authorityLoads atomic.Int64 // trust base store reads
	historyLookups atomic.Int64 // next-epoch history reads
	sealEncodes    atomic.Int64 // complete seal encodings + digests
	quorumChecks   atomic.Int64 // native seal verifications
}

type authorityEntry struct {
	tb     *types.RootTrustBaseV1
	digest [sha256.Size]byte
}

const (
	maxAuthenticatedSeals = 256
	maxCachedEpochs       = 64
)

func newUCAuthenticator(network types.NetworkID, store TrustBaseStore) *ucAuthenticator {
	return &ucAuthenticator{
		network: network, store: store, known: map[[sha256.Size]byte]struct{}{},
		authorities: map[uint64]*authorityEntry{}, ends: map[uint64]uint64{},
	}
}

// authority resolves the trust base of exactly the requested epoch of the configured network,
// from the cache once it has been validated.
func (a *ucAuthenticator) authority(ctx context.Context, epoch uint64) (*authorityEntry, error) {
	a.mu.Lock()
	e := a.authorities[epoch]
	a.mu.Unlock()
	if e != nil {
		return e, nil
	}
	a.authorityLoads.Add(1)
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
	tbHash, err := tb.Hash(crypto.SHA256)
	if err != nil {
		return nil, fmt.Errorf("hashing trust base: %w", err)
	}
	e = &authorityEntry{tb: tb}
	copy(e.digest[:], tbHash)
	a.mu.Lock()
	if len(a.authorities) >= maxCachedEpochs {
		clear(a.authorities)
	}
	a.authorities[epoch] = e
	a.mu.Unlock()
	return e, nil
}

// trustBase resolves the trust base of exactly the requested epoch of the configured network.
func (a *ucAuthenticator) trustBase(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	e, err := a.authority(ctx, epoch)
	if err != nil {
		return nil, err
	}
	return e.tb, nil
}

// authOp is the scope of one intake operation (one received response: its UC and its
// partition view). It holds a snapshot of the authority and history it resolved, so that
// the UC and the view do not resolve them (or encode the same seal) twice. It must not
// outlive the operation: the history snapshot is consistent only for it.
type authOp struct {
	a      *ucAuthenticator
	ctx    context.Context
	epochs map[uint64]*opEpoch
	seals  map[*types.UnicitySeal]error
}

type opEpoch struct {
	auth      *authorityEntry
	err       error
	nextKnown bool
	next      uint64
	histErr   error
}

// begin starts an intake operation.
func (a *ucAuthenticator) begin(ctx context.Context) *authOp {
	return &authOp{a: a, ctx: ctx, epochs: map[uint64]*opEpoch{}, seals: map[*types.UnicitySeal]error{}}
}

// epoch resolves the authority and the known end of the epoch once per operation.
func (o *authOp) epoch(epoch uint64) *opEpoch {
	if e, ok := o.epochs[epoch]; ok {
		return e
	}
	e := &opEpoch{}
	o.epochs[epoch] = e
	e.auth, e.err = o.a.authority(o.ctx, epoch)
	if e.err != nil {
		return e
	}
	e.next, e.nextKnown, e.histErr = o.a.epochEnd(o.ctx, epoch)
	return e
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

// verifyUC authenticates the certificate of the shard in a one-call operation.
func (a *ucAuthenticator) verifyUC(ctx context.Context, uc *types.UnicityCertificate, partition types.PartitionID, shard types.ShardID) error {
	return a.begin(ctx).verifyUC(uc, partition, shard)
}

// verifyUC authenticates the certificate of the shard.
func (o *authOp) verifyUC(uc *types.UnicityCertificate, partition types.PartitionID, shard types.ShardID) error {
	a := o.a
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
	return o.verifySeal(uc.UnicitySeal)
}

// AdmittedTrustBases is the optional lookup of trust bases already admitted to the local
// history. It must never fetch: an unknown epoch is (nil, false, nil).
type AdmittedTrustBases interface {
	GetAdmitted(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, bool, error)
}

// epochEnd returns the start round of the epoch following "epoch" if that trust base
// is already admitted locally. Retired authority may not certify at or after it. An unknown
// future boundary is not an error (nothing proves freshness without it, as for ordinary UCs).
// Known boundaries are cached for good; unknown ones are looked up again by every operation.
func (a *ucAuthenticator) epochEnd(ctx context.Context, epoch uint64) (uint64, bool, error) {
	a.mu.Lock()
	next, ok := a.ends[epoch]
	a.mu.Unlock()
	if ok {
		return next, true, nil
	}
	next, known, err := a.nextEpochStart(ctx, epoch)
	if err != nil || !known {
		return 0, false, err
	}
	a.mu.Lock()
	if len(a.ends) >= maxCachedEpochs {
		clear(a.ends)
	}
	a.ends[epoch] = next
	a.mu.Unlock()
	return next, true, nil
}

func (a *ucAuthenticator) nextEpochStart(ctx context.Context, epoch uint64) (uint64, bool, error) {
	a.historyLookups.Add(1)
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

// verifySeal authenticates the complete seal in a one-call operation.
func (a *ucAuthenticator) verifySeal(ctx context.Context, seal *types.UnicitySeal) error {
	return a.begin(ctx).verifySeal(seal)
}

// verifySeal authenticates the complete seal under the trust base of its own epoch, inside
// the interval of that epoch: the known end boundary is checked on EVERY authentication
// (against the operation's snapshot of the history), before any memoized verdict is used,
// so history learned later retires cached seals. The same seal object is judged once per
// operation.
func (o *authOp) verifySeal(seal *types.UnicitySeal) error {
	if seal == nil {
		return types.ErrUnicitySealIsNil
	}
	if err, ok := o.seals[seal]; ok {
		return err
	}
	err := o.verifySealUncached(seal)
	o.seals[seal] = err
	return err
}

func (o *authOp) verifySealUncached(seal *types.UnicitySeal) error {
	a := o.a
	ep := o.epoch(seal.Epoch)
	if ep.err != nil {
		return ep.err
	}
	if ep.histErr != nil {
		return ep.histErr
	}
	if ep.nextKnown && seal.RootChainRoundNumber >= ep.next {
		return fmt.Errorf("seal round %d is at or after the start of the next epoch %d (round %d): epoch %d authority has retired",
			seal.RootChainRoundNumber, seal.Epoch+1, ep.next, seal.Epoch)
	}
	a.sealEncodes.Add(1)
	sealBytes, err := seal.MarshalCBOR()
	if err != nil {
		return fmt.Errorf("encoding unicity seal: %w", err)
	}
	h := sha256.New()
	h.Write(ep.auth.digest[:])
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
	a.quorumChecks.Add(1)
	if err := types.TrustBaseSealVerifier(ep.auth.tb).VerifySeal(seal); err != nil {
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

// sealVerifier is the types.SealVerifier of a one-call operation bound to a request context.
func (a *ucAuthenticator) sealVerifier(ctx context.Context) types.SealVerifier {
	return a.begin(ctx).sealVerifier()
}

// sealVerifier is the types.SealVerifier of the operation.
func (o *authOp) sealVerifier() types.SealVerifier {
	return sealVerifierFunc(o.verifySeal)
}

type sealVerifierFunc func(*types.UnicitySeal) error

func (f sealVerifierFunc) VerifySeal(s *types.UnicitySeal) error { return f(s) }

var errNoAuthenticator = errors.New("unicity certificate authenticator is not configured")
