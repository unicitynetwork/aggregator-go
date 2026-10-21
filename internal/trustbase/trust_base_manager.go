package trustbase

import (
	"context"
	"errors"
	"fmt"

	"github.com/unicitynetwork/bft-go-base/types"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

// maxCatchUpEpochs bounds store probes and REST fetches per GetByEpoch call.
const maxCatchUpEpochs = 16

// TrustBaseManager manages trust base synchronization between BFT node and local state
type TrustBaseManager struct {
	log                *logger.Logger
	trustBaseStore     ManagerStore
	bftRestClient      BFTRestClient
	trustBaseValidator TrustBaseVerifier
}

type (
	TrustBaseStore interface {
		GetByEpoch(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error)
		Store(ctx context.Context, trustBase types.RootTrustBase) error
	}
	// ManagerStore is the store the manager needs: the validator's view plus the admitted head.
	ManagerStore interface {
		TrustBaseStore
		GetLatest(ctx context.Context) (*types.RootTrustBaseV1, error)
	}
	BFTRestClient interface {
		GetTrustBases(ctx context.Context, epoch1, epoch2 uint64) ([]*types.RootTrustBaseV1, error)
	}
	TrustBaseVerifier interface {
		Verify(ctx context.Context, trustBase types.RootTrustBase) error
	}
)

func NewTrustBaseManager(log *logger.Logger, trustBaseStore ManagerStore, bftRestClient BFTRestClient, trustBaseVerifier TrustBaseVerifier) *TrustBaseManager {
	return &TrustBaseManager{
		log:                log,
		trustBaseStore:     trustBaseStore,
		bftRestClient:      bftRestClient,
		trustBaseValidator: trustBaseVerifier,
	}
}

// GetAdmitted returns the trust base of the epoch only if it is already admitted to the
// local store. It never asks the BFT node and never stores anything: it is the lookup for
// history the aggregator already knows (for example the start of the next epoch, which
// ends the interval an epoch's keys may certify in).
func (s *TrustBaseManager) GetAdmitted(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, bool, error) {
	tb, err := s.trustBaseStore.GetByEpoch(ctx, epoch)
	if err != nil {
		if errors.Is(err, interfaces.ErrTrustBaseNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to load admitted trust base for epoch %d: %w", epoch, err)
	}
	return tb, tb != nil, nil
}

// GetByEpoch returns trust base from (in order of precedence):
// cache -> storage -> BFT node -> error if not found.
//
// The BFT REST endpoint is NOT a trust anchor. The installed epoch-1 trust base (from configuration)
// is the only anchor: a trust base fetched from the BFT node is admitted only if it has exactly the
// requested epoch, the anchor's network, is epoch > 1, and extends the already installed predecessor
// (hash link + predecessor quorum signatures). Replacement genesis (epoch 1) entries are never accepted
// from the network. The value returned is always the one held in storage, never the received object.
func (s *TrustBaseManager) GetByEpoch(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	// attempt to load from local store (cache + disk via decorator chain)
	tbFromCache, err := s.trustBaseStore.GetByEpoch(ctx, epoch)
	if err != nil && !errors.Is(err, interfaces.ErrTrustBaseNotFound) {
		return nil, fmt.Errorf("failed to load trust base for epoch %d: %w", epoch, err)
	}
	if tbFromCache != nil {
		return tbFromCache, nil
	}

	// epoch 1 is the configured anchor and cannot be obtained from the network
	if epoch <= 1 {
		return nil, fmt.Errorf("trust base anchor (epoch 1) is not installed and cannot be fetched from BFT node, requested epoch %d", epoch)
	}

	// the installed anchor fixes the expected network
	anchor, err := s.trustBaseStore.GetByEpoch(ctx, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to load installed trust base anchor (epoch 1): %w", err)
	}
	if anchor == nil {
		return nil, errors.New("installed trust base anchor (epoch 1) not found")
	}

	// Resolve history iteratively from the installed chain head. The requested epoch may come from an
	// unauthenticated message, so work is bounded: at most maxCatchUpEpochs store probes below the
	// requested epoch and at most maxCatchUpEpochs REST fetches, stopping at the first failure.
	head := uint64(0)
	for e := epoch - 1; e >= 1 && epoch-e <= maxCatchUpEpochs; e-- {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		tb, getErr := s.trustBaseStore.GetByEpoch(ctx, e)
		if getErr != nil && !errors.Is(getErr, interfaces.ErrTrustBaseNotFound) {
			return nil, fmt.Errorf("failed to load trust base for epoch %d: %w", e, getErr)
		}
		if tb != nil {
			head = e
			break
		}
	}
	if head == 0 {
		return nil, fmt.Errorf("trust base epoch %d is more than %d epochs beyond the installed chain", epoch, maxCatchUpEpochs)
	}
	var tb *types.RootTrustBaseV1
	for e := head + 1; e <= epoch; e++ {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if tb, err = s.fetchAndAdmit(ctx, e, anchor); err != nil {
			return nil, fmt.Errorf("cannot extend trust base chain to epoch %d: %w", epoch, err)
		}
	}
	return tb, nil
}

// fetchAndAdmit fetches exactly one epoch from the BFT node, verifies it against the installed
// predecessor and stores it. It returns the stored admitted entry.
func (s *TrustBaseManager) fetchAndAdmit(ctx context.Context, epoch uint64, anchor *types.RootTrustBaseV1) (*types.RootTrustBaseV1, error) {
	// not found locally: query BFT node
	trustBases, err := s.bftRestClient.GetTrustBases(ctx, epoch, epoch)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch trust base from BFT node for epoch %d: %w", epoch, err)
	}

	// validate trust base received from BFT node
	if len(trustBases) != 1 || trustBases[0] == nil {
		return nil, fmt.Errorf("trust base for epoch %d not found for BFT node, len(trustBases)=%d", epoch, len(trustBases))
	}
	tb := trustBases[0]
	if tb.GetEpoch() != epoch {
		return nil, fmt.Errorf("BFT node returned trust base for epoch %d, requested epoch %d", tb.GetEpoch(), epoch)
	}
	if tb.GetNetworkID() != anchor.GetNetworkID() {
		return nil, fmt.Errorf("BFT node returned trust base for network %d, expected network %d", tb.GetNetworkID(), anchor.GetNetworkID())
	}
	if err = s.trustBaseValidator.Verify(ctx, tb); err != nil {
		return nil, fmt.Errorf("failed to verify trust base received from BFT node for epoch %d: %w", epoch, err)
	}

	// store the received trust base
	if err = s.trustBaseStore.Store(ctx, tb); err != nil {
		if !errors.Is(err, interfaces.ErrTrustBaseAlreadyExists) {
			return nil, fmt.Errorf("failed to store trust base for epoch %d: %w", epoch, err)
		}
		s.log.WithContext(ctx).Warn("Trust base already stored, not overwriting it", "epoch", epoch)
		// return the admitted stored entry, not the received object
		stored, getErr := s.trustBaseStore.GetByEpoch(ctx, epoch)
		if getErr != nil {
			return nil, fmt.Errorf("failed to load stored trust base for epoch %d: %w", epoch, getErr)
		}
		if stored == nil {
			return nil, fmt.Errorf("stored trust base for epoch %d not found after conflict", epoch)
		}
		return stored, nil
	}
	s.log.WithContext(ctx).Info("Updated trust base storage with new trust base from BFT node", "epoch", epoch)
	return tb, nil
}

// ErrNextEpochUnavailable is returned by AdmitNext when the BFT node has no usable trust base for the
// epoch after the admitted head.
var ErrNextEpochUnavailable = errors.New("the next trust base epoch is not available")

// GetByEpochLocal returns the trust base of the epoch from the local store only (interfaces.
// ErrTrustBaseNotFound if it is not admitted): it never queries the BFT node, so it is safe to call
// for every received message.
func (s *TrustBaseManager) GetByEpochLocal(ctx context.Context, epoch uint64) (*types.RootTrustBaseV1, error) {
	tb, known, err := s.GetAdmitted(ctx, epoch)
	if err != nil {
		return nil, err
	}
	if !known {
		return nil, interfaces.ErrTrustBaseNotFound
	}
	return tb, nil
}

// Head returns the highest admitted epoch. Admission only ever extends the history by one verified
// successor, so the stored epochs below the head are contiguous.
func (s *TrustBaseManager) Head(ctx context.Context) (uint64, error) {
	tb, err := s.trustBaseStore.GetLatest(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to load the latest trust base: %w", err)
	}
	return tb.GetEpoch(), nil
}

// AdmitNext asks the BFT node for exactly the epoch after the admitted head, verifies it against the
// installed anchor's network and its predecessor (hash link and predecessor quorum) and stores it. It
// never follows an epoch number that came from a received message and never skips a predecessor. The
// admitted epoch is returned.
func (s *TrustBaseManager) AdmitNext(ctx context.Context) (uint64, error) {
	head, err := s.Head(ctx)
	if err != nil {
		return 0, err
	}
	anchor, err := s.trustBaseStore.GetByEpoch(ctx, 1)
	if err != nil {
		return 0, fmt.Errorf("failed to load installed trust base anchor (epoch 1): %w", err)
	}
	if _, err := s.fetchAndAdmit(ctx, head+1, anchor); err != nil {
		return 0, fmt.Errorf("%w: %w", ErrNextEpochUnavailable, err)
	}
	return head + 1, nil
}
