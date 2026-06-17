package round

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type preCollectionResult struct {
	snapshot      smtbackend.Snapshot
	commitments   []*models.CertificationRequest
	leaves        []smtbackend.LeafInput
	recordsStaged bool
	blockNumber   *api.BigInt
	proposalID    string
}

type advanceRequest struct {
	resultCh chan advanceResponse
}

type advanceResponse struct {
	result *preCollectionResult
	err    error
}

type precollectorPrepareStats struct {
	flushCalls int
	flushAdded int
	flushTotal time.Duration
	flushMax   time.Duration
	stageCalls int
	stageAdded int
	stageTotal time.Duration
	stageMax   time.Duration
	fork       time.Duration
	total      time.Duration
}

type precollectorPrepareOutcome struct {
	result       *preCollectionResult
	nextSnapshot smtbackend.Snapshot
	stats        precollectorPrepareStats
	err          error
}

type childPrecollector struct {
	commitmentStream <-chan *models.CertificationRequest
	commitmentQueue  interfaces.CommitmentQueue
	logger           *logger.Logger
	maxPerRound      int
	markProofPending func([]*models.CertificationRequest)
	blockNumber      *api.BigInt
	proposalID       string
	stageProposed    func(context.Context, *api.BigInt, string, int, []*models.CertificationRequest) error

	advanceCh chan advanceRequest
	stopCh    chan struct{}
	doneCh    chan struct{}
	stopErrMu sync.Mutex
	stopErr   error
}

func newChildPrecollector(
	stream <-chan *models.CertificationRequest,
	queue interfaces.CommitmentQueue,
	log *logger.Logger,
	maxPerRound int,
	markProofPending func([]*models.CertificationRequest),
	blockNumber *api.BigInt,
	proposalID string,
	stageProposed func(context.Context, *api.BigInt, string, int, []*models.CertificationRequest) error,
) *childPrecollector {
	if maxPerRound <= 0 {
		maxPerRound = 10000
	}
	if proposalID == "" {
		proposalID = uuid.NewString()
	}
	return &childPrecollector{
		commitmentStream: stream,
		commitmentQueue:  queue,
		logger:           log,
		maxPerRound:      maxPerRound,
		markProofPending: markProofPending,
		blockNumber:      cloneBigInt(blockNumber),
		proposalID:       proposalID,
		stageProposed:    stageProposed,
		advanceCh:        make(chan advanceRequest),
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
	}
}

// Start launches the precollector on a snapshot it owns and discards on exit.
// The caller forks the base snapshot under roundMutex and passes the fork here,
// so the base snapshot can never be discarded while it is being forked.
func (cp *childPrecollector) Start(ctx context.Context, snapshot smtbackend.Snapshot) {
	go cp.run(ctx, snapshot)
}

// AdvanceRound returns the current round's collected data and internally chains
// a new collection from the current snapshot before returning.
func (cp *childPrecollector) AdvanceRound() (*preCollectionResult, error) {
	req := advanceRequest{resultCh: make(chan advanceResponse, 1)}
	select {
	case cp.advanceCh <- req:
	case <-cp.doneCh:
		return nil, cp.stoppedError()
	}
	select {
	case resp := <-req.resultCh:
		return resp.result, resp.err
	case <-cp.doneCh:
		select {
		case resp := <-req.resultCh:
			return resp.result, resp.err
		default:
		}
		return nil, cp.stoppedError()
	}
}

func (cp *childPrecollector) Stop() {
	select {
	case <-cp.doneCh:
		return // already stopped
	default:
	}
	close(cp.stopCh)
	<-cp.doneCh
}

func (cp *childPrecollector) run(ctx context.Context, snapshot smtbackend.Snapshot) {
	defer close(cp.doneCh)

	commitments := make([]*models.CertificationRequest, 0)

	defer func() {
		if snapshot != nil {
			snapshot.Discard(ctx)
		}
	}()

	prepareRound := func(
		snapshot smtbackend.Snapshot,
		blockNumber *api.BigInt,
		proposalID string,
		rawCommitments []*models.CertificationRequest,
	) precollectorPrepareOutcome {
		prepareStart := time.Now()
		outcome := precollectorPrepareOutcome{}

		var added []*models.CertificationRequest
		var addedLeaves []smtbackend.LeafInput
		if len(rawCommitments) > 0 {
			start := time.Now()
			var err error
			added, addedLeaves, err = cp.addBatch(ctx, snapshot, rawCommitments)
			elapsed := time.Since(start)
			outcome.stats.flushCalls = 1
			outcome.stats.flushAdded = len(added)
			outcome.stats.flushTotal = elapsed
			outcome.stats.flushMax = elapsed
			if err != nil {
				outcome.err = err
				outcome.stats.total = time.Since(prepareStart)
				return outcome
			}
		}

		stageDone := make(chan error, 1)
		if len(added) > 0 && cp.stageProposed != nil && blockNumber != nil {
			outcome.stats.stageCalls = 1
			outcome.stats.stageAdded = len(added)
			go func() {
				stageStart := time.Now()
				err := cp.stageProposed(ctx, blockNumber, proposalID, 0, added)
				stageElapsed := time.Since(stageStart)
				outcome.stats.stageTotal = stageElapsed
				outcome.stats.stageMax = stageElapsed
				stageDone <- err
			}()
		} else {
			stageDone <- nil
		}

		if cp.markProofPending != nil {
			cp.markProofPending(added)
		}

		forkStart := time.Now()
		nextSnapshot, forkErr := snapshot.Fork(ctx)
		outcome.stats.fork = time.Since(forkStart)
		stageErr := <-stageDone
		if stageErr != nil {
			if nextSnapshot != nil {
				nextSnapshot.Discard(ctx)
			}
			outcome.err = stageErr
			outcome.stats.total = time.Since(prepareStart)
			return outcome
		}
		if forkErr != nil {
			outcome.err = fmt.Errorf("failed to fork next precollector snapshot: %w", forkErr)
			outcome.stats.total = time.Since(prepareStart)
			return outcome
		}

		outcome.result = &preCollectionResult{
			snapshot:      snapshot,
			commitments:   added,
			leaves:        addedLeaves,
			recordsStaged: len(added) > 0 && cp.stageProposed != nil && blockNumber != nil,
			blockNumber:   cloneBigInt(blockNumber),
			proposalID:    proposalID,
		}
		outcome.nextSnapshot = nextSnapshot
		outcome.stats.total = time.Since(prepareStart)
		return outcome
	}

	prepareSynchronously := func() (precollectorPrepareOutcome, error) {
		outcome := prepareRound(snapshot, cloneBigInt(cp.blockNumber), cp.proposalID, commitments)
		if outcome.err != nil {
			return outcome, outcome.err
		}
		cp.logger.WithContext(ctx).Debug("Precollector prepared",
			"commitments", len(outcome.result.commitments),
			"leaves", len(outcome.result.leaves),
			"maxPerRound", cp.maxPerRound,
			"flushCalls", outcome.stats.flushCalls,
			"flushAdded", outcome.stats.flushAdded,
			"flushTotal", outcome.stats.flushTotal.String(),
			"flushMax", outcome.stats.flushMax.String(),
			"stageCalls", outcome.stats.stageCalls,
			"stageAdded", outcome.stats.stageAdded,
			"stageTotal", outcome.stats.stageTotal.String(),
			"stageMax", outcome.stats.stageMax.String(),
			"fork", outcome.stats.fork.String(),
			"total", outcome.stats.total.String())
		return outcome, nil
	}

	// streamCh is nil when we've hit maxPerRound so we stop reading
	streamCh := cp.commitmentStream

	for {
		activeCount := len(commitments)
		if activeCount >= cp.maxPerRound {
			streamCh = nil
		} else {
			streamCh = cp.commitmentStream
		}

		select {
		case commitment := <-streamCh:
			commitments = append(commitments, commitment)

		case req := <-cp.advanceCh:
			advanceStart := time.Now()
			pendingAtAdvance := len(commitments)
			outcome, err := prepareSynchronously()
			if err != nil {
				cp.setStopErr(err)
				req.resultCh <- advanceResponse{err: err}
				return
			}
			result := outcome.result
			cp.logger.WithContext(ctx).Debug("Precollector advanced",
				"commitments", len(result.commitments),
				"leaves", len(result.leaves),
				"pendingAtAdvance", pendingAtAdvance,
				"maxPerRound", cp.maxPerRound,
				"advanceFlush", outcome.stats.flushTotal.String(),
				"total", time.Since(advanceStart).String(),
				"flushCalls", outcome.stats.flushCalls,
				"flushAdded", outcome.stats.flushAdded,
				"flushTotal", outcome.stats.flushTotal.String(),
				"flushMax", outcome.stats.flushMax.String(),
				"stageCalls", outcome.stats.stageCalls,
				"stageAdded", outcome.stats.stageAdded,
				"stageTotal", outcome.stats.stageTotal.String(),
				"stageMax", outcome.stats.stageMax.String(),
				"fork", outcome.stats.fork.String())
			snapshot = outcome.nextSnapshot
			cp.blockNumber = incrementBigInt(cp.blockNumber)
			cp.proposalID = uuid.NewString()
			commitments = make([]*models.CertificationRequest, 0)
			req.resultCh <- advanceResponse{result: result}

		case <-cp.stopCh:
			return

		case <-ctx.Done():
			return
		}
	}
}

func cloneBigInt(v *api.BigInt) *api.BigInt {
	if v == nil || v.Int == nil {
		return nil
	}
	return api.NewBigInt(new(big.Int).Set(v.Int))
}

func incrementBigInt(v *api.BigInt) *api.BigInt {
	if v == nil || v.Int == nil {
		return nil
	}
	return api.NewBigInt(new(big.Int).Add(v.Int, big.NewInt(1)))
}

func (cp *childPrecollector) addBatch(
	ctx context.Context,
	snapshot smtbackend.Snapshot,
	commitments []*models.CertificationRequest,
) ([]*models.CertificationRequest, []smtbackend.LeafInput, error) {
	if len(commitments) == 0 {
		return nil, nil, nil
	}

	leavesToAdd := make([]smtbackend.LeafInput, 0, len(commitments))
	valid := make([]*models.CertificationRequest, 0, len(commitments))

	for _, c := range commitments {
		leaf, err := commitmentLeafInput(c)
		if err != nil {
			cp.logger.WithContext(ctx).Error("Failed to create leaf input",
				"stateID", c.StateID.String(), "error", err.Error())
			continue
		}
		leavesToAdd = append(leavesToAdd, leaf)
		valid = append(valid, c)
	}

	if len(leavesToAdd) == 0 {
		return nil, nil, nil
	}

	added, addedLeaves, dropped, err := addCommitmentLeaves(ctx, cp.logger, snapshot, leavesToAdd, valid)
	if err != nil {
		cp.logger.WithContext(ctx).Error("Failed to add precollector leaves", "error", err.Error())
		return nil, nil, err
	}
	ackDroppedCommitments(ctx, cp.logger, cp.commitmentQueue, dropped)
	return added, addedLeaves, nil
}

func (cp *childPrecollector) setStopErr(err error) {
	if err == nil {
		return
	}
	cp.stopErrMu.Lock()
	if cp.stopErr == nil {
		cp.stopErr = err
	}
	cp.stopErrMu.Unlock()
}

func (cp *childPrecollector) stoppedError() error {
	cp.stopErrMu.Lock()
	defer cp.stopErrMu.Unlock()
	if cp.stopErr != nil {
		return fmt.Errorf("precollector stopped: %w", cp.stopErr)
	}
	return fmt.Errorf("precollector stopped")
}
