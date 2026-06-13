package round

import (
	"context"
	"fmt"
	"sync"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

type preCollectionResult struct {
	snapshot    smtbackend.Snapshot
	commitments []*models.CertificationRequest
	leaves      []smtbackend.LeafInput
}

type advanceRequest struct {
	resultCh chan advanceResponse
}

type advanceResponse struct {
	result *preCollectionResult
	err    error
}

type childPrecollector struct {
	commitmentStream <-chan *models.CertificationRequest
	commitmentQueue  interfaces.CommitmentQueue
	logger           *logger.Logger
	maxPerRound      int
	markProofPending func([]*models.CertificationRequest)

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
) *childPrecollector {
	if maxPerRound <= 0 {
		maxPerRound = 10000
	}
	return &childPrecollector{
		commitmentStream: stream,
		commitmentQueue:  queue,
		logger:           log,
		maxPerRound:      maxPerRound,
		markProofPending: markProofPending,
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

	defer func() {
		if snapshot != nil {
			snapshot.Discard(ctx)
		}
	}()
	commitments := make([]*models.CertificationRequest, 0)
	leaves := make([]smtbackend.LeafInput, 0)
	pending := make([]*models.CertificationRequest, 0, miniBatchSize)
	count := 0

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		added, addedLeaves, err := cp.addBatch(ctx, snapshot, pending)
		if err != nil {
			return err
		}
		if cp.markProofPending != nil {
			cp.markProofPending(added)
		}
		commitments = append(commitments, added...)
		leaves = append(leaves, addedLeaves...)
		count += len(added)
		pending = pending[:0]
		return nil
	}

	// streamCh is nil when we've hit maxPerRound so we stop reading
	streamCh := cp.commitmentStream

	for {
		if count+len(pending) >= cp.maxPerRound {
			streamCh = nil
		} else {
			streamCh = cp.commitmentStream
		}

		select {
		case commitment := <-streamCh:
			pending = append(pending, commitment)
			if len(pending) >= miniBatchSize {
				if err := flush(); err != nil {
					cp.setStopErr(err)
					return
				}
			}

		case req := <-cp.advanceCh:
			if err := drainBufferedCommitments(cp.commitmentStream, cp.maxPerRound, &count, &pending, flush); err != nil {
				cp.setStopErr(err)
				req.resultCh <- advanceResponse{err: err}
				return
			}
			if err := flush(); err != nil {
				cp.setStopErr(err)
				req.resultCh <- advanceResponse{err: err}
				return
			}
			result := &preCollectionResult{
				snapshot:    snapshot,
				commitments: commitments,
				leaves:      leaves,
			}
			// Chain new collection from current snapshot
			nextSnapshot, err := snapshot.Fork(ctx)
			if err != nil {
				err = fmt.Errorf("failed to fork next precollector snapshot: %w", err)
				cp.setStopErr(err)
				req.resultCh <- advanceResponse{err: err}
				return
			}
			snapshot = nextSnapshot
			commitments = make([]*models.CertificationRequest, 0)
			leaves = make([]smtbackend.LeafInput, 0)
			count = 0
			req.resultCh <- advanceResponse{result: result}

		case <-cp.stopCh:
			if err := flush(); err != nil {
				cp.setStopErr(err)
			}
			return

		case <-ctx.Done():
			if err := flush(); err != nil {
				cp.setStopErr(err)
			}
			return
		}
	}
}

// drainBufferedCommitments folds already-buffered commitments into the current
// round before an advance boundary is cut.
func drainBufferedCommitments(
	stream <-chan *models.CertificationRequest,
	maxPerRound int,
	count *int,
	pending *[]*models.CertificationRequest,
	flush func() error,
) error {
	for *count+len(*pending) < maxPerRound {
		select {
		case commitment, ok := <-stream:
			if !ok {
				return nil
			}
			*pending = append(*pending, commitment)
			if len(*pending) >= miniBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		default:
			return nil
		}
	}
	return nil
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
