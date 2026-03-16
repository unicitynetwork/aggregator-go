package round

import (
	"context"
	"errors"
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

type preCollectionResult struct {
	snapshot    *smt.ThreadSafeSmtSnapshot
	commitments []*models.CertificationRequest
	leaves      []*smt.Leaf
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

	advanceCh chan advanceRequest
	stopCh    chan struct{}
	doneCh    chan struct{}
}

func newChildPrecollector(
	stream <-chan *models.CertificationRequest,
	queue interfaces.CommitmentQueue,
	log *logger.Logger,
	maxPerRound int,
) *childPrecollector {
	if maxPerRound <= 0 {
		maxPerRound = 10000
	}
	return &childPrecollector{
		commitmentStream: stream,
		commitmentQueue:  queue,
		logger:           log,
		maxPerRound:      maxPerRound,
		advanceCh:        make(chan advanceRequest),
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
	}
}

func (cp *childPrecollector) Start(ctx context.Context, baseSnapshot *smt.ThreadSafeSmtSnapshot) {
	go cp.run(ctx, baseSnapshot)
}

// AdvanceRound returns the current round's collected data and internally chains
// a new collection from the current snapshot before returning.
func (cp *childPrecollector) AdvanceRound() (*preCollectionResult, error) {
	req := advanceRequest{resultCh: make(chan advanceResponse, 1)}
	select {
	case cp.advanceCh <- req:
	case <-cp.doneCh:
		return nil, fmt.Errorf("precollector stopped")
	}
	select {
	case resp := <-req.resultCh:
		return resp.result, resp.err
	case <-cp.doneCh:
		return nil, fmt.Errorf("precollector stopped")
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

func (cp *childPrecollector) run(ctx context.Context, baseSnapshot *smt.ThreadSafeSmtSnapshot) {
	defer close(cp.doneCh)

	snapshot := baseSnapshot.CreateSnapshot()
	commitments := make([]*models.CertificationRequest, 0)
	leaves := make([]*smt.Leaf, 0)
	pending := make([]*models.CertificationRequest, 0, miniBatchSize)
	count := 0

	flush := func() {
		if len(pending) == 0 {
			return
		}
		added, addedLeaves := cp.addBatch(ctx, snapshot, pending)
		commitments = append(commitments, added...)
		leaves = append(leaves, addedLeaves...)
		count += len(added)
		pending = pending[:0]
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
				flush()
			}

		case req := <-cp.advanceCh:
			flush()
			result := &preCollectionResult{
				snapshot:    snapshot,
				commitments: commitments,
				leaves:      leaves,
			}
			// Chain new collection from current snapshot
			snapshot = snapshot.CreateSnapshot()
			commitments = make([]*models.CertificationRequest, 0)
			leaves = make([]*smt.Leaf, 0)
			count = 0
			req.resultCh <- advanceResponse{result: result}

		case <-cp.stopCh:
			flush()
			return

		case <-ctx.Done():
			flush()
			return
		}
	}
}

func (cp *childPrecollector) addBatch(
	ctx context.Context,
	snapshot *smt.ThreadSafeSmtSnapshot,
	commitments []*models.CertificationRequest,
) ([]*models.CertificationRequest, []*smt.Leaf) {
	if len(commitments) == 0 {
		return nil, nil
	}

	leavesToAdd := make([]*smt.Leaf, 0, len(commitments))
	valid := make([]*models.CertificationRequest, 0, len(commitments))

	for _, c := range commitments {
		path, err := c.StateID.GetPath()
		if err != nil {
			cp.logger.WithContext(ctx).Error("Failed to get path for commitment",
				"stateID", c.StateID.String(), "error", err.Error())
			continue
		}
		leafValue, err := c.LeafValue()
		if err != nil {
			cp.logger.WithContext(ctx).Error("Failed to create leaf value",
				"stateID", c.StateID.String(), "error", err.Error())
			continue
		}
		leavesToAdd = append(leavesToAdd, smt.NewLeaf(path, leafValue))
		valid = append(valid, c)
	}

	if len(leavesToAdd) == 0 {
		return nil, nil
	}

	if _, err := snapshot.AddLeaves(leavesToAdd); err != nil {
		result := tryAddLeavesOneByOne(ctx, cp.logger, cp.commitmentQueue, snapshot, leavesToAdd, valid)
		return result.successCommitments, result.successLeaves
	}

	return valid, leavesToAdd
}

// tryAddLeavesOneByOne adds leaves one-by-one to a snapshot and returns results.
// Package-level function usable by both standalone processMiniBatch and childPrecollector.
func tryAddLeavesOneByOne(
	ctx context.Context,
	log *logger.Logger,
	queue interfaces.CommitmentQueue,
	snapshot *smt.ThreadSafeSmtSnapshot,
	leaves []*smt.Leaf,
	commitments []*models.CertificationRequest,
) leafAddResult {
	result := leafAddResult{
		successLeaves:      make([]*smt.Leaf, 0, len(leaves)),
		successCommitments: make([]*models.CertificationRequest, 0, len(commitments)),
		rejected:           nil,
	}

	for i, leaf := range leaves {
		if err := snapshot.AddLeaf(leaf.Path, leaf.Value); err != nil {
			if errors.Is(err, smt.ErrDuplicateLeaf) {
				result.successLeaves = append(result.successLeaves, leaf)
				result.successCommitments = append(result.successCommitments, commitments[i])
				continue
			}
			log.WithContext(ctx).Warn("Rejected conflicting leaf",
				"path", leaf.Path.String(),
				"error", err.Error())
			result.rejected = append(result.rejected, interfaces.CertificationRequestAck{
				StateID:  commitments[i].StateID,
				StreamID: commitments[i].StreamID,
			})
			continue
		}
		result.successLeaves = append(result.successLeaves, leaf)
		result.successCommitments = append(result.successCommitments, commitments[i])
	}

	if len(result.rejected) > 0 && queue != nil {
		if err := queue.MarkProcessed(ctx, result.rejected); err != nil {
			log.WithContext(ctx).Error("Failed to mark rejected commitments as processed",
				"count", len(result.rejected),
				"error", err.Error())
		}
	}

	return result
}
