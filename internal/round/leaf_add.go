package round

import (
	"context"
	"errors"
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/metrics"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

// ErrRequestExpired reports a request whose timeout the round's reference time
// has already reached. The request can never be inserted in this or any later
// round, so it is acked out of the queue rather than retried.
var ErrRequestExpired = errors.New("certification request expired")

// commitmentExpired reports whether the request may still be inserted in a
// round with this reference time. The timeout is exclusive.
func commitmentExpired(commitment *models.CertificationRequest, referenceTime uint64) bool {
	timeout := commitment.EffectiveTimeout
	if timeout == 0 {
		timeout = commitment.CertificationData.Timeout
	}
	return timeout != 0 && referenceTime >= timeout
}

// commitmentLeafInput materialises a commitment's SMT leaf under the round's
// pinned reference time, recording that time on the commitment so the record
// and the served proof report the value the leaf was actually built from.
func commitmentLeafInput(commitment *models.CertificationRequest, referenceTime uint64) (smtbackend.LeafInput, error) {
	if commitmentExpired(commitment, referenceTime) {
		return smtbackend.LeafInput{}, ErrRequestExpired
	}
	key, err := commitment.StateID.GetTreeKey()
	if err != nil {
		return smtbackend.LeafInput{}, err
	}
	leafValue, err := commitment.LeafValue(referenceTime)
	if err != nil {
		return smtbackend.LeafInput{}, err
	}
	commitment.ReferenceTime = referenceTime
	return smtbackend.LeafInput{
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), leafValue...),
	}, nil
}

func addCommitmentLeaves(
	ctx context.Context,
	log *logger.Logger,
	snapshot smtbackend.Snapshot,
	leaves []smtbackend.LeafInput,
	commitments []*models.CertificationRequest,
) ([]*models.CertificationRequest, []smtbackend.LeafInput, []interfaces.CertificationRequestAck, error) {
	result, err := snapshot.AddLeavesClassified(ctx, leaves)
	if err != nil {
		return nil, nil, nil, err
	}
	metrics.SMTBatchMaterializedNodes.Observe(float64(result.Stats.MaterializedNodes))
	metrics.SMTBatchNodeReads.Observe(float64(result.Stats.NodeReads))
	metrics.SMTBatchOverlayEntries.Observe(float64(result.Stats.OverlayEntries))
	metrics.SMTBatchOverlayBytes.Observe(float64(result.Stats.OverlayBytes))

	addedCommitments := make([]*models.CertificationRequest, 0, len(result.AcceptedIndexes))
	addedLeaves := make([]smtbackend.LeafInput, 0, len(result.AcceptedIndexes))
	for _, idx := range result.AcceptedIndexes {
		if idx < 0 || idx >= len(commitments) || idx >= len(leaves) {
			return nil, nil, nil, fmt.Errorf("SMT backend returned invalid accepted leaf index %d", idx)
		}
		addedCommitments = append(addedCommitments, commitments[idx])
		addedLeaves = append(addedLeaves, leaves[idx])
	}

	dropped := make([]interfaces.CertificationRequestAck, 0, len(result.DuplicateIndexes)+len(result.Rejected))
	for _, idx := range result.DuplicateIndexes {
		if idx < 0 || idx >= len(commitments) {
			return nil, nil, nil, fmt.Errorf("SMT backend returned invalid duplicate leaf index %d", idx)
		}
		dropped = append(dropped, interfaces.CertificationRequestAck{
			StateID:  commitments[idx].StateID,
			StreamID: commitments[idx].StreamID,
		})
	}
	for _, rejected := range result.Rejected {
		errText := "<nil>"
		if rejected.Err != nil {
			errText = rejected.Err.Error()
		}
		if rejected.Index < 0 || rejected.Index >= len(commitments) {
			return nil, nil, nil, fmt.Errorf("SMT backend returned invalid rejected leaf index %d", rejected.Index)
		}
		log.WithContext(ctx).Warn("Rejected commitment leaf",
			"stateID", commitments[rejected.Index].StateID.String(),
			"reason", string(rejected.Reason),
			"error", errText)
		dropped = append(dropped, interfaces.CertificationRequestAck{
			StateID:  commitments[rejected.Index].StateID,
			StreamID: commitments[rejected.Index].StreamID,
		})
	}

	return addedCommitments, addedLeaves, dropped, nil
}

func ackDroppedCommitments(ctx context.Context, log *logger.Logger, queue interfaces.CommitmentQueue, dropped []interfaces.CertificationRequestAck) {
	if len(dropped) == 0 || queue == nil {
		return
	}
	if err := queue.MarkProcessed(ctx, dropped); err != nil {
		log.WithContext(ctx).Error("Failed to mark dropped commitments as processed",
			"count", len(dropped),
			"error", err.Error())
	}
}
