package round

import (
	"context"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

func addCommitmentLeaves(
	ctx context.Context,
	log *logger.Logger,
	snapshot *smt.ThreadSafeSmtSnapshot,
	leaves []*smt.Leaf,
	commitments []*models.CertificationRequest,
) ([]*models.CertificationRequest, []*smt.Leaf, []interfaces.CertificationRequestAck) {
	result := snapshot.AddLeavesClassified(leaves)

	addedCommitments := make([]*models.CertificationRequest, 0, len(result.AddedIndexes))
	addedLeaves := make([]*smt.Leaf, 0, len(result.AddedIndexes))
	for _, idx := range result.AddedIndexes {
		addedCommitments = append(addedCommitments, commitments[idx])
		addedLeaves = append(addedLeaves, leaves[idx])
	}

	dropped := make([]interfaces.CertificationRequestAck, 0, len(result.DuplicateIndexes)+len(result.Rejected))
	for _, idx := range result.DuplicateIndexes {
		dropped = append(dropped, interfaces.CertificationRequestAck{
			StateID:  commitments[idx].StateID,
			StreamID: commitments[idx].StreamID,
		})
	}
	for _, rejected := range result.Rejected {
		log.WithContext(ctx).Warn("Rejected commitment leaf",
			"path", leaves[rejected.Index].Path.String(),
			"error", rejected.Err.Error())
		dropped = append(dropped, interfaces.CertificationRequestAck{
			StateID:  commitments[rejected.Index].StateID,
			StreamID: commitments[rejected.Index].StreamID,
		})
	}

	return addedCommitments, addedLeaves, dropped
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
