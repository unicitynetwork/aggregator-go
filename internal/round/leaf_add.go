package round

import (
	"context"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
)

func commitmentLeafInput(commitment *models.CertificationRequest) (smtbackend.LeafInput, error) {
	key, err := commitment.StateID.GetTreeKey()
	if err != nil {
		return smtbackend.LeafInput{}, err
	}
	leafValue, err := commitment.LeafValue()
	if err != nil {
		return smtbackend.LeafInput{}, err
	}
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

	addedCommitments := make([]*models.CertificationRequest, 0, len(result.AcceptedIndexes))
	addedLeaves := make([]smtbackend.LeafInput, 0, len(result.AcceptedIndexes))
	for _, idx := range result.AcceptedIndexes {
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
		errText := "<nil>"
		if rejected.Err != nil {
			errText = rejected.Err.Error()
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
