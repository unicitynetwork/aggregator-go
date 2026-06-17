package round

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/unicitynetwork/aggregator-go/internal/logger"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
	"github.com/unicitynetwork/aggregator-go/internal/storage/interfaces"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

type RecoveryResult struct {
	Recovered   bool
	BlockNumber *api.BigInt
	StateIDs    []api.StateID
}

// FinalizeCertifiedProposal completes a previously stored proposed block when
// BFT returns the UC after this process lost its in-memory proposedBlock.
func (rm *RoundManager) FinalizeCertifiedProposal(
	ctx context.Context,
	blockNumber *api.BigInt,
	rootHash api.HexBytes,
	unicityCertificate api.HexBytes,
) (bool, error) {
	if rm.smtBackend == nil {
		return false, fmt.Errorf("SMT backend not initialized")
	}

	block, err := getBlockAnyFinalization(ctx, rm.storage, blockNumber)
	if err != nil {
		return false, fmt.Errorf("failed to load durable proposal block %s: %w", blockNumber.String(), err)
	}
	if block == nil {
		return false, nil
	}
	if !bytes.Equal(block.RootHash, rootHash) {
		return false, fmt.Errorf("durable proposal block %s root %s does not match certified root %s",
			block.Index.String(), block.RootHash.String(), rootHash.String())
	}
	if block.Finalized {
		return true, nil
	}
	if block.Status != "" && block.Status != models.FinalityStatusProposed && block.Status != models.FinalityStatusFinalizing {
		return false, nil
	}
	block.UnicityCertificate = unicityCertificate

	records, err := loadAggregatorRecordsForBlockAnyFinalization(ctx, rm.storage, block)
	if err != nil {
		return false, fmt.Errorf("failed to load durable proposal records %s: %w", block.Index.String(), err)
	}
	stateIDs, leaves, err := stateIDsAndLeavesFromAggregatorRecords(records)
	if err != nil {
		return false, fmt.Errorf("failed to derive durable proposal leaves for block %s: %w", block.Index.String(), err)
	}

	rm.finalizationMu.Lock()
	defer rm.finalizationMu.Unlock()

	snapshot, err := rm.smtBackend.CreateSnapshot(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to create SMT snapshot for durable proposal %s: %w", block.Index.String(), err)
	}
	snapshotCommitted := false
	defer func() {
		if !snapshotCommitted {
			snapshot.Discard(ctx)
		}
	}()

	candidateRoot, err := applyDurableProposalLeaves(ctx, snapshot, leaves)
	if err != nil {
		return false, fmt.Errorf("failed to apply durable proposal leaves for block %s: %w", block.Index.String(), err)
	}
	if !bytes.Equal(candidateRoot, rootHash) {
		return false, fmt.Errorf("durable proposal block %s candidate root %s does not match certified root %s",
			block.Index.String(), api.HexBytes(candidateRoot).String(), rootHash.String())
	}

	if !rm.usesDiskSMTBackend() {
		smtNodes, err := rm.convertLeavesToNodes(leaves)
		if err != nil {
			return false, fmt.Errorf("failed to convert durable proposal leaves to SMT nodes: %w", err)
		}
		if _, err := rm.storeDataParallel(ctx, smtNodes); err != nil {
			return false, fmt.Errorf("failed to store durable proposal SMT nodes: %w", err)
		}
	}

	var preparedProofView smtbackend.PreparedProofView
	proofViewPublished := false
	defer func() {
		if preparedProofView != nil && !proofViewPublished {
			preparedProofView.Discard(ctx)
		}
	}()
	if rm.usesDiskSMTBackend() {
		if err := setBlockFinalizedWithCertificate(ctx, rm.storage, block); err != nil {
			return false, err
		}
		block.Finalized = true
		block.Status = models.FinalityStatusFinalized
	}
	if publishable, ok := snapshot.(smtbackend.ProofViewPreparingSnapshot); ok {
		preparedProofView, err = publishable.CommitAndPrepareProofView(ctx, smtbackend.CommitMetadata{BlockNumber: block.Index, RootHash: rootHash})
	} else {
		err = snapshot.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: block.Index, RootHash: rootHash})
	}
	if err != nil {
		return false, fmt.Errorf("failed to commit durable proposal SMT snapshot: %w", err)
	}
	snapshotCommitted = true

	if !rm.usesDiskSMTBackend() {
		if err := setBlockFinalizedWithCertificate(ctx, rm.storage, block); err != nil {
			return false, err
		}
		block.Finalized = true
		block.Status = models.FinalityStatusFinalized
	}

	rm.markProofsReady(block, stateIDs, records)
	if preparedProofView != nil {
		if err := preparedProofView.Publish(ctx); err != nil {
			return false, fmt.Errorf("failed to publish durable proposal proof view: %w", err)
		}
		proofViewPublished = true
	}
	rm.stateTracker.SetLastSyncedBlock(block.Index.Int)
	rm.ackRecoveredProposalCommitments(ctx, block.Index, stateIDs)
	rm.logger.WithContext(ctx).Info("Durable proposal finalized",
		"blockNumber", block.Index.String(),
		"records", len(records))
	return true, nil
}

// LoadDurableProposal returns a stored uncertified proposal for the requested
// round, if one exists.
func (rm *RoundManager) LoadDurableProposal(ctx context.Context, blockNumber *api.BigInt) (*models.Block, bool, error) {
	block, err := getBlockAnyFinalization(ctx, rm.storage, blockNumber)
	if err != nil {
		return nil, false, fmt.Errorf("failed to load durable proposal block %s: %w", blockNumber.String(), err)
	}
	if block == nil || block.Finalized || block.Status != models.FinalityStatusProposed {
		return nil, false, nil
	}
	return block, true, nil
}

type aggregatorRecordAnyFinalizationStorage interface {
	GetByStateIDAnyFinalization(ctx context.Context, stateID api.StateID) (*models.AggregatorRecord, error)
	GetByBlockNumberAnyFinalization(ctx context.Context, blockNumber *api.BigInt) ([]*models.AggregatorRecord, error)
}

type aggregatorRecordProposalAnyFinalizationStorage interface {
	GetByBlockNumberAndProposalIDAnyFinalization(ctx context.Context, blockNumber *api.BigInt, proposalID string) ([]*models.AggregatorRecord, error)
}

type aggregatorRecordSerialBatchStorage interface {
	StoreBatchSerial(ctx context.Context, records []*models.AggregatorRecord) error
}

type blockAnyFinalizationStorage interface {
	GetByNumberAnyFinalization(ctx context.Context, blockNumber *api.BigInt) (*models.Block, error)
}

type blockCertificateFinalizer interface {
	SetFinalizedWithCertificate(ctx context.Context, block *models.Block) error
}

type blockStatusUpdater interface {
	SetStatus(ctx context.Context, blockNumber *api.BigInt, status string) error
}

// AbandonDurableProposal removes an uncertified proposed block from the retry
// path. Proposed aggregator records are proposalId-scoped and left for lazy
// cleanup so abandonment stays a one-document status transition.
func (rm *RoundManager) AbandonDurableProposal(ctx context.Context, blockNumber *api.BigInt, rootHash api.HexBytes) error {
	block, err := getBlockAnyFinalization(ctx, rm.storage, blockNumber)
	if err != nil {
		return fmt.Errorf("failed to load durable proposal block %s: %w", blockNumber.String(), err)
	}
	if block == nil || block.Finalized || block.Status != models.FinalityStatusProposed {
		return nil
	}
	if !bytes.Equal(block.RootHash, rootHash) {
		return fmt.Errorf("refusing to abandon durable proposal %s: stored root %s does not match expected root %s",
			block.Index.String(), block.RootHash.String(), rootHash.String())
	}

	return rm.storage.WithTransaction(ctx, func(txCtx context.Context) error {
		if updater, ok := rm.storage.BlockStorage().(blockStatusUpdater); ok {
			if err := updater.SetStatus(txCtx, block.Index, models.FinalityStatusAbandoned); err != nil {
				return err
			}
		}
		return nil
	})
}

// RecoverUnfinalizedBlock checks for and completes any unfinalized blocks.
// Must be called before starting the round manager.
func RecoverUnfinalizedBlock(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	commitmentQueue interfaces.CommitmentQueue,
) (*RecoveryResult, error) {
	return recoverUnfinalizedBlock(ctx, log, storage, commitmentQueue, true)
}

func recoverUnfinalizedBlock(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	commitmentQueue interfaces.CommitmentQueue,
	repairSMTNodes bool,
) (*RecoveryResult, error) {
	log.WithContext(ctx).Info("Checking for unfinalized blocks...")

	unfinalizedBlocks, err := storage.BlockStorage().GetUnfinalized(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get unfinalized blocks: %w", err)
	}

	if len(unfinalizedBlocks) > 1 {
		return nil, fmt.Errorf("FATAL: found %d unfinalized blocks - data corruption", len(unfinalizedBlocks))
	}

	if len(unfinalizedBlocks) == 0 {
		log.WithContext(ctx).Info("No unfinalized blocks found")

		// Cleanup pending commitments that are already processed
		// (handles case where block was finalized but MarkProcessed failed)
		if err := CleanupProcessedPendingCommitments(ctx, log, storage, commitmentQueue); err != nil {
			return nil, fmt.Errorf("failed to cleanup pending commitments: %w", err)
		}

		return &RecoveryResult{Recovered: false}, nil
	}

	block := unfinalizedBlocks[0]
	log.WithContext(ctx).Info("Found unfinalized block, starting recovery",
		"blockNumber", block.Index.String(),
		"rootHash", block.RootHash.String())

	stateIDs, err := recoverBlock(ctx, log, storage, commitmentQueue, block, repairSMTNodes)
	if err != nil {
		return nil, fmt.Errorf("failed to recover block %s: %w", block.Index.String(), err)
	}

	log.WithContext(ctx).Info("Block recovery completed successfully", "blockNumber", block.Index.String())
	return &RecoveryResult{
		Recovered:   true,
		BlockNumber: block.Index,
		StateIDs:    stateIDs,
	}, nil
}

func recoverBlock(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	commitmentQueue interfaces.CommitmentQueue,
	block *models.Block,
	repairSMTNodes bool,
) ([]api.StateID, error) {
	records, err := loadAggregatorRecordsForBlockAnyFinalization(ctx, storage, block)
	if err != nil {
		return nil, fmt.Errorf("failed to get aggregator records: %w", err)
	}
	stateIDs, _, err := stateIDsAndLeavesFromAggregatorRecords(records)
	if err != nil {
		return nil, err
	}

	log.WithContext(ctx).Info("Aggregator records found", "stateCount", len(stateIDs))

	if repairSMTNodes {
		smtKeyStrings := make([]string, len(stateIDs))
		for i, stateID := range stateIDs {
			keyBytes, err := stateID.GetTreeKey()
			if err != nil {
				return nil, fmt.Errorf("failed to get SMT key for stateID: %w", err)
			}
			smtKeyStrings[i] = api.HexBytes(keyBytes).String()
		}
		existingSmtKeys, err := storage.SmtStorage().GetExistingKeys(ctx, smtKeyStrings)
		if err != nil {
			return nil, fmt.Errorf("failed to check existing SMT nodes: %w", err)
		}

		var missingSmtKeys []api.StateID
		for i, stateID := range stateIDs {
			if !existingSmtKeys[smtKeyStrings[i]] {
				missingSmtKeys = append(missingSmtKeys, stateID)
			}
		}

		log.WithContext(ctx).Info("Recovery status",
			"totalStateIDs", len(stateIDs),
			"existingRecords", len(records),
			"existingSmtNodes", len(existingSmtKeys),
			"missingRecords", 0,
			"missingSmtNodes", len(missingSmtKeys))

		if len(missingSmtKeys) > 0 {
			if err := recoverMissingSMTNodes(ctx, log, storage, commitmentQueue, missingSmtKeys); err != nil {
				return nil, err
			}
		}
	} else {
		log.WithContext(ctx).Info("Recovery status",
			"totalStateIDs", len(stateIDs),
			"existingRecords", len(records),
			"repairSMTNodes", false)
	}

	if err := setBlockFinalizedWithCertificate(ctx, storage, block); err != nil {
		return nil, err
	}

	// Ack certification requests in Redis - fetch only the ones we need by state ID.
	commitmentMap, err := commitmentQueue.GetByStateIDs(ctx, stateIDs)
	if err != nil {
		log.WithContext(ctx).Warn("Failed to get commitments for acking, they may be re-processed", "error", err)
	} else if len(commitmentMap) > 0 {
		ackEntries := make([]interfaces.CertificationRequestAck, 0, len(commitmentMap))
		for _, c := range commitmentMap {
			ackEntries = append(ackEntries, interfaces.CertificationRequestAck{
				StateID:  c.StateID,
				StreamID: c.StreamID,
			})
		}
		if err := commitmentQueue.MarkProcessed(ctx, ackEntries); err != nil {
			log.WithContext(ctx).Warn("Failed to ack commitments, they may be re-processed", "error", err)
		} else {
			log.WithContext(ctx).Info("Acked commitments", "count", len(ackEntries))
		}
	}

	return stateIDs, nil
}

func sortAggregatorRecordsByLeafIndex(records []*models.AggregatorRecord) {
	sort.SliceStable(records, func(i, j int) bool {
		left := records[i]
		right := records[j]
		if left == nil || left.LeafIndex == nil || left.LeafIndex.Int == nil {
			return right != nil && right.LeafIndex != nil && right.LeafIndex.Int != nil
		}
		if right == nil || right.LeafIndex == nil || right.LeafIndex.Int == nil {
			return false
		}
		return left.LeafIndex.Int.Cmp(right.LeafIndex.Int) < 0
	})
}

func loadAggregatorRecordsForBlockAnyFinalization(ctx context.Context, storage interfaces.Storage, block *models.Block) ([]*models.AggregatorRecord, error) {
	if block == nil {
		return nil, fmt.Errorf("block is nil")
	}
	records, err := getAggregatorRecordsByBlockAndProposalAnyFinalization(ctx, storage, block.Index, block.ProposalID)
	if err != nil {
		return nil, err
	}
	sortAggregatorRecordsByLeafIndex(records)
	return records, nil
}

func stateIDsAndLeavesFromAggregatorRecords(records []*models.AggregatorRecord) ([]api.StateID, []smtbackend.LeafInput, error) {
	stateIDs := make([]api.StateID, len(records))
	leaves := make([]smtbackend.LeafInput, len(records))
	for i, record := range records {
		if record == nil {
			return nil, nil, fmt.Errorf("nil aggregator record at index %d", i)
		}
		stateIDs[i] = record.StateID
		key, err := record.StateID.GetTreeKey()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get SMT key for stateID %s: %w", record.StateID.String(), err)
		}
		leaves[i] = smtbackend.LeafInput{
			Key:   append([]byte(nil), key...),
			Value: append([]byte(nil), record.CertificationData.TransactionHash...),
		}
	}
	return stateIDs, leaves, nil
}

func applyDurableProposalLeaves(ctx context.Context, snapshot smtbackend.Snapshot, leaves []smtbackend.LeafInput) ([]byte, error) {
	if len(leaves) == 0 {
		return snapshot.RootHashRaw(ctx)
	}
	result, err := snapshot.AddLeavesClassified(ctx, leaves)
	if err != nil {
		return nil, err
	}
	if err := result.ValidateAllAccepted(len(leaves)); err != nil {
		return nil, err
	}
	return result.CandidateRoot, nil
}

func (rm *RoundManager) ackRecoveredProposalCommitments(ctx context.Context, blockNumber *api.BigInt, stateIDs []api.StateID) {
	if rm.commitmentQueue == nil || len(stateIDs) == 0 {
		return
	}
	commitmentMap, err := rm.commitmentQueue.GetByStateIDs(ctx, stateIDs)
	if err != nil {
		rm.logger.WithContext(ctx).Warn("Failed to load durable proposal commitments for ack",
			"blockNumber", blockNumber.String(),
			"error", err.Error())
		return
	}
	if len(commitmentMap) == 0 {
		return
	}
	ackEntries := make([]interfaces.CertificationRequestAck, 0, len(commitmentMap))
	for _, commitment := range commitmentMap {
		ackEntries = append(ackEntries, interfaces.CertificationRequestAck{
			StateID:  commitment.StateID,
			StreamID: commitment.StreamID,
		})
	}
	if err := rm.commitmentQueue.MarkProcessed(ctx, ackEntries); err != nil {
		rm.logger.WithContext(ctx).Warn("Failed to ack durable proposal commitments",
			"blockNumber", blockNumber.String(),
			"commitments", len(ackEntries),
			"error", err.Error())
	}
}

func recoverMissingSMTNodes(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	commitmentQueue interfaces.CommitmentQueue,
	missingSmtKeys []api.StateID,
) error {
	if len(missingSmtKeys) == 0 {
		return nil
	}

	commitmentMap, err := commitmentQueue.GetByStateIDs(ctx, missingSmtKeys)
	if err != nil {
		return fmt.Errorf("failed to get commitments: %w", err)
	}

	var nodes []*models.SmtNode
	for _, stateID := range missingSmtKeys {
		commitment, ok := commitmentMap[stateID.String()]
		if !ok {
			keyBytes, err := stateID.GetTreeKey()
			if err != nil {
				return fmt.Errorf("failed to get SMT key for stateID: %w", err)
			}
			existingNode, err := storage.SmtStorage().GetByKey(ctx, keyBytes)
			if err != nil {
				return fmt.Errorf("failed to check existing SMT node: %w", err)
			}
			if existingNode != nil {
				continue
			}
			existingRecord, err := getAggregatorRecordAnyFinalization(ctx, storage, stateID)
			if err != nil {
				return fmt.Errorf("failed to check existing aggregator record: %w", err)
			}
			if existingRecord == nil {
				return fmt.Errorf("FATAL: durable aggregator record not found for SMT key %s", stateID)
			}
			nodes = append(nodes, models.NewSmtNode(keyBytes, append([]byte(nil), existingRecord.CertificationData.TransactionHash...)))
			continue
		}

		keyBytes, err := commitment.StateID.GetTreeKey()
		if err != nil {
			return fmt.Errorf("failed to get SMT key for commitment: %w", err)
		}
		leafValue, err := commitment.LeafValue()
		if err != nil {
			return fmt.Errorf("failed to create leaf value: %w", err)
		}
		nodes = append(nodes, models.NewSmtNode(keyBytes, leafValue))
	}

	if len(nodes) > 0 {
		if err := storage.SmtStorage().StoreBatch(ctx, nodes); err != nil {
			return fmt.Errorf("failed to store missing SMT nodes: %w", err)
		}
		log.WithContext(ctx).Info("Stored missing SMT nodes", "count", len(nodes))
	}

	return nil
}

func getBlockAnyFinalization(ctx context.Context, storage interfaces.Storage, blockNumber *api.BigInt) (*models.Block, error) {
	if reader, ok := storage.BlockStorage().(blockAnyFinalizationStorage); ok {
		return reader.GetByNumberAnyFinalization(ctx, blockNumber)
	}
	block, err := storage.BlockStorage().GetByNumber(ctx, blockNumber)
	if err != nil {
		return nil, err
	}
	if block != nil {
		return block, nil
	}
	unfinalized, err := storage.BlockStorage().GetUnfinalized(ctx)
	if err != nil {
		return nil, err
	}
	for _, candidate := range unfinalized {
		if candidate != nil && candidate.Index.Cmp(blockNumber.Int) == 0 {
			return candidate, nil
		}
	}
	return nil, nil
}

func setBlockFinalizedWithCertificate(ctx context.Context, storage interfaces.Storage, block *models.Block) error {
	if updater, ok := storage.BlockStorage().(blockCertificateFinalizer); ok {
		if err := updater.SetFinalizedWithCertificate(ctx, block); err != nil {
			return fmt.Errorf("failed to set block as finalized: %w", err)
		}
		return nil
	}
	if err := storage.BlockStorage().SetFinalized(ctx, block.Index, true); err != nil {
		return fmt.Errorf("failed to set block as finalized: %w", err)
	}
	return nil
}

func getAggregatorRecordAnyFinalization(ctx context.Context, storage interfaces.Storage, stateID api.StateID) (*models.AggregatorRecord, error) {
	if reader, ok := storage.AggregatorRecordStorage().(aggregatorRecordAnyFinalizationStorage); ok {
		return reader.GetByStateIDAnyFinalization(ctx, stateID)
	}
	return storage.AggregatorRecordStorage().GetByStateID(ctx, stateID)
}

func getAggregatorRecordsByBlockAnyFinalization(ctx context.Context, storage interfaces.Storage, blockNumber *api.BigInt) ([]*models.AggregatorRecord, error) {
	if reader, ok := storage.AggregatorRecordStorage().(aggregatorRecordAnyFinalizationStorage); ok {
		return reader.GetByBlockNumberAnyFinalization(ctx, blockNumber)
	}
	return storage.AggregatorRecordStorage().GetByBlockNumber(ctx, blockNumber)
}

func getAggregatorRecordsByBlockAndProposalAnyFinalization(
	ctx context.Context,
	storage interfaces.Storage,
	blockNumber *api.BigInt,
	proposalID string,
) ([]*models.AggregatorRecord, error) {
	if reader, ok := storage.AggregatorRecordStorage().(aggregatorRecordProposalAnyFinalizationStorage); ok {
		return reader.GetByBlockNumberAndProposalIDAnyFinalization(ctx, blockNumber, proposalID)
	}
	records, err := getAggregatorRecordsByBlockAnyFinalization(ctx, storage, blockNumber)
	if err != nil {
		return nil, err
	}
	return filterAggregatorRecordsByProposalID(records, proposalID), nil
}

func filterAggregatorRecordsByProposalID(records []*models.AggregatorRecord, proposalID string) []*models.AggregatorRecord {
	filtered := records[:0]
	for _, record := range records {
		if record != nil && record.ProposalID == proposalID {
			filtered = append(filtered, record)
		}
	}
	return filtered
}

// LoadRecoveredNodesIntoBackend loads SMT nodes for a recovered block into the active SMT backend.
// Used in HA mode when a follower becomes leader.
func LoadRecoveredNodesIntoBackend(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	backend smtbackend.Backend,
	blockNumber *api.BigInt,
	stateIDs []api.StateID,
) error {
	if backend == nil {
		return fmt.Errorf("SMT backend not initialized")
	}
	var expectedRoot []byte
	var block *models.Block
	if blockNumber != nil {
		var err error
		block, err = storage.BlockStorage().GetByNumber(ctx, blockNumber)
		if err != nil {
			return fmt.Errorf("failed to load recovered finalized block %s: %w", blockNumber.String(), err)
		}
		if block != nil {
			expectedRoot = block.RootHash
		}
	}
	if len(stateIDs) == 0 {
		if blockNumber == nil {
			return nil
		}
		return loadRecoveredLeavesIntoBackend(ctx, log, backend, blockNumber, expectedRoot, nil)
	}

	if usesDiskBackedBackend(backend) {
		if blockNumber == nil {
			return fmt.Errorf("disk recovered block number is nil")
		}
		if block == nil {
			return fmt.Errorf("failed to load recovered finalized block %s", blockNumber.String())
		}
		records, err := loadAggregatorRecordsForBlockAnyFinalization(ctx, storage, block)
		if err != nil {
			return fmt.Errorf("failed to load recovered aggregator records: %w", err)
		}
		recordStateIDs, leaves, err := stateIDsAndLeavesFromAggregatorRecords(records)
		if err != nil {
			return fmt.Errorf("failed to load recovered leaves from aggregator records: %w", err)
		}
		if !sameStateIDs(recordStateIDs, stateIDs) {
			return fmt.Errorf("recovered aggregator records state IDs mismatch: records=%d recovered=%d", len(recordStateIDs), len(stateIDs))
		}
		log.WithContext(ctx).Info("Loading recovered records into disk SMT", "count", len(leaves))
		return loadRecoveredLeavesIntoBackend(ctx, log, backend, blockNumber, expectedRoot, leaves)
	}

	log.WithContext(ctx).Info("Loading recovered SMT nodes", "count", len(stateIDs))

	// Deduplicate state IDs (duplicates can occur when the same certification
	// request is submitted twice in the same round).
	seen := make(map[string]struct{}, len(stateIDs))
	keys := make([]api.HexBytes, 0, len(stateIDs))
	for _, stateID := range stateIDs {
		key := string(stateID)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		keyBytes, err := stateID.GetTreeKey()
		if err != nil {
			return fmt.Errorf("failed to get SMT key for stateID %s: %w", stateID, err)
		}
		keys = append(keys, api.HexBytes(keyBytes))
	}

	nodes, err := storage.SmtStorage().GetByKeys(ctx, keys)
	if err != nil {
		return fmt.Errorf("failed to get SMT nodes: %w", err)
	}

	if len(nodes) != len(keys) {
		return fmt.Errorf("FATAL: expected %d SMT nodes but found %d - data corruption", len(keys), len(nodes))
	}

	leaves := make([]smtbackend.LeafInput, len(nodes))
	for i, node := range nodes {
		leaves[i] = smtbackend.LeafInput{
			Key:   node.Key,
			Value: node.Value,
		}
	}

	return loadRecoveredLeavesIntoBackend(ctx, log, backend, blockNumber, expectedRoot, leaves)
}

func usesDiskBackedBackend(backend smtbackend.Backend) bool {
	diskBacked, ok := backend.(smtbackend.DiskBacked)
	return ok && diskBacked.IsDiskBackedSMT()
}

func loadRecoveredLeavesIntoBackend(
	ctx context.Context,
	log *logger.Logger,
	backend smtbackend.Backend,
	blockNumber *api.BigInt,
	expectedRoot []byte,
	leaves []smtbackend.LeafInput,
) error {
	snapshot, err := backend.CreateSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("failed to create SMT snapshot: %w", err)
	}
	defer snapshot.Discard(ctx)

	var rootHash []byte
	if len(leaves) == 0 {
		state, err := backend.CommittedState(ctx)
		if err != nil {
			return fmt.Errorf("failed to read SMT backend state: %w", err)
		}
		rootHash = state.RootHash
	} else {
		result, err := snapshot.AddLeavesClassified(ctx, leaves)
		if err != nil {
			return fmt.Errorf("failed to add recovered nodes to SMT: %w", err)
		}
		if err := result.ValidateAllAccepted(len(leaves)); err != nil {
			return fmt.Errorf("failed to add recovered nodes to SMT: %w", err)
		}
		rootHash = result.CandidateRoot
	}
	if expectedRoot != nil {
		if !bytes.Equal(rootHash, expectedRoot) {
			return fmt.Errorf("recovered SMT root mismatch: candidate=%s block=%s",
				api.HexBytes(rootHash).String(), api.HexBytes(expectedRoot).String())
		}
		rootHash = expectedRoot
	}
	if err := snapshot.Commit(ctx, smtbackend.CommitMetadata{BlockNumber: blockNumber, RootHash: rootHash}); err != nil {
		return fmt.Errorf("failed to commit recovered nodes to SMT: %w", err)
	}

	log.WithContext(ctx).Info("Loaded recovered SMT nodes", "count", len(leaves))
	return nil
}

// CleanupProcessedPendingCommitments ACKs pending commitments that are already in AggregatorRecords.
// This handles the case where a block was finalized but MarkProcessed failed (e.g., Redis was down).
func CleanupProcessedPendingCommitments(
	ctx context.Context,
	log *logger.Logger,
	storage interfaces.Storage,
	commitmentQueue interfaces.CommitmentQueue,
) error {
	pendingCommitments, err := commitmentQueue.GetAllPending(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pending commitments: %w", err)
	}
	if len(pendingCommitments) == 0 {
		return nil
	}

	stateIDs := make([]string, len(pendingCommitments))
	for i, c := range pendingCommitments {
		stateIDs[i] = c.StateID.String()
	}

	existingIDs, err := storage.AggregatorRecordStorage().GetExistingStateIDs(ctx, stateIDs)
	if err != nil {
		return fmt.Errorf("failed to check existing records: %w", err)
	}

	var ackEntries []interfaces.CertificationRequestAck
	for _, c := range pendingCommitments {
		if existingIDs[c.StateID.String()] {
			ackEntries = append(ackEntries, interfaces.CertificationRequestAck{
				StateID:  c.StateID,
				StreamID: c.StreamID,
			})
		}
	}

	if len(ackEntries) > 0 {
		if err := commitmentQueue.MarkProcessed(ctx, ackEntries); err != nil {
			return fmt.Errorf("failed to ack processed commitments: %w", err)
		}
		log.WithContext(ctx).Info("Cleaned up already-processed pending commitments", "count", len(ackEntries))
	}

	return nil
}
