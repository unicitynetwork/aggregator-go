package round

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"

	"github.com/fxamacker/cbor/v2"
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/internal/smt"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// processBatch processes a batch of commitments and adds them to the SMT
func (rm *RoundManager) processBatch(ctx context.Context, commitments []*models.Commitment, blockNumber *api.BigInt) (string, []*models.AggregatorRecord, error) {
	rm.logger.WithContext(ctx).Info("Processing commitment batch",
		"commitmentCount", len(commitments),
		"blockNumber", blockNumber.String())

	// Convert commitments to SMT leaves
	leaves := make([]*smt.Leaf, len(commitments))
	records := make([]*models.AggregatorRecord, len(commitments))

	for i, commitment := range commitments {
		// Generate leaf path from requestID
		path, err := commitment.RequestID.GetPath()
		if err != nil {
			return "", nil, fmt.Errorf("failed to get path for requestID %s: %w", commitment.RequestID, err)
		}

		// Create leaf value (hash of commitment data)
		leafValue, err := rm.createLeafValue(commitment)
		if err != nil {
			return "", nil, fmt.Errorf("failed to create leaf value for commitment %s: %w", commitment.RequestID, err)
		}

		leaves[i] = &smt.Leaf{
			Path:  path,
			Value: leafValue,
		}

		// Create aggregator record with the current block number
		leafIndex := api.NewBigInt(big.NewInt(int64(i)))
		records[i] = models.NewAggregatorRecord(commitment, blockNumber, leafIndex)

		rm.logger.WithContext(ctx).Debug("Created SMT leaf for commitment",
			"requestId", commitment.RequestID.String(),
			"path", path,
			"leafIndex", i)
	}

	// Store aggregator records BEFORE adding leaves to SMT snapshot
	rm.logger.WithContext(ctx).Debug("Storing aggregator records before SMT snapshot update",
		"recordCount", len(records))

	if err := rm.storeAggregatorRecords(ctx, records); err != nil {
		return "", nil, fmt.Errorf("failed to store aggregator records: %w", err)
	}

	// Get the current round's snapshot
	rm.roundMutex.RLock()
	snapshot := rm.currentRound.Snapshot
	rm.roundMutex.RUnlock()

	if snapshot == nil {
		return "", nil, fmt.Errorf("no snapshot available for current round")
	}

	// Add all leaves to the round's SMT snapshot (not the main SMT yet)
	rootHash, err := snapshot.AddLeaves(leaves)
	if err != nil {
		return "", nil, fmt.Errorf("failed to add batch to SMT snapshot: %w", err)
	}

	rm.logger.WithContext(ctx).Info("Successfully processed commitment batch to snapshot",
		"rootHash", rootHash,
		"commitmentCount", len(commitments))

	// Records have been stored during processing
	rm.logger.WithContext(ctx).Debug("Aggregator records stored successfully during batch processing",
		"recordCount", len(records))

	return rootHash, records, nil
}

// createLeafValue creates the value to store in the SMT leaf for a commitment
// This matches the TypeScript LeafValue.create() method exactly:
// - CBOR encode the authenticator as an array [algorithm, publicKey, signature, stateHashImprint]
// - Hash the CBOR-encoded authenticator and transaction hash imprint using SHA256
// - Return as DataHash imprint format (2-byte algorithm prefix + hash)
func (rm *RoundManager) createLeafValue(commitment *models.Commitment) ([]byte, error) {
	// Get the state hash imprint for CBOR encoding
	stateHashImprint, err := commitment.Authenticator.StateHash.Imprint()
	if err != nil {
		return nil, fmt.Errorf("failed to get state hash imprint: %w", err)
	}

	// CBOR encode the authenticator as an array (matching TypeScript authenticator.toCBOR())
	// TypeScript: [algorithm, publicKey, signature.encode(), stateHash.imprint]
	authenticatorArray := []interface{}{
		commitment.Authenticator.Algorithm,         // algorithm as text string
		[]byte(commitment.Authenticator.PublicKey), // publicKey as byte string
		[]byte(commitment.Authenticator.Signature), // signature as byte string
		stateHashImprint,                           // stateHash.imprint as byte string
	}

	authenticatorCBOR, err := cbor.Marshal(authenticatorArray)
	if err != nil {
		return nil, fmt.Errorf("failed to CBOR encode authenticator: %w", err)
	}

	// Get the transaction hash imprint
	transactionHashImprint, err := commitment.TransactionHash.Imprint()
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction hash imprint: %w", err)
	}

	// Create SHA256 hasher and update with CBOR-encoded authenticator and transaction hash imprint
	// This matches the TypeScript DataHasher(SHA256).update(authenticator.toCBOR()).update(transactionHash.imprint).digest()
	hasher := sha256.New()
	hasher.Write(authenticatorCBOR)
	hasher.Write(transactionHashImprint)

	// Get the final hash
	hash := hasher.Sum(nil)

	// Return as DataHash imprint with SHA256 algorithm prefix (0x00, 0x00)
	imprint := make([]byte, 2+len(hash))
	imprint[0] = 0x00 // SHA256 algorithm high byte
	imprint[1] = 0x00 // SHA256 algorithm low byte
	copy(imprint[2:], hash[:])

	return imprint, nil
}

// ProposeBlock creates and proposes a new block with the given data
func (rm *RoundManager) proposeBlock(ctx context.Context, blockNumber *api.BigInt, rootHash string, records []*models.AggregatorRecord) error {
	rm.logger.WithContext(ctx).Info("proposeBlock called",
		"blockNumber", blockNumber.String(),
		"rootHash", rootHash,
		"recordCount", len(records))

	rm.roundMutex.Lock()
	if rm.currentRound != nil {
		rm.logger.WithContext(ctx).Debug("Changing round state to finalizing",
			"roundNumber", rm.currentRound.Number.String(),
			"previousState", rm.currentRound.State.String())
		rm.currentRound.State = RoundStateFinalizing
	}
	rm.roundMutex.Unlock()

	rm.logger.WithContext(ctx).Info("Creating block proposal",
		"blockNumber", blockNumber.String(),
		"rootHash", rootHash,
		"recordCount", len(records))

	// Get parent block hash
	var parentHash api.HexBytes
	if blockNumber.Cmp(big.NewInt(1)) > 0 {
		// Get previous block
		prevBlockNumber := api.NewBigInt(nil)
		prevBlockNumber.Set(blockNumber.Int)
		prevBlockNumber.Sub(prevBlockNumber.Int, big.NewInt(1))

		prevBlock, err := rm.storage.BlockStorage().GetByNumber(ctx, prevBlockNumber)
		if err != nil {
			return fmt.Errorf("failed to get previous block %s: %w", prevBlockNumber.String(), err)
		}
		if prevBlock != nil {
			// Use the block's root hash as the "hash" for now
			parentHash = prevBlock.RootHash
		}
	}

	// Create block (simplified for now)
	block := models.NewBlock(
		blockNumber,
		"unicity",
		"1.0",
		"mainnet",
		api.NewHexBytes([]byte(rootHash)),
		parentHash,
	)

	rm.logger.WithContext(ctx).Info("Sending certification request to BFT client",
		"blockNumber", blockNumber.String(),
		"bftClientType", fmt.Sprintf("%T", rm.bftClient))

	if err := rm.bftClient.CertificationRequest(ctx, block); err != nil {
		rm.logger.WithContext(ctx).Error("Failed to send certification request",
			"blockNumber", blockNumber.String(),
			"error", err.Error())
		return fmt.Errorf("failed to send certification request: %w", err)
	}

	rm.logger.WithContext(ctx).Info("Certification request sent successfully",
		"blockNumber", blockNumber.String())

	return nil
}

// finalizeBlock creates and persists a new block with the given data
func (rm *RoundManager) FinalizeBlock(ctx context.Context, block *models.Block) error {
	rm.logger.WithContext(ctx).Info("FinalizeBlock called",
		"blockNumber", block.Index.String(),
		"rootHash", block.RootHash.String(),
		"hasUnicityCertificate", block.UnicityCertificate != nil)

	// CRITICAL: Store all commitment data BEFORE storing the block to prevent race conditions
	// where API returns partial block data

	// First, collect all request IDs that will be in this block
	rm.roundMutex.Lock()
	requestIds := make([]api.RequestID, 0)
	if rm.currentRound != nil {
		requestIds = make([]api.RequestID, 0, len(rm.currentRound.Commitments))
		for _, commitment := range rm.currentRound.Commitments {
			requestIds = append(requestIds, commitment.RequestID)
		}
	}
	rm.roundMutex.Unlock()

	rm.roundMutex.Lock()
	if rm.currentRound != nil && len(rm.currentRound.Commitments) > 0 {
		rm.logger.WithContext(ctx).Debug("Preparing commitment data before block storage",
			"roundNumber", rm.currentRound.Number.String(),
			"commitmentCount", len(rm.currentRound.Commitments),
			"recordCount", len(rm.currentRound.PendingRecords))

		// Extract data we need
		requestIDs := make([]api.RequestID, len(rm.currentRound.Commitments))
		for i, commitment := range rm.currentRound.Commitments {
			requestIDs[i] = commitment.RequestID
		}
		rm.roundMutex.Unlock()

		// Note: Aggregator records are now stored during processBatch, not here
		rm.logger.WithContext(ctx).Debug("Aggregator records already stored during batch processing",
			"commitmentCount", len(requestIDs))

		// Mark commitments as processed BEFORE storing the block
		if err := rm.storage.CommitmentStorage().MarkProcessed(ctx, requestIDs); err != nil {
			rm.logger.WithContext(ctx).Error("Failed to mark commitments as processed",
				"error", err.Error(),
				"blockNumber", block.Index.String())
			return fmt.Errorf("failed to mark commitments as processed: %w", err)
		}

		rm.logger.WithContext(ctx).Info("Successfully prepared commitment data",
			"count", len(requestIDs),
			"blockNumber", block.Index.String())

	} else {
		rm.roundMutex.Unlock()
	}

	// Store the block first - before committing the snapshot
	rm.logger.WithContext(ctx).Debug("Storing block in database",
		"blockNumber", block.Index.String())

	if err := rm.storage.BlockStorage().Store(ctx, block); err != nil {
		rm.logger.WithContext(ctx).Error("Failed to store block",
			"blockNumber", block.Index.String(),
			"error", err.Error())
		return fmt.Errorf("failed to store block: %w", err)
	}

	// Store block records mapping
	if err := rm.storage.BlockRecordsStorage().Store(ctx, models.NewBlockRecords(block.Index, requestIds)); err != nil {
		return fmt.Errorf("failed to store block record: %w", err)
	}

	// CRITICAL: Commit the snapshot to the main SMT AFTER storing the block successfully
	// This ensures the SMT state only reflects successfully persisted blocks
	rm.roundMutex.Lock()
	snapshot := rm.currentRound.Snapshot
	rm.roundMutex.Unlock()

	if snapshot != nil {
		rm.logger.WithContext(ctx).Info("Committing snapshot to main SMT after successful block storage",
			"blockNumber", block.Index.String())

		if err := snapshot.Commit(rm.smt); err != nil {
			rm.logger.WithContext(ctx).Error("Failed to commit snapshot to SMT",
				"blockNumber", block.Index.String(),
				"error", err.Error())
			// Note: Block is already stored, but SMT is inconsistent
			// This is a critical error that needs manual intervention
			return fmt.Errorf("CRITICAL: block stored but snapshot commit failed - SMT inconsistent: %w", err)
		}

		rm.logger.WithContext(ctx).Info("Successfully committed snapshot to main SMT",
			"blockNumber", block.Index.String())
	}

	// Update current round with finalized block
	rm.roundMutex.Lock()
	if rm.currentRound != nil {
		rm.currentRound.Block = block
		// Clear pending data as it's now finalized
		rm.currentRound.PendingRecords = nil
		rm.currentRound.PendingRootHash = ""
		// Clear the snapshot as it has been committed to the main SMT
		rm.currentRound.Snapshot = nil
	}
	rm.roundMutex.Unlock()

	rm.logger.WithContext(ctx).Info("Block finalized and stored successfully",
		"blockNumber", block.Index.String(),
		"rootHash", block.RootHash.String())

	return nil
}

func (rm *RoundManager) storeAggregatorRecords(ctx context.Context, records []*models.AggregatorRecord) error {
	for _, record := range records {
		// Check if record already exists to prevent duplicate key errors
		existing, err := rm.storage.AggregatorRecordStorage().GetByRequestID(ctx, record.RequestID)
		if err != nil {
			return fmt.Errorf("failed to check existing aggregator record for %s: %w", record.RequestID, err)
		}

		if existing != nil {
			rm.logger.WithContext(ctx).Debug("Aggregator record already exists, skipping",
				"requestId", record.RequestID.String())
			continue
		}

		if err := rm.storage.AggregatorRecordStorage().Store(ctx, record); err != nil {
			return fmt.Errorf("failed to store aggregator record for %s: %w", record.RequestID, err)
		}
		existing, err = rm.storage.AggregatorRecordStorage().GetByRequestID(ctx, record.RequestID)
		if err != nil {
			rm.logger.WithContext(ctx).Error("Failed to check existing aggregator record",
				"requestId", record.RequestID.String(),
				"error", err.Error())
		}
		if existing == nil {
			rm.logger.WithContext(ctx).Error("Stored aggregator record not found after storage",
				"requestId", record.RequestID.String())
		}
	}
	rm.logger.WithContext(ctx).Info("Successfully stored aggregator records",
		"recordCount", len(records))
	return nil
}
