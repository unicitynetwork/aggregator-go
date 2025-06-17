package round

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"

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
		path, err := rm.generateLeafPath(commitment.RequestID)
		if err != nil {
			return "", nil, fmt.Errorf("failed to generate leaf path for commitment %s: %w", commitment.RequestID, err)
		}

		// Create leaf value (hash of commitment data)
		leafValue, err := rm.createLeafValue(commitment)
		if err != nil {
			return "", nil, fmt.Errorf("failed to create leaf value for commitment %s: %w", commitment.RequestID, err)
		}

		leaves[i] = &smt.Leaf{
			Path:  big.NewInt(int64(path)),
			Value: leafValue,
		}

		// Create aggregator record
		leafIndex := api.NewBigInt(big.NewInt(int64(i)))
		records[i] = models.NewAggregatorRecord(commitment, blockNumber, leafIndex)

		rm.logger.WithContext(ctx).Debug("Created SMT leaf for commitment",
			"requestId", commitment.RequestID.String(),
			"path", path,
			"leafIndex", i)
	}

	// Add all leaves to SMT in a single batch operation
	rootHash, err := rm.smt.AddLeaves(leaves)
	if err != nil {
		return "", nil, fmt.Errorf("failed to add batch to SMT: %w", err)
	}

	rm.logger.WithContext(ctx).Info("Successfully processed commitment batch",
		"rootHash", rootHash,
		"commitmentCount", len(commitments))

	// Store aggregator records
	for _, record := range records {
		// Check if record already exists to prevent duplicate key errors
		existing, err := rm.storage.AggregatorRecordStorage().GetByRequestID(ctx, record.RequestID)
		if err != nil {
			return "", nil, fmt.Errorf("failed to check existing aggregator record for %s: %w", record.RequestID, err)
		}

		if existing != nil {
			rm.logger.WithContext(ctx).Debug("Aggregator record already exists, skipping",
				"requestId", record.RequestID.String())
			continue
		}

		if err := rm.storage.AggregatorRecordStorage().Store(ctx, record); err != nil {
			return "", nil, fmt.Errorf("failed to store aggregator record for %s: %w", record.RequestID, err)
		}
	}

	return rootHash, records, nil
}

// generateLeafPath generates a uint64 path for the SMT from a RequestID
func (rm *RoundManager) generateLeafPath(requestID api.RequestID) (uint64, error) {
	// Convert RequestID to bytes
	requestIDBytes, err := requestID.Bytes()
	if err != nil {
		return 0, fmt.Errorf("failed to convert requestID to bytes: %w", err)
	}

	// Hash the requestID to get a deterministic path
	hash := sha256.Sum256(requestIDBytes)

	// Use the first 8 bytes of the hash as a uint64 path
	path := uint64(0)
	for i := 0; i < 8 && i < len(hash); i++ {
		path = (path << 8) | uint64(hash[i])
	}

	return path, nil
}

// createLeafValue creates the value to store in the SMT leaf for a commitment
func (rm *RoundManager) createLeafValue(commitment *models.Commitment) ([]byte, error) {
	// Create a deterministic value from commitment data
	// This includes requestID, transactionHash, and authenticator data

	requestIDBytes, err := commitment.RequestID.Bytes()
	if err != nil {
		return nil, fmt.Errorf("failed to get requestID bytes: %w", err)
	}

	transactionHashBytes, err := commitment.TransactionHash.Bytes()
	if err != nil {
		return nil, fmt.Errorf("failed to get transactionHash bytes: %w", err)
	}

	// Combine all the data
	data := make([]byte, 0, len(requestIDBytes)+len(transactionHashBytes)+len(commitment.Authenticator.StateHash)+len(commitment.Authenticator.PublicKey)+len(commitment.Authenticator.Signature))
	data = append(data, requestIDBytes...)
	data = append(data, transactionHashBytes...)
	data = append(data, commitment.Authenticator.StateHash...)
	data = append(data, commitment.Authenticator.PublicKey...)
	data = append(data, commitment.Authenticator.Signature...)

	// Hash the combined data to create a fixed-size leaf value
	hash := sha256.Sum256(data)
	return hash[:], nil
}

// finalizeBlock creates and persists a new block with the given data
func (rm *RoundManager) finalizeBlock(ctx context.Context, blockNumber *api.BigInt, rootHash string, records []*models.AggregatorRecord) error {
	rm.roundMutex.Lock()
	if rm.currentRound != nil {
		rm.currentRound.State = RoundStateFinalizing
	}
	rm.roundMutex.Unlock()

	rm.logger.WithContext(ctx).Info("Finalizing block",
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
	if err := rm.bftClient.CertificationRequest(ctx, rootHash); err != nil {
		return fmt.Errorf("failed to send certification request: %w", err)
	}

	// Store block
	if err := rm.storage.BlockStorage().Store(ctx, block); err != nil {
		return fmt.Errorf("failed to store block: %w", err)
	}

	// Update current round with finalized block
	rm.roundMutex.Lock()
	if rm.currentRound != nil {
		rm.currentRound.Block = block
	}
	rm.roundMutex.Unlock()

	rm.logger.WithContext(ctx).Info("Block finalized successfully",
		"blockNumber", blockNumber.String(),
		"rootHash", block.RootHash.String(),
		"commitmentCount", len(records))

	return nil
}
