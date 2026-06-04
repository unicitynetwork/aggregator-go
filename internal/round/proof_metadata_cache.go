package round

import (
	"github.com/unicitynetwork/aggregator-go/internal/models"
	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

const maxProofMetadataCacheBlocks = 8192

type proofMetadataCache struct {
	maxRecords int

	records     map[string]*models.AggregatorRecord
	recordOrder []recordRef
	recordHead  int

	blocks     map[string]*models.Block
	blockOrder []string
	blockHead  int
}

type recordRef struct {
	key    string
	record *models.AggregatorRecord
}

func newProofMetadataCache(maxRecords int) *proofMetadataCache {
	if maxRecords <= 0 {
		return nil
	}
	return &proofMetadataCache{
		maxRecords: maxRecords,
		records:    make(map[string]*models.AggregatorRecord),
		blocks:     make(map[string]*models.Block),
	}
}

func (c *proofMetadataCache) add(block *models.Block, records []*models.AggregatorRecord) {
	if c == nil || block == nil {
		return
	}

	rootKey := block.RootHash.String()
	if _, exists := c.blocks[rootKey]; !exists {
		c.blockOrder = append(c.blockOrder, rootKey)
	}
	c.blocks[rootKey] = block
	c.evictBlocks()

	for _, record := range records {
		if record == nil {
			continue
		}
		stateKey := record.StateID.String()
		c.recordOrder = append(c.recordOrder, recordRef{key: stateKey, record: record})
		c.records[stateKey] = record
	}
	c.evictRecords()
}

func (c *proofMetadataCache) get(stateID api.StateID, rootHash api.HexBytes) (*models.Block, *models.AggregatorRecord, bool) {
	if c == nil {
		return nil, nil, false
	}

	block := c.blocks[rootHash.String()]
	if block == nil {
		return nil, nil, false
	}

	record := c.records[stateID.String()]
	if record == nil {
		return nil, nil, false
	}
	if record.BlockNumber == nil || block.Index == nil || record.BlockNumber.Cmp(block.Index.Int) > 0 {
		return nil, nil, false
	}
	return block, record, true
}

func (c *proofMetadataCache) stats() (records int, blocks int) {
	if c == nil {
		return 0, 0
	}
	return len(c.records), len(c.blocks)
}

func (c *proofMetadataCache) evictRecords() {
	for len(c.records) > c.maxRecords && c.recordHead < len(c.recordOrder) {
		ref := c.recordOrder[c.recordHead]
		c.recordHead++
		if active, exists := c.records[ref.key]; exists && active == ref.record {
			delete(c.records, ref.key)
		}
	}
	c.compactRecordOrder()
}

func (c *proofMetadataCache) evictBlocks() {
	for len(c.blocks) > maxProofMetadataCacheBlocks && c.blockHead < len(c.blockOrder) {
		key := c.blockOrder[c.blockHead]
		c.blockHead++
		delete(c.blocks, key)
	}
	c.compactBlockOrder()
}

func (c *proofMetadataCache) compactRecordOrder() {
	if c.recordHead > 4096 && c.recordHead*2 > len(c.recordOrder) {
		copy(c.recordOrder, c.recordOrder[c.recordHead:])
		c.recordOrder = c.recordOrder[:len(c.recordOrder)-c.recordHead]
		c.recordHead = 0
	}
}

func (c *proofMetadataCache) compactBlockOrder() {
	if c.blockHead > 1024 && c.blockHead*2 > len(c.blockOrder) {
		copy(c.blockOrder, c.blockOrder[c.blockHead:])
		c.blockOrder = c.blockOrder[:len(c.blockOrder)-c.blockHead]
		c.blockHead = 0
	}
}
