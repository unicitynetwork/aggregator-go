//go:build rocksdb

package service

import "testing"

func TestProofStatusFlowDiskBackend(t *testing.T) {
	cfg := testDiskBFTShardServiceConfig(t)
	cfg.Database.Database = "test_proof_status_disk"
	proofStatusFlow(t, cfg)
}
