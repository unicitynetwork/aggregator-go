package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/unicitynetwork/aggregator-go/internal/config"
)

func main() {
	os.Setenv("BFT_ENABLED", "false")
	c, err := config.Load()
	if err != nil {
		fmt.Println("ERR:", err)
		os.Exit(1)
	}
	fmt.Printf("Chain.ForkID=%q Chain.ID=%q Chain.Version=%q\n", c.Chain.ForkID, c.Chain.ID, c.Chain.Version)
	fmt.Printf("Server.EnableH2C=%v Server.HTTP2MaxConcurrentStreams=%d\n", c.Server.EnableH2C, c.Server.HTTP2MaxConcurrentStreams)
	fmt.Printf("DB.FinalizationInsertChunkSize=%d Workers=%d\n", c.Database.FinalizationInsertChunkSize, c.Database.FinalizationInsertChunkWorkers)
	fmt.Printf("Log.FilePath=%q MaxSizeMB=%d MaxBackups=%d MaxAgeDays=%d Compress=%v\n", c.Logging.FilePath, c.Logging.MaxSizeMB, c.Logging.MaxBackups, c.Logging.MaxAgeDays, c.Logging.CompressBackups)
	fmt.Printf("Proc.BatchLimit=%d MaxCommitmentsPerRound=%d CollectPhase=%s MiniBatch=%d StreamBuf=%d Grace=%s SkipDup=%v TTL=%s\n",
		c.Processing.BatchLimit, c.Processing.MaxCommitmentsPerRound, c.Processing.CollectPhaseDuration, c.Processing.CollectMiniBatchSize,
		c.Processing.CommitmentStreamBufferSize, c.Processing.PrecollectorGracePeriod, c.Processing.SkipDuplicateCheck, c.Processing.DefaultRequestTTL)
	fmt.Printf("Redis.PoolSize=%d MinIdle=%d MaxRetries=%d Dial=%s Read=%s Write=%s\n", c.Redis.PoolSize, c.Redis.MinIdleConns, c.Redis.MaxRetries, c.Redis.DialTimeout, c.Redis.ReadTimeout, c.Redis.WriteTimeout)
	fmt.Printf("Storage.AckBatch=%d DeleteAfterAck=%v Cleanup=%s MaxStreamLen=%d MaxBatch=%d Flush=%s\n", c.Storage.RedisAckBatchSize, c.Storage.RedisDeleteAfterAck, c.Storage.RedisCleanupInterval, c.Storage.RedisMaxStreamLength, c.Storage.RedisMaxBatchSize, c.Storage.RedisFlushInterval)
	fmt.Printf("SMT.PrecomputeProofs=%v ProofMetadataCacheEntries=%d MaterializeWorkers=%d\n", c.SMT.PrecomputeProofs, c.SMT.ProofMetadataCacheEntries, c.SMT.MaterializeWorkers)
	b, _ := json.MarshalIndent(c.Sharding, "", "  ")
	fmt.Printf("Sharding=%s\n", b)
	fmt.Printf("Signing.KeyFile=%q\n", c.Signing.KeyFile)
}
