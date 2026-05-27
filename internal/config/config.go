package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	cmdbft "github.com/unicitynetwork/bft-core/cli/ubft/cmd"

	"github.com/unicitynetwork/bft-core/network"
	"github.com/unicitynetwork/bft-go-base/types"
	"github.com/unicitynetwork/bft-go-base/util"

	"github.com/unicitynetwork/aggregator-go/pkg/api"
)

// Config represents the application configuration
type Config struct {
	Server     ServerConfig     `mapstructure:"server"`
	Database   DatabaseConfig   `mapstructure:"database"`
	Redis      RedisConfig      `mapstructure:"redis"`
	Storage    StorageConfig    `mapstructure:"storage"`
	SMT        SMTConfig        `mapstructure:"smt"`
	HA         HAConfig         `mapstructure:"ha"`
	Logging    LoggingConfig    `mapstructure:"logging"`
	BFT        BFTConfig        `mapstructure:"bft"`
	Processing ProcessingConfig `mapstructure:"processing"`
	Sharding   ShardingConfig   `mapstructure:"sharding"`
	Chain      ChainConfig      `mapstructure:"chain"`
	Signing    SigningConfig    `mapstructure:"signing"`
}

// SigningConfig holds the aggregator's signing key configuration
type SigningConfig struct {
	KeyFile string          `mapstructure:"key_file"`
	KeyConf *cmdbft.KeyConf `mapstructure:"key_conf"`
}

// ChainConfig holds metadata about the current chain configuration
type ChainConfig struct {
	ID      string `mapstructure:"id"`
	Version string `mapstructure:"version"`
	ForkID  string `mapstructure:"fork_id"`
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	Port                      string        `mapstructure:"port"`
	Host                      string        `mapstructure:"host"`
	ReadTimeout               time.Duration `mapstructure:"read_timeout"`
	WriteTimeout              time.Duration `mapstructure:"write_timeout"`
	IdleTimeout               time.Duration `mapstructure:"idle_timeout"`
	ConcurrencyLimit          int           `mapstructure:"concurrency_limit"`
	EnableDocs                bool          `mapstructure:"enable_docs"`
	EnableCORS                bool          `mapstructure:"enable_cors"`
	TLSCertFile               string        `mapstructure:"tls_cert_file"`
	TLSKeyFile                string        `mapstructure:"tls_key_file"`
	EnableTLS                 bool          `mapstructure:"enable_tls"`
	EnableH2C                 bool          `mapstructure:"enable_h2c"`
	HTTP2MaxConcurrentStreams int           `mapstructure:"http2_max_concurrent_streams"`
}

// DatabaseConfig holds MongoDB configuration
type DatabaseConfig struct {
	URI                    string        `mapstructure:"uri"`
	Database               string        `mapstructure:"database"`
	ConnectTimeout         time.Duration `mapstructure:"connect_timeout"`
	ServerSelectionTimeout time.Duration `mapstructure:"server_selection_timeout"`
	SocketTimeout          time.Duration `mapstructure:"socket_timeout"`
	MaxPoolSize            uint64        `mapstructure:"max_pool_size"`
	MinPoolSize            uint64        `mapstructure:"min_pool_size"`
	MaxConnIdleTime        time.Duration `mapstructure:"max_conn_idle_time"`
	// Optional finalization insert chunking. A zero chunk size keeps the
	// existing single InsertMany behavior.
	FinalizationInsertChunkSize    int `mapstructure:"finalization_insert_chunk_size"`
	FinalizationInsertChunkWorkers int `mapstructure:"finalization_insert_chunk_workers"`
}

// HAConfig holds High Availability configuration
type HAConfig struct {
	Enabled                       bool          `mapstructure:"enabled"`
	LockTTLSeconds                int           `mapstructure:"lock_ttl_seconds"`
	LeaderHeartbeatInterval       time.Duration `mapstructure:"leader_heartbeat_interval"`
	LeaderElectionPollingInterval time.Duration `mapstructure:"leader_election_polling_interval"`
	LockID                        string        `mapstructure:"lock_id"`
	ServerID                      string        `mapstructure:"server_id"`
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level           string `mapstructure:"level"`
	Format          string `mapstructure:"format"`
	Output          string `mapstructure:"output"`
	EnableJSON      bool   `mapstructure:"enable_json"`
	EnableAsync     bool   `mapstructure:"enable_async"`
	AsyncBufferSize int    `mapstructure:"async_buffer_size"`
	// File logging with rotation
	FilePath        string `mapstructure:"file_path"`        // Path to log file (empty = no file logging)
	MaxSizeMB       int    `mapstructure:"max_size_mb"`      // Max size in MB before rotation (default 100)
	MaxBackups      int    `mapstructure:"max_backups"`      // Max number of old log files to retain (default 30)
	MaxAgeDays      int    `mapstructure:"max_age_days"`     // Max days to retain old log files (default 30)
	CompressBackups bool   `mapstructure:"compress_backups"` // Compress rotated log files (default true)
}

// ProcessingConfig holds batch processing configuration
type ProcessingConfig struct {
	BatchLimit                 int           `mapstructure:"batch_limit"`
	RoundDuration              time.Duration `mapstructure:"round_duration"`
	PrecollectorGracePeriod    time.Duration `mapstructure:"precollector_grace_period"`     // Extra wait before cutting a precollected round snapshot
	MaxCommitmentsPerRound     int           `mapstructure:"max_commitments_per_round"`     // Stop waiting once this many commitments collected
	CollectPhaseDuration       time.Duration `mapstructure:"collect_phase_duration"`        // Non-child fixed collection window before proposing a round
	CommitmentStreamBufferSize int           `mapstructure:"commitment_stream_buffer_size"` // Buffer between queue streamer and round collection
	SkipDuplicateCheck         bool          `mapstructure:"skip_duplicate_check"`          // Skip finalized record lookup on submit
}

// RedisConfig holds Redis connection configuration
type RedisConfig struct {
	Host             string        `mapstructure:"host"`
	Port             int           `mapstructure:"port"`
	Password         string        `mapstructure:"password"`
	DB               int           `mapstructure:"db"`
	PoolSize         int           `mapstructure:"pool_size"`
	MinIdleConns     int           `mapstructure:"min_idle_conns"`
	MaxRetries       int           `mapstructure:"max_retries"`
	DialTimeout      time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout      time.Duration `mapstructure:"read_timeout"`
	WriteTimeout     time.Duration `mapstructure:"write_timeout"`
	SentinelAddrs    []string      `mapstructure:"sentinel_addrs"`
	MasterName       string        `mapstructure:"master_name"`
	SentinelPassword string        `mapstructure:"sentinel_password"`
	SentinelUsername string        `mapstructure:"sentinel_username"`
}

// StorageConfig holds storage layer configuration
type StorageConfig struct {
	UseRedisForCommitments bool          `mapstructure:"use_redis_for_commitments"`
	RedisStreamName        string        `mapstructure:"redis_stream_name"`
	RedisFlushInterval     time.Duration `mapstructure:"redis_flush_interval"`
	RedisMaxBatchSize      int           `mapstructure:"redis_max_batch_size"`
	RedisCleanupInterval   time.Duration `mapstructure:"redis_cleanup_interval"`
	RedisMaxStreamLength   int64         `mapstructure:"redis_max_stream_length"`
}

type SMTBackend string

const (
	SMTBackendMemory  SMTBackend = "memory"
	SMTBackendRocksDB SMTBackend = "rocksdb"
)

func (b SMTBackend) OrDefault() SMTBackend {
	if b == "" {
		return SMTBackendMemory
	}
	return b
}

func (b SMTBackend) IsValid() bool {
	switch b.OrDefault() {
	case SMTBackendMemory, SMTBackendRocksDB:
		return true
	default:
		return false
	}
}

type SMTConfig struct {
	Backend                  SMTBackend `mapstructure:"backend"`
	DiskPath                 string     `mapstructure:"disk_path"`
	RocksDBCacheMB           int        `mapstructure:"rocksdb_cache_mb"`
	RocksDBBGJobs            int        `mapstructure:"rocksdb_bg_jobs"`
	RocksDBSubcompactions    int        `mapstructure:"rocksdb_subcompactions"`
	RocksDBBloomBits         float64    `mapstructure:"rocksdb_bloom_bits"`
	RocksDBMemTableMB        int        `mapstructure:"rocksdb_memtable_mb"`
	MaterializeWorkers       int        `mapstructure:"materialize_workers"`
	StartupReplayLimitBlocks int        `mapstructure:"startup_replay_limit_blocks"`
}

// ShardingMode represents the aggregator operating mode
type ShardingMode string

const (
	ShardingModeStandalone ShardingMode = "standalone"
	ShardingModeParent     ShardingMode = "parent"
	ShardingModeChild      ShardingMode = "child"
	// ShardingModeBFTShard runs the aggregator as one shard validator in a
	// multi-shard BFT partition.
	ShardingModeBFTShard ShardingMode = "bft-shard"
)

// String returns the string representation of the sharding mode
func (sm ShardingMode) String() string {
	return string(sm)
}

// IsValid returns true if the sharding mode is valid
func (sm ShardingMode) IsValid() bool {
	switch sm {
	case ShardingModeStandalone, ShardingModeParent, ShardingModeChild, ShardingModeBFTShard:
		return true
	default:
		return false
	}
}

// IsStandalone returns true if this is standalone mode
func (sm ShardingMode) IsStandalone() bool {
	return sm == ShardingModeStandalone
}

// IsParent returns true if this is parent mode
func (sm ShardingMode) IsParent() bool {
	return sm == ShardingModeParent
}

// IsChild returns true if this is child mode
func (sm ShardingMode) IsChild() bool {
	return sm == ShardingModeChild
}

// IsBFTShard returns true if this is BFT-side sharding mode
func (sm ShardingMode) IsBFTShard() bool {
	return sm == ShardingModeBFTShard
}

// ShardingConfig holds sharding configuration
type ShardingConfig struct {
	Mode                       ShardingMode  `mapstructure:"mode"`                          // Operating mode: standalone, parent, or child
	ShardIDLength              int           `mapstructure:"shard_id_length"`               // Bit length for shard IDs (e.g., 4 bits = 16 shards)
	ParentCollectPhaseDuration time.Duration `mapstructure:"parent_collect_phase_duration"` // Collection window for parent mode before sending to BFT
	Child                      ChildConfig   `mapstructure:"child"`                         // child aggregator config
}

type ChildConfig struct {
	ParentRpcAddr      string        `mapstructure:"parent_rpc_addr"`
	ShardID            api.ShardID   `mapstructure:"shard_id"`
	ParentPollTimeout  time.Duration `mapstructure:"parent_poll_timeout"`
	ParentPollInterval time.Duration `mapstructure:"parent_poll_interval"`
}

func (c ShardingConfig) Validate() error {
	if c.Mode == ShardingModeChild {
		if err := c.Child.Validate(); err != nil {
			return fmt.Errorf("invalid child mode configuration: %w", err)
		}
	}
	if c.Mode == ShardingModeStandalone {
		if c.Child.ShardID != 0 {
			return errors.New("shard_id must be undefined in standalone mode")
		}
	}
	return nil
}

func (c ChildConfig) Validate() error {
	if c.ParentRpcAddr == "" {
		return errors.New("parent rpc addr is required")
	}
	if c.ShardID <= 1 {
		return errors.New("shard ID must be positive and have at least 2 bits")
	}
	if c.ParentPollTimeout == 0 {
		return errors.New("parent poll timeout is required")
	}
	if c.ParentPollInterval == 0 {
		return errors.New("parent poll interval is required")
	}
	return nil
}

type BFTConfig struct {
	Enabled    bool                              `mapstructure:"enabled"`
	KeyConf    *cmdbft.KeyConf                   `mapstructure:"key_conf"`
	ShardConf  *types.PartitionDescriptionRecord `mapstructure:"shard_conf"`
	TrustBases []types.RootTrustBaseV1           `mapstructure:"trust_bases"`

	// Peer configuration
	Address                    string        `mapstructure:"address"`
	AnnounceAddresses          []string      `mapstructure:"announce_addresses"`
	BootstrapAddresses         []string      `mapstructure:"bootstrap_addresses"`
	BootstrapConnectRetry      int           `mapstructure:"bootstrap_connect_retry"`
	BootstrapConnectRetryDelay int           `mapstructure:"bootstrap_connect_retry_delay"`
	StubDelay                  time.Duration `mapstructure:"stub_delay"` // Delay for BFT stub (testing only)

	// HeartbeatInterval how often to perform the inactivity check.
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
	// InactivityTimeout duration of inactivity after which a new handshake must be sent.
	InactivityTimeout time.Duration `mapstructure:"inactivity_timeout"`

	// BFT node REST api address
	RPCAddress string `mapstructure:"rpc_address"`
}

func (c *BFTConfig) Validate() error {
	if c.Enabled {
		if len(c.BootstrapAddresses) == 0 {
			return errors.New("BFT_BOOTSTRAP_ADDRESSES must be specified when BFT is enabled")
		}
		for _, addr := range c.BootstrapAddresses {
			if _, err := peer.AddrInfoFromString(addr); err != nil {
				return fmt.Errorf("invalid bootstrap address: %w", err)
			}
		}
	}
	return nil
}

// Load loads configuration from environment variables with defaults
func Load() (*Config, error) {
	config := &Config{
		Chain: ChainConfig{
			ID:      getEnvOrDefault("CHAIN_ID", "unicity"),
			Version: getEnvOrDefault("CHAIN_VERSION", "1.0"),
			ForkID:  getEnvOrDefault("CHAIN_FORK_ID", "testnet"),
		},
		Server: ServerConfig{
			Port:                      getEnvOrDefault("PORT", "3000"),
			Host:                      getEnvOrDefault("HOST", "0.0.0.0"),
			ReadTimeout:               getEnvDurationOrDefault("READ_TIMEOUT", "30s"),
			WriteTimeout:              getEnvDurationOrDefault("WRITE_TIMEOUT", "30s"),
			IdleTimeout:               getEnvDurationOrDefault("IDLE_TIMEOUT", "120s"),
			ConcurrencyLimit:          getEnvIntOrDefault("CONCURRENCY_LIMIT", 1000),
			EnableDocs:                getEnvBoolOrDefault("ENABLE_DOCS", true),
			EnableCORS:                getEnvBoolOrDefault("ENABLE_CORS", true),
			TLSCertFile:               getEnvOrDefault("TLS_CERT_FILE", ""),
			TLSKeyFile:                getEnvOrDefault("TLS_KEY_FILE", ""),
			EnableTLS:                 getEnvBoolOrDefault("ENABLE_TLS", false),
			EnableH2C:                 getEnvBoolOrDefault("ENABLE_H2C", true),
			HTTP2MaxConcurrentStreams: getEnvIntOrDefault("HTTP2_MAX_CONCURRENT_STREAMS", 4096),
		},
		Database: DatabaseConfig{
			URI:                    getEnvOrDefault("MONGODB_URI", "mongodb://localhost:27017"),
			Database:               getEnvOrDefault("MONGODB_DATABASE", "aggregator"),
			ConnectTimeout:         getEnvDurationOrDefault("MONGODB_CONNECT_TIMEOUT", "10s"),
			ServerSelectionTimeout: getEnvDurationOrDefault("MONGODB_SERVER_SELECTION_TIMEOUT", "5s"),
			SocketTimeout:          getEnvDurationOrDefault("MONGODB_SOCKET_TIMEOUT", "30s"),
			MaxPoolSize:            uint64(getEnvIntOrDefault("MONGODB_MAX_POOL_SIZE", 100)),
			MinPoolSize:            uint64(getEnvIntOrDefault("MONGODB_MIN_POOL_SIZE", 5)),
			MaxConnIdleTime:        getEnvDurationOrDefault("MONGODB_MAX_CONN_IDLE_TIME", "5m"),
			FinalizationInsertChunkSize: getEnvIntOrDefault(
				"MONGODB_FINALIZATION_INSERT_CHUNK_SIZE", 0),
			FinalizationInsertChunkWorkers: getEnvIntOrDefault(
				"MONGODB_FINALIZATION_INSERT_CHUNK_WORKERS", 1),
		},
		HA: HAConfig{
			Enabled:                       !getEnvBoolOrDefault("DISABLE_HIGH_AVAILABILITY", false),
			LockTTLSeconds:                getEnvIntOrDefault("LOCK_TTL_SECONDS", 30),
			LeaderHeartbeatInterval:       getEnvDurationOrDefault("LEADER_HEARTBEAT_INTERVAL", "10s"),
			LeaderElectionPollingInterval: getEnvDurationOrDefault("LEADER_ELECTION_POLLING_INTERVAL", "5s"),
			LockID:                        getEnvOrDefault("LOCK_ID", "aggregator_leader_lock"),
			ServerID:                      getEnvOrDefault("SERVER_ID", generateServerID()),
		},
		Logging: LoggingConfig{
			Level:           getEnvOrDefault("LOG_LEVEL", "info"),
			Format:          getEnvOrDefault("LOG_FORMAT", "json"),
			Output:          getEnvOrDefault("LOG_OUTPUT", "stdout"),
			EnableJSON:      getEnvBoolOrDefault("LOG_ENABLE_JSON", true),
			EnableAsync:     getEnvBoolOrDefault("LOG_ENABLE_ASYNC", true),
			AsyncBufferSize: getEnvIntOrDefault("LOG_ASYNC_BUFFER_SIZE", 10000),
			// File logging with rotation
			FilePath:        getEnvOrDefault("LOG_FILE_PATH", ""),
			MaxSizeMB:       getEnvIntOrDefault("LOG_MAX_SIZE_MB", 100),
			MaxBackups:      getEnvIntOrDefault("LOG_MAX_BACKUPS", 30),
			MaxAgeDays:      getEnvIntOrDefault("LOG_MAX_AGE_DAYS", 30),
			CompressBackups: getEnvBoolOrDefault("LOG_COMPRESS_BACKUPS", true),
		},
		Processing: ProcessingConfig{
			BatchLimit:                 getEnvIntOrDefault("BATCH_LIMIT", 1000),
			RoundDuration:              getEnvDurationOrDefault("ROUND_DURATION", "1s"),
			PrecollectorGracePeriod:    getEnvDurationOrDefault("PRECOLLECTOR_GRACE_PERIOD", "0s"),
			MaxCommitmentsPerRound:     getEnvIntOrDefault("MAX_COMMITMENTS_PER_ROUND", 20000),
			CollectPhaseDuration:       getEnvDurationOrDefault("COLLECT_PHASE_DURATION", "200ms"),
			CommitmentStreamBufferSize: getEnvIntOrDefault("COMMITMENT_STREAM_BUFFER_SIZE", 50000),
			SkipDuplicateCheck:         getEnvBoolOrDefault("SKIP_DUPLICATE_CHECK", true),
		},
		Redis: RedisConfig{
			Host:             getEnvOrDefault("REDIS_HOST", "localhost"),
			Port:             getEnvIntOrDefault("REDIS_PORT", 6379),
			Password:         getEnvOrDefault("REDIS_PASSWORD", ""),
			DB:               getEnvIntOrDefault("REDIS_DB", 0),
			PoolSize:         getEnvIntOrDefault("REDIS_POOL_SIZE", 10),
			MinIdleConns:     getEnvIntOrDefault("REDIS_MIN_IDLE_CONNS", 2),
			MaxRetries:       getEnvIntOrDefault("REDIS_MAX_RETRIES", 3),
			DialTimeout:      getEnvDurationOrDefault("REDIS_DIAL_TIMEOUT", "5s"),
			ReadTimeout:      getEnvDurationOrDefault("REDIS_READ_TIMEOUT", "3s"),
			WriteTimeout:     getEnvDurationOrDefault("REDIS_WRITE_TIMEOUT", "3s"),
			SentinelAddrs:    getEnvStringSliceOrDefault("REDIS_SENTINEL_ADDRS", nil),
			MasterName:       getEnvOrDefault("REDIS_MASTER_NAME", ""),
			SentinelPassword: getEnvOrDefault("REDIS_SENTINEL_PASSWORD", ""),
			SentinelUsername: getEnvOrDefault("REDIS_SENTINEL_USERNAME", ""),
		},
		Storage: StorageConfig{
			UseRedisForCommitments: getEnvBoolOrDefault("USE_REDIS_FOR_COMMITMENTS", false),
			RedisStreamName:        getEnvOrDefault("REDIS_STREAM_NAME", "commitments"),
			RedisFlushInterval:     getEnvDurationOrDefault("REDIS_FLUSH_INTERVAL", "100ms"),
			RedisMaxBatchSize:      getEnvIntOrDefault("REDIS_MAX_BATCH_SIZE", 5000),
			RedisCleanupInterval:   getEnvDurationOrDefault("REDIS_CLEANUP_INTERVAL", "5m"),
			RedisMaxStreamLength:   int64(getEnvIntOrDefault("REDIS_MAX_STREAM_LENGTH", 1000000)),
		},
		SMT: SMTConfig{
			Backend:                  SMTBackend(getEnvOrDefault("SMT_BACKEND", string(SMTBackendMemory))),
			DiskPath:                 getEnvOrDefault("SMT_DISK_PATH", ""),
			RocksDBCacheMB:           getEnvIntOrDefault("SMT_ROCKSDB_CACHE_MB", 1024),
			RocksDBBGJobs:            getEnvIntOrDefault("SMT_ROCKSDB_BG_JOBS", 8),
			RocksDBSubcompactions:    getEnvIntOrDefault("SMT_ROCKSDB_SUBCOMPACTIONS", 4),
			RocksDBBloomBits:         getEnvFloatOrDefault("SMT_ROCKSDB_BLOOM_BITS", 10),
			RocksDBMemTableMB:        getEnvIntOrDefault("SMT_ROCKSDB_MEMTABLE_MB", 64),
			MaterializeWorkers:       getEnvIntOrDefault("SMT_MATERIALIZE_WORKERS", 64),
			StartupReplayLimitBlocks: getEnvIntOrDefault("SMT_STARTUP_REPLAY_LIMIT_BLOCKS", 100),
		},
		Sharding: ShardingConfig{
			Mode:                       ShardingMode(getEnvOrDefault("SHARDING_MODE", "standalone")),
			ShardIDLength:              getEnvIntOrDefault("SHARD_ID_LENGTH", 4),
			ParentCollectPhaseDuration: getEnvDurationOrDefault("PARENT_COLLECT_PHASE_DURATION", "200ms"),
			Child: ChildConfig{
				ParentRpcAddr:      getEnvOrDefault("SHARDING_CHILD_PARENT_RPC_ADDR", "http://localhost:3009"),
				ShardID:            getEnvIntOrDefault("SHARDING_CHILD_SHARD_ID", 0),
				ParentPollTimeout:  getEnvDurationOrDefault("SHARDING_CHILD_PARENT_POLL_TIMEOUT", "5s"),
				ParentPollInterval: getEnvDurationOrDefault("SHARDING_CHILD_PARENT_POLL_INTERVAL", "100ms"),
			},
		},
	}
	config.Signing = SigningConfig{
		KeyFile: getEnvOrDefault("SIGNING_KEY_FILE", ""),
	}
	// Only load signing key if a file is specified
	if config.Signing.KeyFile != "" {
		if err := loadConf(config.Signing.KeyFile, &config.Signing.KeyConf); err != nil {
			return nil, fmt.Errorf("failed to load signing key configuration from %s: %w", config.Signing.KeyFile, err)
		}
	}

	config.BFT = BFTConfig{
		Enabled:                    getEnvBoolOrDefault("BFT_ENABLED", true),
		Address:                    getEnvOrDefault("BFT_ADDRESS", "/ip4/0.0.0.0/tcp/9000"),
		RPCAddress:                 getEnvOrDefault("BFT_RPC_ADDRESS", "http://127.0.0.1:8002"),
		AnnounceAddresses:          strings.Split(getEnvOrDefault("BFT_ANNOUNCE_ADDRESSES", ""), ","),
		BootstrapAddresses:         strings.Split(getEnvOrDefault("BFT_BOOTSTRAP_ADDRESSES", ""), ","),
		BootstrapConnectRetry:      getEnvIntOrDefault("BFT_BOOTSTRAP_CONNECT_RETRY", 3),
		BootstrapConnectRetryDelay: getEnvIntOrDefault("BFT_BOOTSTRAP_CONNECT_RETRY_DELAY", 5),
		HeartbeatInterval:          getEnvDurationOrDefault("BFT_HEARTBEAT_INTERVAL", "1s"),
		InactivityTimeout:          getEnvDurationOrDefault("BFT_INACTIVITY_TIMEOUT", "5s"),
	}
	if config.BFT.Enabled {
		config.BFT.KeyConf = config.Signing.KeyConf
		if err := loadConf(getEnvOrDefault("BFT_SHARD_CONF_FILE", "bft-config/shard-conf-7_0.json"), &config.BFT.ShardConf); err != nil {
			return nil, fmt.Errorf("failed to load shard configuration: %w", err)
		}
		trustBaseFiles := strings.Split(getEnvOrDefault("BFT_TRUST_BASE_FILES", "bft-config/trust-base.json"), ",")
		config.BFT.TrustBases = make([]types.RootTrustBaseV1, 0, len(trustBaseFiles))
		for _, file := range trustBaseFiles {
			trustBaseV1 := types.RootTrustBaseV1{}
			if err := loadConf(file, &trustBaseV1); err != nil {
				return nil, fmt.Errorf("failed to load trust base configuration from %s: %w", file, err)
			}
			config.BFT.TrustBases = append(config.BFT.TrustBases, trustBaseV1)
		}
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return config, nil
}

func loadConf(path string, conf any) error {
	if _, err := util.ReadJsonFile(path, &conf); err != nil {
		return fmt.Errorf("failed to load %q: %w", path, err)
	}
	return nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Server.Port == "" {
		return fmt.Errorf("server port cannot be empty")
	}

	if c.Database.URI == "" {
		return fmt.Errorf("database URI cannot be empty")
	}

	if c.Database.Database == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	if c.HA.Enabled && c.HA.ServerID == "" {
		return fmt.Errorf("server ID cannot be empty when HA is enabled")
	}

	if c.Server.EnableTLS && (c.Server.TLSCertFile == "" || c.Server.TLSKeyFile == "") {
		return fmt.Errorf("TLS cert and key files must be provided when TLS is enabled")
	}
	if c.Server.HTTP2MaxConcurrentStreams <= 0 {
		return fmt.Errorf("HTTP/2 max concurrent streams must be positive")
	}
	if c.Processing.CommitmentStreamBufferSize <= 0 {
		return fmt.Errorf("COMMITMENT_STREAM_BUFFER_SIZE must be positive")
	}
	if c.Processing.CollectPhaseDuration <= 0 {
		return fmt.Errorf("COLLECT_PHASE_DURATION must be positive")
	}
	if c.Processing.PrecollectorGracePeriod < 0 {
		return fmt.Errorf("PRECOLLECTOR_GRACE_PERIOD must be non-negative")
	}
	if c.Database.FinalizationInsertChunkSize < 0 {
		return fmt.Errorf("MONGODB_FINALIZATION_INSERT_CHUNK_SIZE must be non-negative")
	}
	if c.Database.FinalizationInsertChunkSize > 0 && c.Database.FinalizationInsertChunkWorkers <= 0 {
		return fmt.Errorf("MONGODB_FINALIZATION_INSERT_CHUNK_WORKERS must be positive when chunking is enabled")
	}
	if !c.SMT.Backend.IsValid() {
		return fmt.Errorf("invalid SMT_BACKEND: %s, must be one of: memory, rocksdb", c.SMT.Backend)
	}
	if c.SMT.RocksDBCacheMB < 0 {
		return fmt.Errorf("SMT_ROCKSDB_CACHE_MB must be non-negative")
	}
	if c.SMT.RocksDBBGJobs < 0 {
		return fmt.Errorf("SMT_ROCKSDB_BG_JOBS must be non-negative")
	}
	if c.SMT.RocksDBSubcompactions < 0 {
		return fmt.Errorf("SMT_ROCKSDB_SUBCOMPACTIONS must be non-negative")
	}
	if c.SMT.RocksDBBloomBits < 0 {
		return fmt.Errorf("SMT_ROCKSDB_BLOOM_BITS must be non-negative")
	}
	if c.SMT.RocksDBMemTableMB < 0 {
		return fmt.Errorf("SMT_ROCKSDB_MEMTABLE_MB must be non-negative")
	}
	if c.SMT.MaterializeWorkers < 0 {
		return fmt.Errorf("SMT_MATERIALIZE_WORKERS must be non-negative")
	}
	if c.SMT.StartupReplayLimitBlocks < 0 {
		return fmt.Errorf("SMT_STARTUP_REPLAY_LIMIT_BLOCKS must be non-negative")
	}
	if c.SMT.Backend.OrDefault() == SMTBackendRocksDB {
		if c.SMT.DiskPath == "" {
			return fmt.Errorf("SMT_DISK_PATH is required when SMT_BACKEND=rocksdb")
		}
		if c.HA.Enabled {
			return fmt.Errorf("SMT_BACKEND=rocksdb is not supported with HA enabled yet; see docs/disk-backed-smt-ha-replication.md")
		}
		if c.Sharding.Mode == ShardingModeParent || c.Sharding.Mode == ShardingModeChild {
			return fmt.Errorf("SMT_BACKEND=rocksdb is not supported with SHARDING_MODE=%s in this phase", c.Sharding.Mode)
		}
	}

	// Validate log level
	validLevels := []string{"debug", "info", "warn", "error", "fatal", "panic"}
	if !contains(validLevels, strings.ToLower(c.Logging.Level)) {
		return fmt.Errorf("invalid log level: %s", c.Logging.Level)
	}

	// Validate sharding configuration
	if !c.Sharding.Mode.IsValid() {
		return fmt.Errorf("invalid sharding mode: %s, must be one of: standalone, parent, child, bft-shard", c.Sharding.Mode)
	}

	if c.Sharding.ShardIDLength < 1 || c.Sharding.ShardIDLength > 16 {
		return fmt.Errorf("shard ID length must be between 1 and 16 bits, got: %d", c.Sharding.ShardIDLength)
	}

	if err := c.Sharding.Validate(); err != nil {
		return fmt.Errorf("invalid sharding configuration: %w", err)
	}

	if err := c.BFT.Validate(); err != nil {
		return err
	}

	if c.Sharding.Mode == ShardingModeBFTShard {
		if !c.BFT.Enabled {
			return errors.New("bft-shard mode requires BFT_ENABLED=true")
		}
		if c.BFT.ShardConf == nil {
			return errors.New("bft-shard mode requires BFT_SHARD_CONF_FILE")
		}
		if c.BFT.ShardConf.ShardID.Length() == 0 {
			return errors.New("bft-shard mode requires a non-empty shard ID; use standalone for single-shard partitions")
		}
		if c.Sharding.Child.ShardID != 0 {
			return fmt.Errorf("bft-shard mode must not set SHARDING_CHILD_SHARD_ID (got %d); it is an app-sharding field and has no meaning in bft-shard", c.Sharding.Child.ShardID)
		}
	}

	// A non-empty BFT shard ID means a multi-shard partition; only bft-shard
	// mode can admit/reject state IDs against it.
	if c.BFT.Enabled && c.BFT.ShardConf != nil && c.BFT.ShardConf.ShardID.Length() > 0 &&
		c.Sharding.Mode != ShardingModeBFTShard {
		return fmt.Errorf("BFT shard conf has a non-empty shard ID but SHARDING_MODE=%s; use SHARDING_MODE=bft-shard for multi-shard BFT partitions",
			c.Sharding.Mode)
	}

	return nil
}

// Helper functions for environment variable parsing
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvIntOrDefault(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvBoolOrDefault(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}

func getEnvFloatOrDefault(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return floatVal
		}
	}
	return defaultValue
}

func getEnvDurationOrDefault(key string, defaultValue string) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	duration, _ := time.ParseDuration(defaultValue)
	return duration
}

func getEnvStringSliceOrDefault(key string, defaultValue []string) []string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		return defaultValue
	}
	return result
}

func generateServerID() string {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func (c *BFTConfig) PeerConf() (*network.PeerConfiguration, error) {
	authKeyPair, err := c.KeyConf.AuthKeyPair()
	if err != nil {
		return nil, fmt.Errorf("invalid authentication key: %w", err)
	}

	bootNodes := make([]peer.AddrInfo, len(c.BootstrapAddresses))
	for i, addr := range c.BootstrapAddresses {
		addrInfo, err := peer.AddrInfoFromString(addr)
		if err != nil {
			return nil, fmt.Errorf("invalid bootstrap address: %w", err)
		}
		bootNodes[i] = *addrInfo
	}

	bootstrapConnectRetry := &network.BootstrapConnectRetry{Count: c.BootstrapConnectRetry, Delay: c.BootstrapConnectRetryDelay}
	if len(c.AnnounceAddresses) == 1 && c.AnnounceAddresses[0] == "" {
		c.AnnounceAddresses = nil
	}

	return network.NewPeerConfiguration(c.Address, c.AnnounceAddresses, authKeyPair, bootNodes, bootstrapConnectRetry)
}
