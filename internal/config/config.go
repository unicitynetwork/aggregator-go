package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/unicitynetwork/bft-core/network"
	"github.com/unicitynetwork/bft-core/partition"
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
	HA         HAConfig         `mapstructure:"ha"`
	Logging    LoggingConfig    `mapstructure:"logging"`
	BFT        BFTConfig        `mapstructure:"bft"`
	Processing ProcessingConfig `mapstructure:"processing"`
	Sharding   ShardingConfig   `mapstructure:"sharding"`
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	Port             string        `mapstructure:"port"`
	Host             string        `mapstructure:"host"`
	ReadTimeout      time.Duration `mapstructure:"read_timeout"`
	WriteTimeout     time.Duration `mapstructure:"write_timeout"`
	IdleTimeout      time.Duration `mapstructure:"idle_timeout"`
	ConcurrencyLimit int           `mapstructure:"concurrency_limit"`
	EnableDocs       bool          `mapstructure:"enable_docs"`
	EnableCORS       bool          `mapstructure:"enable_cors"`
	TLSCertFile      string        `mapstructure:"tls_cert_file"`
	TLSKeyFile       string        `mapstructure:"tls_key_file"`
	EnableTLS        bool          `mapstructure:"enable_tls"`
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
}

// ProcessingConfig holds batch processing configuration
type ProcessingConfig struct {
	BatchLimit             int           `mapstructure:"batch_limit"`
	RoundDuration          time.Duration `mapstructure:"round_duration"`
	MaxCommitmentsPerRound int           `mapstructure:"max_commitments_per_round"` // Stop waiting once this many commitments collected
}

// RedisConfig holds Redis connection configuration
type RedisConfig struct {
	Host         string        `mapstructure:"host"`
	Port         int           `mapstructure:"port"`
	Password     string        `mapstructure:"password"`
	DB           int           `mapstructure:"db"`
	PoolSize     int           `mapstructure:"pool_size"`
	MinIdleConns int           `mapstructure:"min_idle_conns"`
	MaxRetries   int           `mapstructure:"max_retries"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

// StorageConfig holds storage layer configuration
type StorageConfig struct {
	UseRedisForCommitments bool          `mapstructure:"use_redis_for_commitments"`
	RedisFlushInterval     time.Duration `mapstructure:"redis_flush_interval"`
	RedisMaxBatchSize      int           `mapstructure:"redis_max_batch_size"`
	RedisCleanupInterval   time.Duration `mapstructure:"redis_cleanup_interval"`
	RedisMaxStreamLength   int64         `mapstructure:"redis_max_stream_length"`
}

// ShardingMode represents the aggregator operating mode
type ShardingMode string

const (
	ShardingModeStandalone ShardingMode = "standalone"
	ShardingModeParent     ShardingMode = "parent"
	ShardingModeChild      ShardingMode = "child"
)

// String returns the string representation of the sharding mode
func (sm ShardingMode) String() string {
	return string(sm)
}

// IsValid returns true if the sharding mode is valid
func (sm ShardingMode) IsValid() bool {
	switch sm {
	case ShardingModeStandalone, ShardingModeParent, ShardingModeChild:
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

// ShardingConfig holds sharding configuration
type ShardingConfig struct {
	Mode          ShardingMode `mapstructure:"mode"`            // Operating mode: standalone, parent, or child
	ShardIDLength int          `mapstructure:"shard_id_length"` // Bit length for shard IDs (e.g., 4 bits = 16 shards)
	Child         ChildConfig  `mapstructure:"child"`           // child aggregator config
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
	if c.ShardID == 0 {
		return errors.New("shard id is required")
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
	Enabled   bool                              `mapstructure:"enabled"`
	KeyConf   *partition.KeyConf                `mapstructure:"key_conf"`
	ShardConf *types.PartitionDescriptionRecord `mapstructure:"shard_conf"`
	TrustBase types.RootTrustBase               `mapstructure:"trust_base"`
	// Peer configuration
	Address                    string   `mapstructure:"address"`
	AnnounceAddresses          []string `mapstructure:"announce_addresses"`
	BootstrapAddresses         []string `mapstructure:"bootstrap_addresses"`
	BootstrapConnectRetry      int      `mapstructure:"bootstrap_connect_retry"`
	BootstrapConnectRetryDelay int      `mapstructure:"bootstrap_connect_retry_delay"`
}

// Load loads configuration from environment variables with defaults
func Load() (*Config, error) {
	config := &Config{
		Server: ServerConfig{
			Port:             getEnvOrDefault("PORT", "3000"),
			Host:             getEnvOrDefault("HOST", "0.0.0.0"),
			ReadTimeout:      getEnvDurationOrDefault("READ_TIMEOUT", "30s"),
			WriteTimeout:     getEnvDurationOrDefault("WRITE_TIMEOUT", "30s"),
			IdleTimeout:      getEnvDurationOrDefault("IDLE_TIMEOUT", "120s"),
			ConcurrencyLimit: getEnvIntOrDefault("CONCURRENCY_LIMIT", 1000),
			EnableDocs:       getEnvBoolOrDefault("ENABLE_DOCS", true),
			EnableCORS:       getEnvBoolOrDefault("ENABLE_CORS", true),
			TLSCertFile:      getEnvOrDefault("TLS_CERT_FILE", ""),
			TLSKeyFile:       getEnvOrDefault("TLS_KEY_FILE", ""),
			EnableTLS:        getEnvBoolOrDefault("ENABLE_TLS", false),
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
		},
		Processing: ProcessingConfig{
			BatchLimit:             getEnvIntOrDefault("BATCH_LIMIT", 1000),
			RoundDuration:          getEnvDurationOrDefault("ROUND_DURATION", "1s"),
			MaxCommitmentsPerRound: getEnvIntOrDefault("MAX_COMMITMENTS_PER_ROUND", 10000), // Default 10k to keep rounds under 2s
		},
		Redis: RedisConfig{
			Host:         getEnvOrDefault("REDIS_HOST", "localhost"),
			Port:         getEnvIntOrDefault("REDIS_PORT", 6379),
			Password:     getEnvOrDefault("REDIS_PASSWORD", ""),
			DB:           getEnvIntOrDefault("REDIS_DB", 0),
			PoolSize:     getEnvIntOrDefault("REDIS_POOL_SIZE", 10),
			MinIdleConns: getEnvIntOrDefault("REDIS_MIN_IDLE_CONNS", 2),
			MaxRetries:   getEnvIntOrDefault("REDIS_MAX_RETRIES", 3),
			DialTimeout:  getEnvDurationOrDefault("REDIS_DIAL_TIMEOUT", "5s"),
			ReadTimeout:  getEnvDurationOrDefault("REDIS_READ_TIMEOUT", "3s"),
			WriteTimeout: getEnvDurationOrDefault("REDIS_WRITE_TIMEOUT", "3s"),
		},
		Storage: StorageConfig{
			UseRedisForCommitments: getEnvBoolOrDefault("USE_REDIS_FOR_COMMITMENTS", false),
			RedisFlushInterval:     getEnvDurationOrDefault("REDIS_FLUSH_INTERVAL", "100ms"),
			RedisMaxBatchSize:      getEnvIntOrDefault("REDIS_MAX_BATCH_SIZE", 5000),
			RedisCleanupInterval:   getEnvDurationOrDefault("REDIS_CLEANUP_INTERVAL", "5m"),
			RedisMaxStreamLength:   int64(getEnvIntOrDefault("REDIS_MAX_STREAM_LENGTH", 1000000)),
		},
		Sharding: ShardingConfig{
			Mode:          ShardingMode(getEnvOrDefault("SHARDING_MODE", "standalone")),
			ShardIDLength: getEnvIntOrDefault("SHARD_ID_LENGTH", 4),
			Child: ChildConfig{
				ParentRpcAddr:      getEnvOrDefault("SHARDING_CHILD_PARENT_RPC_ADDR", "http://localhost:3009"),
				ShardID:            getEnvIntOrDefault("SHARDING_CHILD_SHARD_ID", 0),
				ParentPollTimeout:  getEnvDurationOrDefault("SHARDING_CHILD_PARENT_POLL_TIMEOUT", "5s"),
				ParentPollInterval: getEnvDurationOrDefault("SHARDING_CHILD_PARENT_POLL_INTERVAL", "100ms"),
			},
		},
	}
	config.BFT = BFTConfig{
		Enabled:                    getEnvBoolOrDefault("BFT_ENABLED", true),
		Address:                    getEnvOrDefault("BFT_ADDRESS", "/ip4/0.0.0.0/tcp/9000"),
		AnnounceAddresses:          strings.Split(getEnvOrDefault("BFT_ANNOUNCE_ADDRESSES", ""), ","),
		BootstrapAddresses:         strings.Split(getEnvOrDefault("BFT_BOOTSTRAP_ADDRESSES", "/ip4/127.0.0.1/tcp/26662/p2p/16Uiu2HAm6eQMr2sQVbcWZsPPbpc2Su7AnnMVGHpC23PUzGTAATnp"), ","),
		BootstrapConnectRetry:      getEnvIntOrDefault("BFT_BOOTSTRAP_CONNECT_RETRY", 3),
		BootstrapConnectRetryDelay: getEnvIntOrDefault("BFT_BOOTSTRAP_CONNECT_RETRY_DELAY", 5),
	}
	if config.BFT.Enabled {
		if err := loadConf(getEnvOrDefault("BFT_KEY_CONF_FILE", "bft-config/keys.json"), &config.BFT.KeyConf); err != nil {
			return nil, fmt.Errorf("failed to load key configuration: %w", err)
		}
		if err := loadConf(getEnvOrDefault("BFT_SHARD_CONF_FILE", "bft-config/shard-conf-7_0.json"), &config.BFT.ShardConf); err != nil {
			return nil, fmt.Errorf("failed to load shard configuration: %w", err)
		}
		trustBaseV1 := types.RootTrustBaseV1{}
		if err := loadConf(getEnvOrDefault("BFT_TRUST_BASE_FILE", "bft-config/trust-base.json"), &trustBaseV1); err != nil {
			return nil, fmt.Errorf("failed to load trust base configuration: %w", err)
		}
		config.BFT.TrustBase = &trustBaseV1
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

	// Validate log level
	validLevels := []string{"debug", "info", "warn", "error", "fatal", "panic"}
	if !contains(validLevels, strings.ToLower(c.Logging.Level)) {
		return fmt.Errorf("invalid log level: %s", c.Logging.Level)
	}

	// Validate sharding configuration
	if !c.Sharding.Mode.IsValid() {
		return fmt.Errorf("invalid sharding mode: %s, must be one of: standalone, parent, child", c.Sharding.Mode)
	}

	if c.Sharding.ShardIDLength < 1 || c.Sharding.ShardIDLength > 16 {
		return fmt.Errorf("shard ID length must be between 1 and 16 bits, got: %d", c.Sharding.ShardIDLength)
	}

	if err := c.Sharding.Validate(); err != nil {
		return fmt.Errorf("invalid sharding configuration: %w", err)
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

func getEnvDurationOrDefault(key string, defaultValue string) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	duration, _ := time.ParseDuration(defaultValue)
	return duration
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

func (c *BFTConfig) GetRootNodes() (peer.IDSlice, error) {
	nodes := c.TrustBase.GetRootNodes()
	idSlice := make(peer.IDSlice, len(nodes))
	for i, node := range nodes {
		id, err := peer.Decode(node.NodeID)
		if err != nil {
			return nil, fmt.Errorf("invalid root node id in trust base: %w", err)
		}
		idSlice[i] = id
	}
	return idSlice, nil
}
