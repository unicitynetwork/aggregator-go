package config

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/unicitynetwork/bft-core/network"
	"github.com/unicitynetwork/bft-core/partition"
	"github.com/unicitynetwork/bft-go-base/types"
	"github.com/unicitynetwork/bft-go-base/util"
)

// Config represents the application configuration
type Config struct {
	Server     ServerConfig     `mapstructure:"server"`
	Database   DatabaseConfig   `mapstructure:"database"`
	HA         HAConfig         `mapstructure:"ha"`
	Logging    LoggingConfig    `mapstructure:"logging"`
	BFT        BFTConfig        `mapstructure:"bft"`
	Processing ProcessingConfig `mapstructure:"processing"`
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
	BatchLimit       int           `mapstructure:"batch_limit"`
	RoundDuration    time.Duration `mapstructure:"round_duration"`
	SMTMaxGoroutines int           `mapstructure:"smt_max_goroutines"`
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
			BatchLimit:       getEnvIntOrDefault("BATCH_LIMIT", 1000),
			RoundDuration:    getEnvDurationOrDefault("ROUND_DURATION", "1s"),
			SMTMaxGoroutines: getEnvIntOrDefault("SMT_MAX_GOROUTINES", 0), // 0 means sequential (no goroutines)
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

	// Handle SMT_MAX_GOROUTINES special value -1 (calculate CPU-based default)
	if config.Processing.SMTMaxGoroutines == -1 {
		config.Processing.SMTMaxGoroutines = calculateDefaultGoroutineLimit()
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

	// Validate SMTMaxGoroutines
	// 0 means no goroutines (sequential), 1-512 for specific limit
	// Note: -1 is handled in Load() and converted to CPU-based value
	if c.Processing.SMTMaxGoroutines < 0 || c.Processing.SMTMaxGoroutines > 512 {
		return fmt.Errorf("SMT_MAX_GOROUTINES must be 0 (sequential) or between 1-512, got: %d", c.Processing.SMTMaxGoroutines)
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

// calculateDefaultGoroutineLimit calculates the CPU-based default goroutine limit for SMT
func calculateDefaultGoroutineLimit() int {
	// Default: Limit to 2x CPU cores, with a minimum of 8 and maximum of 512
	numCPU := runtime.GOMAXPROCS(0)
	maxGoroutines := numCPU * 2
	if maxGoroutines < 8 {
		maxGoroutines = 8
	}
	if maxGoroutines > 512 {
		maxGoroutines = 512
	}
	return maxGoroutines
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
