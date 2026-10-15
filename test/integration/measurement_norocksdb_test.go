//go:build e2e_bft && !rocksdb

package integration

// haveRocksDB is false without -tags rocksdb: only the memory backend variants run.
const haveRocksDB = false
