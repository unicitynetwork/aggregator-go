//go:build !rocksdb

package round

import (
	"fmt"

	"github.com/unicitynetwork/aggregator-go/internal/config"
	smtbackend "github.com/unicitynetwork/aggregator-go/internal/smt/backend"
)

func newConfiguredRocksDBSMTBackend(*config.Config) (smtbackend.Backend, error) {
	return nil, fmt.Errorf("SMT_BACKEND=rocksdb requires a binary built with -tags rocksdb")
}
