// Command gen-ni-fixtures generates the shared non-inclusion corpus (testdata/non-inclusion/v1).
//
//	UC_FIXTURE_AGGREGATOR_COMMIT=$(git rev-parse HEAD) go run ./cmd/gen-ni-fixtures -out testdata/non-inclusion/v1
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/unicitynetwork/aggregator-go/internal/nifixture"
)

func main() {
	out := flag.String("out", "testdata/non-inclusion/v1", "output directory (replaced)")
	flag.Parse()
	files, err := nifixture.Generate(os.Getenv("UC_FIXTURE_AGGREGATOR_COMMIT"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	if err := os.RemoveAll(*out); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	for name, b := range files {
		p := filepath.Join(*out, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		if err := os.WriteFile(p, b, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	}
	fmt.Printf("wrote %d files to %s\n", len(files), *out)
}
