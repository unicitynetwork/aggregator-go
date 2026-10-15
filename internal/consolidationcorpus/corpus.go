// Package consolidationcorpus defines the file schema of the portable UC
// consolidation fixture corpus (testdata/uc-consolidation/v1), loads it and
// evaluates a case through the reference library (pkg/consolidation). The
// generator and the Go consumer tests share it; SDK consumers implement the
// same pipeline over the same files.
package consolidationcorpus

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/unicitynetwork/bft-go-base/types"
)

// Version of the corpus layout and schema.
const Version = 1

// File names of a case.
const (
	FileContext       = "context.json"
	FileTrustBase     = "trust-base.cbor"
	FileProducer      = "producer.json"
	FileResponseV1    = "response-v1.cbor"
	FileResponseV2    = "response-v2.cbor"
	FilePartitionView = "partition-view.cbor"
	FileRequests      = "requests.json"
	FileResponses     = "responses.json"
	FileExpected      = "expected.json"
	DirShardConfigs   = "shard-configs/"
	DirUCs            = "ucs/"
)

// Files is the content of a case directory by relative slash path.
type Files map[string][]byte

// Clone deep copies the files.
func (f Files) Clone() Files {
	c := make(Files, len(f))
	for k, v := range f {
		c[k] = append([]byte(nil), v...)
	}
	return c
}

// ReadDir loads every file below dir.
func ReadDir(dir string) (Files, error) {
	files := Files{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(rel)] = b
		return nil
	})
	return files, err
}

// WriteDir writes the files below dir.
func (f Files) WriteDir(dir string) error {
	names := make([]string, 0, len(f))
	for n := range f {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		p := filepath.Join(dir, filepath.FromSlash(n))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(p, f[n], 0o644); err != nil {
			return err
		}
	}
	return nil
}

// ShardConfig is one admitted shard configuration.
type ShardConfig struct {
	// Shard is the shard ID bit string, empty for the single shard.
	Shard string `json:"shard"`
	// File is the native configuration bytes; Hash its committed hash (hex).
	File string `json:"file"`
	Hash string `json:"hash"`
}

// Context is the trusted verification context of a case.
type Context struct {
	CorpusVersion int    `json:"corpusVersion"`
	NetworkID     uint32 `json:"networkId"`
	PartitionID   uint32 `json:"partitionId"`
	// Shards are all shards of the partition in canonical order.
	Shards       []string      `json:"shards"`
	ShardConfigs []ShardConfig `json:"shardConfigs"`
	// Master is the shard whose latest batch supplies the view.
	Master       string   `json:"master"`
	Participants []string `json:"participants"`
	TrustBase    string   `json:"trustBase"`
}

// Batch is the original requests of one shard.
type Batch struct {
	Shard string `json:"shard"`
	// Requests are hex CBOR api.CertificationRequest, in order.
	Requests []string `json:"requests"`
}

// Evidence is one original input with the proof it was certified by.
type Evidence struct {
	Request string `json:"request"` // hex CBOR api.CertificationRequest
	Proof   string `json:"proof"`   // hex CBOR api.GetInclusionProofResponseV2
}

// Requests is requests.json.
type Requests struct {
	Master   Batch      `json:"master"`
	Peers    []Batch    `json:"peers"`
	Evidence []Evidence `json:"evidence"`
}

// RPCResult is the result of a get_inclusion_proofs.v1 call.
type RPCResult struct {
	Proofs        []string `json:"proofs"`
	PartitionView string   `json:"partitionView,omitempty"`
}

// RPCResponse is an exact JSON-RPC success response.
type RPCResponse struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      int       `json:"id"`
	Result  RPCResult `json:"result"`
}

// PeerResponse is the exact-root response of a peer shard.
type PeerResponse struct {
	Shard    string      `json:"shard"`
	Response RPCResponse `json:"response"`
}

// Responses is responses.json: the master response (latest, with the view)
// and one exact-root response per peer.
type Responses struct {
	Master RPCResponse    `json:"master"`
	Peers  []PeerResponse `json:"peers"`
}

// Decode parses the JSON file into v.
func (f Files) Decode(name string, v any) error {
	b, ok := f[name]
	if !ok {
		return fmt.Errorf("missing file %s", name)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

// Encode sets the JSON file from v in the corpus canonical form (two space indent, trailing newline).
func (f Files) Encode(name string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f[name] = append(b, '\n')
	return nil
}

// ParseShard converts a shard ID bit string ("", "0", "10") to the native ID.
func ParseShard(s string) (types.ShardID, error) {
	id := types.ShardID{}
	for _, c := range s {
		a, b := id.Split()
		switch c {
		case '0':
			id = a
		case '1':
			id = b
		default:
			return types.ShardID{}, fmt.Errorf("invalid shard ID %q", s)
		}
	}
	return id, nil
}

// ShardName is the bit string of a shard ID.
func ShardName(id types.ShardID) string { return id.String() }

// Hex helpers: corpus bytes are lowercase hex.
func Hex(b []byte) string { return hex.EncodeToString(b) }

func UnHex(s string) ([]byte, error) { return hex.DecodeString(strings.TrimPrefix(s, "0x")) }
