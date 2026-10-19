// Package serve is `nisshi serve`: the TOML configuration, the ingestion API (ADR 0003), token
// authentication (ADR 0008), status and metrics (ADR 0010), the seal schedule (ADR 0006), and
// the process's own JSON logs (ADR 0009).
package serve

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

// Config is the TOML file; unknown keys are an error.
type Config struct {
	Listen        []string          `toml:"listen"`
	ListenMetrics string            `toml:"listen_metrics"`
	DataDir       string            `toml:"data_dir"`
	TLS           TLS               `toml:"tls"`
	Tokens        map[string]string `toml:"tokens"` // hex sha256(token) -> "node:<name>" | "reader"
	RetentionDays int               `toml:"retention_days"`
	DiskReserve   uint64            `toml:"disk_reserve"`
}

// TLS holds the server certificate (ADR 0008).
type TLS struct {
	CertFile string `toml:"cert_file"`
	KeyFile  string `toml:"key_file"`
}

var (
	hexRe  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	nodeRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
)

// LoadConfig reads and checks a configuration file.
func LoadConfig(path string) (Config, error) {
	cfg := Config{RetentionDays: 400, DiskReserve: 2 << 30}
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	dec := toml.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	var problems []string
	if len(cfg.Listen) == 0 {
		problems = append(problems, "listen: at least one address")
	}
	if cfg.DataDir == "" {
		problems = append(problems, "data_dir: required")
	}
	if cfg.TLS.CertFile == "" || cfg.TLS.KeyFile == "" {
		problems = append(problems, "tls.cert_file and tls.key_file: required")
	}
	if cfg.RetentionDays < 0 {
		problems = append(problems, "retention_days: 0 or more")
	}
	for k, v := range cfg.Tokens {
		if !hexRe.MatchString(k) {
			problems = append(problems, "tokens: key "+k+" is not a lowercase hex SHA-256")
		}
		if name, ok := strings.CutPrefix(v, "node:"); v != "reader" && (!ok || !nodeRe.MatchString(name)) {
			problems = append(problems, "tokens: identity "+v+" is not node:<name> or reader")
		}
	}
	if len(problems) > 0 {
		return cfg, errors.New(path + ": " + strings.Join(problems, "; "))
	}
	return cfg, nil
}
