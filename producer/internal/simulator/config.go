package simulator

import (
	"fmt"
	"strconv"
	"time"
)

const (
	defaultPackages = 3
	maxPackages     = 100 // keeps a tick well under the 500-record PutRecords limit
	minInterval     = time.Second
)

// Config controls the optional automatic emitter.
type Config struct {
	Enabled  bool
	Interval time.Duration
	Packages int
}

// ParseConfig reads EMIT_INTERVAL (e.g. "25s"; empty or "0" disables the emitter)
// and EMIT_PACKAGES (default 3, at most 100) through getenv.
func ParseConfig(getenv func(string) string) (Config, error) {
	cfg := Config{Packages: defaultPackages}

	if raw := getenv("EMIT_PACKAGES"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxPackages {
			return Config{}, fmt.Errorf("EMIT_PACKAGES %q must be a whole number between 1 and %d", raw, maxPackages)
		}
		cfg.Packages = n
	}

	raw := getenv("EMIT_INTERVAL")
	if raw == "" {
		return cfg, nil
	}
	interval, err := time.ParseDuration(raw)
	if err != nil {
		return Config{}, fmt.Errorf("EMIT_INTERVAL %q is not a valid duration (example: 25s)", raw)
	}
	switch {
	case interval < 0:
		return Config{}, fmt.Errorf("EMIT_INTERVAL %q must not be negative", raw)
	case interval == 0:
		return cfg, nil
	case interval < minInterval:
		return Config{}, fmt.Errorf("EMIT_INTERVAL %q must be at least %s", raw, minInterval)
	}
	cfg.Enabled = true
	cfg.Interval = interval
	return cfg, nil
}
