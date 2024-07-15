package metricsdedupprocessor

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
)

var _ component.Config = (*Config)(nil)

type Config struct {
	ReplicaLabel string        `mapstructure:"replica_label"`
	SwapTimeout  time.Duration `mapstructure:"swap_timeout"`
}

func (cfg *Config) Validate() error {
	if len(cfg.ReplicaLabel) == 0 {
		return fmt.Errorf("missing required field \"replica_label\"")
	}
	if cfg.SwapTimeout <= 0 {
		return fmt.Errorf("invalid swap timeout %v", cfg.SwapTimeout)
	}
	return nil
}
