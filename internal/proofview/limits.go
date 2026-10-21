package proofview

import (
	"fmt"
	"time"

	"github.com/unicitynetwork/aggregator-go/internal/config"
)

// Limits bound what a Catalog retains and serves. The defaults are the
// consolidation admission limits from the UC consolidation plan (§2.3, §2.5).
type Limits struct {
	// MaxViews is the number of finalized local state views retained.
	MaxViews int
	// MaxAttachmentsPerView bounds distinct certification attachments (UC plus
	// optional partition view) kept per local view; the latest is always kept.
	MaxAttachmentsPerView int
	// RetentionBudgetBytes is the conservative incremental retention budget,
	// counting backend-retained bytes, attachments, and retired-but-pinned
	// views. It is accounting, not a hard process memory cap.
	RetentionBudgetBytes uint64
	// MaxConcurrentPins bounds simultaneously held leases.
	MaxConcurrentPins int
	// MaxLeaseDuration is the deadline applied to each lease's context.
	MaxLeaseDuration time.Duration
}

const (
	DefaultMaxViews              = 16
	DefaultMaxAttachmentsPerView = 4
	DefaultRetentionBudgetBytes  = 256 << 20
	DefaultMaxConcurrentPins     = 8
	DefaultMaxLeaseDuration      = 5 * time.Second
)

// DefaultLimits returns the plan defaults.
func DefaultLimits() Limits {
	return Limits{
		MaxViews:              DefaultMaxViews,
		MaxAttachmentsPerView: DefaultMaxAttachmentsPerView,
		RetentionBudgetBytes:  DefaultRetentionBudgetBytes,
		MaxConcurrentPins:     DefaultMaxConcurrentPins,
		MaxLeaseDuration:      DefaultMaxLeaseDuration,
	}
}

// Validate rejects non-positive limits.
func (l Limits) Validate() error {
	switch {
	case l.MaxViews <= 0:
		return fmt.Errorf("proofview: MaxViews must be positive, got %d", l.MaxViews)
	case l.MaxAttachmentsPerView <= 0:
		return fmt.Errorf("proofview: MaxAttachmentsPerView must be positive, got %d", l.MaxAttachmentsPerView)
	case l.RetentionBudgetBytes == 0:
		return fmt.Errorf("proofview: RetentionBudgetBytes must be positive")
	case l.MaxConcurrentPins <= 0:
		return fmt.Errorf("proofview: MaxConcurrentPins must be positive, got %d", l.MaxConcurrentPins)
	case l.MaxLeaseDuration <= 0:
		return fmt.Errorf("proofview: MaxLeaseDuration must be positive, got %s", l.MaxLeaseDuration)
	}
	return nil
}

// LimitsFromConfig maps the SMT retention settings onto Limits, filling any
// zero value with its default. The result is validated.
func LimitsFromConfig(cfg config.SMTConfig) (Limits, error) {
	l := DefaultLimits()
	if cfg.RetainedViews > 0 {
		l.MaxViews = cfg.RetainedViews
	}
	if cfg.RetainedAttachmentsPerView > 0 {
		l.MaxAttachmentsPerView = cfg.RetainedAttachmentsPerView
	}
	if cfg.RetentionBudgetMB > 0 {
		l.RetentionBudgetBytes = uint64(cfg.RetentionBudgetMB) << 20
	}
	if cfg.MaxConcurrentBatches > 0 {
		l.MaxConcurrentPins = cfg.MaxConcurrentBatches
	}
	if cfg.BatchTimeout > 0 {
		l.MaxLeaseDuration = cfg.BatchTimeout
	}
	return l, l.Validate()
}
