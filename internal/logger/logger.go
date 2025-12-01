package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// ContextKey type for context keys
type ContextKey string

const (
	// StateIDKey is the context key for state ID
	StateIDKey ContextKey = "state_id"
	// ComponentKey is the context key for component name
	ComponentKey ContextKey = "component"
)

// Logger wraps slog.Logger with additional functionality
type Logger struct {
	*slog.Logger
}

// New creates a new logger instance
func New(level, format, output string, enableJSON bool) (*Logger, error) {
	// Parse log level
	var logLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn", "warning":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	// Determine output writer
	var writer io.Writer
	switch output {
	case "stdout", "":
		writer = os.Stdout
	case "stderr":
		writer = os.Stderr
	default:
		file, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		writer = file
	}

	// Create handler options
	opts := &slog.HandlerOptions{
		Level: logLevel,
	}

	// Create handler based on format
	var handler slog.Handler
	if enableJSON || format == "json" {
		handler = slog.NewJSONHandler(writer, opts)
	} else {
		handler = slog.NewTextHandler(writer, opts)
	}

	logger := slog.New(handler)
	return &Logger{Logger: logger}, nil
}

// WithContext creates a new logger with context values
func (l *Logger) WithContext(ctx context.Context) *slog.Logger {
	var args []any

	// Add state ID if available
	if stateID := ctx.Value(StateIDKey); stateID != nil {
		if rid, ok := stateID.(string); ok {
			args = append(args, slog.String("state_id", rid))
		}
	}

	// Add component if available
	if component := ctx.Value(ComponentKey); component != nil {
		if comp, ok := component.(string); ok {
			args = append(args, slog.String("component", comp))
		}
	}

	if len(args) == 0 {
		return l.Logger
	}

	return l.Logger.With(args...)
}

// WithComponent creates a new logger with component field
func (l *Logger) WithComponent(component string) *slog.Logger {
	return l.Logger.With(slog.String("component", component))
}

// WithStateID creates a new logger with state ID field
func (l *Logger) WithStateID(stateID string) *slog.Logger {
	return l.Logger.With(slog.String("state_id", stateID))
}

// WithFields creates a new logger with multiple fields
func (l *Logger) WithFields(fields map[string]interface{}) *slog.Logger {
	args := make([]any, 0, len(fields)*2)
	for key, value := range fields {
		args = append(args, key, value)
	}
	return l.Logger.With(args...)
}

// WithError creates a new logger with error field
func (l *Logger) WithError(err error) *slog.Logger {
	return l.Logger.With(slog.String("error", err.Error()))
}

// Context-aware logging methods
func (l *Logger) DebugContext(ctx context.Context, msg string, args ...any) {
	l.WithContext(ctx).Debug(msg, args...)
}

func (l *Logger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.WithContext(ctx).Info(msg, args...)
}

func (l *Logger) WarnContext(ctx context.Context, msg string, args ...any) {
	l.WithContext(ctx).Warn(msg, args...)
}

func (l *Logger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.WithContext(ctx).Error(msg, args...)
}

// Formatted context-aware logging methods (for backward compatibility)
func (l *Logger) DebugfContext(ctx context.Context, format string, args ...interface{}) {
	l.WithContext(ctx).Debug(format, args...)
}

func (l *Logger) InfofContext(ctx context.Context, format string, args ...interface{}) {
	l.WithContext(ctx).Info(format, args...)
}

func (l *Logger) WarnfContext(ctx context.Context, format string, args ...interface{}) {
	l.WithContext(ctx).Warn(format, args...)
}

func (l *Logger) ErrorfContext(ctx context.Context, format string, args ...interface{}) {
	l.WithContext(ctx).Error(format, args...)
}

// Compatibility methods for easier migration from logrus
func (l *Logger) Fatal(args ...interface{}) {
	l.Logger.Error("fatal error", slog.Any("args", args))
	os.Exit(1)
}

func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.Logger.Error("fatal error", slog.String("msg", format), slog.Any("args", args))
	os.Exit(1)
}
