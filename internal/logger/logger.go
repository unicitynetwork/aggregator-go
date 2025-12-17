package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

// ContextKey type for context keys
type ContextKey string

const (
	// RequestIDKey is the context key for request ID
	RequestIDKey ContextKey = "request_id"
	// ComponentKey is the context key for component name
	ComponentKey ContextKey = "component"
)

// Logger wraps slog.Logger with additional functionality
type Logger struct {
	*slog.Logger
	fileWriter io.Closer // holds reference to lumberjack for cleanup
}

// LogConfig holds the configuration for creating a logger
type LogConfig struct {
	Level           string
	Format          string
	Output          string
	EnableJSON      bool
	FilePath        string // Path to log file (empty = no file logging)
	MaxSizeMB       int    // Max size in MB before rotation
	MaxBackups      int    // Max number of old log files to retain
	MaxAgeDays      int    // Max days to retain old log files
	CompressBackups bool   // Compress rotated log files
}

// New creates a new logger instance
func New(level, format, output string, enableJSON bool) (*Logger, error) {
	return NewWithConfig(LogConfig{
		Level:      level,
		Format:     format,
		Output:     output,
		EnableJSON: enableJSON,
	})
}

// NewWithConfig creates a new logger instance with full configuration including file rotation
func NewWithConfig(cfg LogConfig) (*Logger, error) {
	// Parse log level
	var logLevel slog.Level
	switch strings.ToLower(cfg.Level) {
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

	// Determine console output writer
	var consoleWriter io.Writer
	switch cfg.Output {
	case "stderr":
		consoleWriter = os.Stderr
	default:
		consoleWriter = os.Stdout
	}

	// Build the final writer (console only, or multi-writer with file)
	var writer io.Writer
	var fileWriter *lumberjack.Logger

	if cfg.FilePath != "" {
		// Set defaults if not provided
		maxSize := cfg.MaxSizeMB
		if maxSize <= 0 {
			maxSize = 100 // 100 MB default
		}
		maxBackups := cfg.MaxBackups
		if maxBackups <= 0 {
			maxBackups = 30
		}
		maxAge := cfg.MaxAgeDays
		if maxAge <= 0 {
			maxAge = 30
		}

		// Create lumberjack logger for file rotation
		fileWriter = &lumberjack.Logger{
			Filename:   cfg.FilePath,
			MaxSize:    maxSize,    // megabytes
			MaxBackups: maxBackups, // number of backups
			MaxAge:     maxAge,     // days
			Compress:   cfg.CompressBackups,
			LocalTime:  true, // use local time for backup timestamps
		}

		// Combine console and file writers
		writer = io.MultiWriter(consoleWriter, fileWriter)
	} else {
		writer = consoleWriter
	}

	// Create handler options
	opts := &slog.HandlerOptions{
		Level: logLevel,
	}

	// Create handler based on format
	var handler slog.Handler
	if cfg.EnableJSON || cfg.Format == "json" {
		handler = slog.NewJSONHandler(writer, opts)
	} else {
		handler = slog.NewTextHandler(writer, opts)
	}

	logger := slog.New(handler)
	return &Logger{Logger: logger, fileWriter: fileWriter}, nil
}

// Close closes any file writers (call on shutdown for clean log rotation)
func (l *Logger) Close() error {
	if l.fileWriter != nil {
		return l.fileWriter.Close()
	}
	return nil
}

// WithContext creates a new logger with context values
func (l *Logger) WithContext(ctx context.Context) *slog.Logger {
	var args []any

	// Add request ID if available
	if requestID := ctx.Value(RequestIDKey); requestID != nil {
		if rid, ok := requestID.(string); ok {
			args = append(args, slog.String("request_id", rid))
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

// WithRequestID creates a new logger with request ID field
func (l *Logger) WithRequestID(requestID string) *slog.Logger {
	return l.Logger.With(slog.String("request_id", requestID))
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
