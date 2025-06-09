package logger

import (
	"context"
	"os"

	"github.com/sirupsen/logrus"
)

// ContextKey type for context keys
type ContextKey string

const (
	// RequestIDKey is the context key for request ID
	RequestIDKey ContextKey = "request_id"
	// ComponentKey is the context key for component name
	ComponentKey ContextKey = "component"
)

// Logger wraps logrus.Logger with additional functionality
type Logger struct {
	*logrus.Logger
}

// New creates a new logger instance
func New(level, format, output string, enableJSON bool) (*Logger, error) {
	log := logrus.New()

	// Set log level
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		return nil, err
	}
	log.SetLevel(lvl)

	// Set output
	switch output {
	case "stdout":
		log.SetOutput(os.Stdout)
	case "stderr":
		log.SetOutput(os.Stderr)
	default:
		file, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		log.SetOutput(file)
	}

	// Set formatter
	if enableJSON || format == "json" {
		log.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
		})
	} else {
		log.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
		})
	}

	return &Logger{Logger: log}, nil
}

// WithContext creates a new logger entry with context values
func (l *Logger) WithContext(ctx context.Context) *logrus.Entry {
	entry := l.Logger.WithContext(ctx)

	// Add request ID if available
	if requestID := ctx.Value(RequestIDKey); requestID != nil {
		entry = entry.WithField("request_id", requestID)
	}

	// Add component if available
	if component := ctx.Value(ComponentKey); component != nil {
		entry = entry.WithField("component", component)
	}

	return entry
}

// WithComponent creates a new logger entry with component field
func (l *Logger) WithComponent(component string) *logrus.Entry {
	return l.Logger.WithField("component", component)
}

// WithRequestID creates a new logger entry with request ID field
func (l *Logger) WithRequestID(requestID string) *logrus.Entry {
	return l.Logger.WithField("request_id", requestID)
}

// WithFields creates a new logger entry with multiple fields
func (l *Logger) WithFields(fields map[string]interface{}) *logrus.Entry {
	return l.Logger.WithFields(fields)
}

// WithError creates a new logger entry with error field
func (l *Logger) WithError(err error) *logrus.Entry {
	return l.Logger.WithError(err)
}

// Performance-optimized logging methods that check level before formatting
func (l *Logger) DebugContext(ctx context.Context, args ...interface{}) {
	if l.Logger.IsLevelEnabled(logrus.DebugLevel) {
		l.WithContext(ctx).Debug(args...)
	}
}

func (l *Logger) InfoContext(ctx context.Context, args ...interface{}) {
	if l.Logger.IsLevelEnabled(logrus.InfoLevel) {
		l.WithContext(ctx).Info(args...)
	}
}

func (l *Logger) WarnContext(ctx context.Context, args ...interface{}) {
	if l.Logger.IsLevelEnabled(logrus.WarnLevel) {
		l.WithContext(ctx).Warn(args...)
	}
}

func (l *Logger) ErrorContext(ctx context.Context, args ...interface{}) {
	if l.Logger.IsLevelEnabled(logrus.ErrorLevel) {
		l.WithContext(ctx).Error(args...)
	}
}

func (l *Logger) DebugfContext(ctx context.Context, format string, args ...interface{}) {
	if l.Logger.IsLevelEnabled(logrus.DebugLevel) {
		l.WithContext(ctx).Debugf(format, args...)
	}
}

func (l *Logger) InfofContext(ctx context.Context, format string, args ...interface{}) {
	if l.Logger.IsLevelEnabled(logrus.InfoLevel) {
		l.WithContext(ctx).Infof(format, args...)
	}
}

func (l *Logger) WarnfContext(ctx context.Context, format string, args ...interface{}) {
	if l.Logger.IsLevelEnabled(logrus.WarnLevel) {
		l.WithContext(ctx).Warnf(format, args...)
	}
}

func (l *Logger) ErrorfContext(ctx context.Context, format string, args ...interface{}) {
	if l.Logger.IsLevelEnabled(logrus.ErrorLevel) {
		l.WithContext(ctx).Errorf(format, args...)
	}
}