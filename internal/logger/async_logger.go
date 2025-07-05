package logger

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// AsyncLogger wraps a Logger to provide asynchronous logging capabilities
type AsyncLogger struct {
	logger     *Logger
	entries    chan logEntry
	wg         sync.WaitGroup
	stopped    atomic.Bool
	bufferSize int
}

type logEntry struct {
	ctx    context.Context
	level  slog.Level
	msg    string
	args   []any
}

// AsyncLoggerWrapper wraps both the Logger interface and the AsyncLogger functionality
type AsyncLoggerWrapper struct {
	*Logger
	asyncLogger *AsyncLogger
}

// Stop gracefully shuts down the async logger
func (alw *AsyncLoggerWrapper) Stop() {
	alw.asyncLogger.Stop()
}

// NewAsyncLogger creates a new async logger with the specified buffer size
func NewAsyncLogger(baseLogger *Logger, bufferSize int) *AsyncLoggerWrapper {
	if bufferSize <= 0 {
		bufferSize = 10000 // Default buffer size
	}
	
	al := &AsyncLogger{
		logger:     baseLogger,
		entries:    make(chan logEntry, bufferSize),
		bufferSize: bufferSize,
	}
	
	// Start the background worker
	al.wg.Add(1)
	go al.worker()
	
	// Create a new Logger that wraps the async logger
	handler := &asyncHandler{
		asyncLogger: al,
		baseHandler: baseLogger.Logger.Handler(),
	}
	
	return &AsyncLoggerWrapper{
		Logger: &Logger{
			Logger: slog.New(handler),
		},
		asyncLogger: al,
	}
}

// worker processes log entries from the channel
func (al *AsyncLogger) worker() {
	defer al.wg.Done()
	
	// Batch processing for better performance
	const batchSize = 100
	batch := make([]logEntry, 0, batchSize)
	ticker := time.NewTicker(10 * time.Millisecond) // Flush every 10ms if batch not full
	defer ticker.Stop()
	
	for {
		select {
		case entry, ok := <-al.entries:
			if !ok {
				// Channel closed, flush remaining entries
				al.flushBatch(batch)
				return
			}
			
			batch = append(batch, entry)
			
			// Flush if batch is full
			if len(batch) >= batchSize {
				al.flushBatch(batch)
				batch = batch[:0]
			}
			
		case <-ticker.C:
			// Periodic flush to prevent log delays
			if len(batch) > 0 {
				al.flushBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// flushBatch writes all entries in the batch
func (al *AsyncLogger) flushBatch(batch []logEntry) {
	for _, entry := range batch {
		// Use the underlying synchronous logger
		switch entry.level {
		case slog.LevelDebug:
			al.logger.WithContext(entry.ctx).Debug(entry.msg, entry.args...)
		case slog.LevelInfo:
			al.logger.WithContext(entry.ctx).Info(entry.msg, entry.args...)
		case slog.LevelWarn:
			al.logger.WithContext(entry.ctx).Warn(entry.msg, entry.args...)
		case slog.LevelError:
			al.logger.WithContext(entry.ctx).Error(entry.msg, entry.args...)
		}
	}
}

// Stop gracefully shuts down the async logger
func (al *AsyncLogger) Stop() {
	if al.stopped.CompareAndSwap(false, true) {
		close(al.entries)
		al.wg.Wait()
	}
}

// WithContext returns a new AsyncContextLogger
func (al *AsyncLogger) WithContext(ctx context.Context) *AsyncContextLogger {
	return &AsyncContextLogger{
		AsyncLogger: al,
		ctx:         ctx,
	}
}

// AsyncContextLogger provides async logging with context
type AsyncContextLogger struct {
	*AsyncLogger
	ctx context.Context
}

// log asynchronously sends a log entry
func (acl *AsyncContextLogger) log(level slog.Level, msg string, args ...any) {
	if acl.AsyncLogger.stopped.Load() {
		// Fallback to synchronous logging if async logger is stopped
		cl := acl.AsyncLogger.logger.WithContext(acl.ctx)
		switch level {
		case slog.LevelDebug:
			cl.Debug(msg, args...)
		case slog.LevelInfo:
			cl.Info(msg, args...)
		case slog.LevelWarn:
			cl.Warn(msg, args...)
		case slog.LevelError:
			cl.Error(msg, args...)
		}
		return
	}
	
	// Try to send to channel, drop if full to prevent blocking
	select {
	case acl.AsyncLogger.entries <- logEntry{
		ctx:   acl.ctx,
		level: level,
		msg:   msg,
		args:  args,
	}:
		// Successfully queued
	default:
		// Buffer full, increment dropped counter and optionally log
		// In production, you might want to track dropped logs
	}
}

// Debug logs at debug level asynchronously
func (acl *AsyncContextLogger) Debug(msg string, args ...any) {
	acl.log(slog.LevelDebug, msg, args...)
}

// Info logs at info level asynchronously
func (acl *AsyncContextLogger) Info(msg string, args ...any) {
	acl.log(slog.LevelInfo, msg, args...)
}

// Warn logs at warn level asynchronously
func (acl *AsyncContextLogger) Warn(msg string, args ...any) {
	acl.log(slog.LevelWarn, msg, args...)
}

// Error logs at error level asynchronously
func (acl *AsyncContextLogger) Error(msg string, args ...any) {
	acl.log(slog.LevelError, msg, args...)
}

// asyncHandler implements slog.Handler for async logging
type asyncHandler struct {
	asyncLogger *AsyncLogger
	baseHandler slog.Handler
}

// Enabled implements slog.Handler
func (h *asyncHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.baseHandler.Enabled(ctx, level)
}

// Handle implements slog.Handler
func (h *asyncHandler) Handle(ctx context.Context, record slog.Record) error {
	if h.asyncLogger.stopped.Load() {
		// Fallback to synchronous logging if async logger is stopped
		return h.baseHandler.Handle(ctx, record)
	}
	
	// Convert record to our log entry format
	args := make([]any, 0, record.NumAttrs()*2)
	record.Attrs(func(attr slog.Attr) bool {
		args = append(args, attr.Key, attr.Value.Any())
		return true
	})
	
	// Try to send to channel, drop if full to prevent blocking
	select {
	case h.asyncLogger.entries <- logEntry{
		ctx:   ctx,
		level: record.Level,
		msg:   record.Message,
		args:  args,
	}:
		// Successfully queued
		return nil
	default:
		// Buffer full, drop the log
		// In production, you might want to track dropped logs
		return nil
	}
}

// WithAttrs implements slog.Handler
func (h *asyncHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &asyncHandler{
		asyncLogger: h.asyncLogger,
		baseHandler: h.baseHandler.WithAttrs(attrs),
	}
}

// WithGroup implements slog.Handler
func (h *asyncHandler) WithGroup(name string) slog.Handler {
	return &asyncHandler{
		asyncLogger: h.asyncLogger,
		baseHandler: h.baseHandler.WithGroup(name),
	}
}