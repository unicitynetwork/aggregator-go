package logger

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// AsyncLogger wraps a Logger to provide asynchronous logging capabilities
type AsyncLogger struct {
	logger       *Logger
	entries      chan logEntry
	wg           sync.WaitGroup
	stopped      atomic.Bool
	bufferSize   int
	droppedLogs  atomic.Uint64 // Counter for dropped log entries
	lastReported atomic.Uint64 // Last reported dropped count
}

type logEntry struct {
	ctx     context.Context
	record  slog.Record
	handler slog.Handler
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

// GetDroppedLogs returns the total number of dropped log entries
func (alw *AsyncLoggerWrapper) GetDroppedLogs() uint64 {
	return alw.asyncLogger.GetDroppedLogs()
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
	al.wg.Go(al.worker)

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
	// Batch processing for better performance
	const batchSize = 100
	batch := make([]logEntry, 0, batchSize)
	ticker := time.NewTicker(10 * time.Millisecond) // Flush every 10ms if batch not full
	defer ticker.Stop()

	// Periodic reporting of dropped logs
	reportTicker := time.NewTicker(30 * time.Second)
	defer reportTicker.Stop()

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

		case <-reportTicker.C:
			// Periodic reporting of dropped logs
			if dropped := al.droppedLogs.Load(); dropped > 0 {
				last := al.lastReported.Load()
				if dropped > last {
					al.checkAndReportDropped()
				}
			}
		}
	}
}

// flushBatch writes all entries in the batch
func (al *AsyncLogger) flushBatch(batch []logEntry) {
	for _, entry := range batch {
		if err := entry.handler.Handle(entry.ctx, entry.record); err != nil {
			al.logger.WithContext(entry.ctx).Warn("Async handler failed to write log entry", "error", err.Error())
		}
	}
}

// Stop gracefully shuts down the async logger
func (al *AsyncLogger) Stop() {
	if al.stopped.CompareAndSwap(false, true) {
		close(al.entries)
		al.wg.Wait()

		// Report final dropped count if any
		if dropped := al.droppedLogs.Load(); dropped > 0 {
			al.logger.Warn("Async logger shutdown - final dropped log count",
				"droppedTotal", dropped,
				"bufferSize", al.bufferSize,
			)
		}
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
		ctx:     acl.ctx,
		record:  newRecord(level, msg, args...),
		handler: acl.AsyncLogger.logger.WithContext(acl.ctx).Handler(),
	}:
		// Successfully queued
	default:
		// Buffer full, increment dropped counter
		acl.AsyncLogger.droppedLogs.Add(1)
		// Report periodically (every 1000 drops)
		acl.checkAndReportDropped()
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

func newRecord(level slog.Level, msg string, args ...any) slog.Record {
	r := slog.NewRecord(time.Now(), level, msg, 0)
	r.AddAttrs(argsToAttrs(args)...)
	return r
}

func argsToAttrs(args []any) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(args))
	for len(args) > 0 {
		switch v := args[0].(type) {
		case slog.Attr:
			attrs = append(attrs, v)
			args = args[1:]
		case string:
			if len(args) >= 2 {
				attrs = append(attrs, slog.Any(v, args[1]))
				args = args[2:]
			} else {
				attrs = append(attrs, slog.Any(v, nil))
				args = args[1:]
			}
		default:
			if len(args) >= 2 {
				attrs = append(attrs, slog.Any(fmt.Sprint(v), args[1]))
				args = args[2:]
			} else {
				attrs = append(attrs, slog.Any("!BADKEY", v))
				args = args[1:]
			}
		}
	}
	return attrs
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

	// Clone the record since slog may reuse it after Handle returns
	cloned := record.Clone()

	// Try to send to channel, drop if full to prevent blocking
	select {
	case h.asyncLogger.entries <- logEntry{
		ctx:     ctx,
		record:  cloned,
		handler: h.baseHandler,
	}:
		// Successfully queued
		return nil
	default:
		// Buffer full, increment dropped counter
		h.asyncLogger.droppedLogs.Add(1)
		// Report periodically
		h.asyncLogger.checkAndReportDropped()
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

// checkAndReportDropped reports dropped logs periodically
func (al *AsyncLogger) checkAndReportDropped() {
	current := al.droppedLogs.Load()
	last := al.lastReported.Load()

	// Report every 1000 drops or if it's the first drop
	if current-last >= 1000 || (current > 0 && last == 0) {
		// Try to update lastReported atomically
		if al.lastReported.CompareAndSwap(last, current) {
			// Log synchronously to ensure this critical metric is recorded
			al.logger.Warn("Async logger buffer full, logs dropped",
				"droppedTotal", current,
				"droppedSinceLastReport", current-last,
				"bufferSize", al.bufferSize,
			)
		}
	}
}

// GetDroppedLogs returns the total number of dropped log entries
func (al *AsyncLogger) GetDroppedLogs() uint64 {
	return al.droppedLogs.Load()
}

// checkAndReportDropped on AsyncContextLogger delegates to AsyncLogger
func (acl *AsyncContextLogger) checkAndReportDropped() {
	acl.AsyncLogger.checkAndReportDropped()
}
