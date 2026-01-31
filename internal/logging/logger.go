package logging

import (
	"log/slog"
	"os"
	"sync/atomic"
	"time"
)

// Global logger instance (accessed atomically for thread-safety)
var globalLogger atomic.Pointer[slog.Logger]

// Init initializes the global logger with the specified file path
// If path is empty, logging is disabled
func Init(path string) error {
	if path == "" {
		return nil
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	handler := slog.NewJSONHandler(file, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	logger := slog.New(handler)
	globalLogger.Store(logger)

	// Write header
	Info("Timeline Downloader log started")

	return nil
}

// Close closes the global logger
// Note: slog doesn't require explicit close; the file will be closed when the process exits
// or when the caller closes the underlying file handle if needed
func Close() {
	globalLogger.Store(nil)
}

// Info logs an info message with structured key-value pairs
func Info(msg string, args ...any) {
	logger := globalLogger.Load()
	if logger == nil {
		return
	}
	logger.Info(msg, args...)
}

// Error logs an error message with structured key-value pairs
func Error(msg string, args ...any) {
	logger := globalLogger.Load()
	if logger == nil {
		return
	}
	logger.Error(msg, args...)
}

// Warn logs a warning message with structured key-value pairs
func Warn(msg string, args ...any) {
	logger := globalLogger.Load()
	if logger == nil {
		return
	}
	logger.Warn(msg, args...)
}

// Debug logs a debug message with structured key-value pairs
func Debug(msg string, args ...any) {
	logger := globalLogger.Load()
	if logger == nil {
		return
	}
	logger.Debug(msg, args...)
}

// InfoWithJob logs an info message with job ID context
func InfoWithJob(jobID int, msg string, args ...any) {
	logger := globalLogger.Load()
	if logger == nil {
		return
	}
	allArgs := append([]any{"job_id", jobID}, args...)
	logger.Info(msg, allArgs...)
}

// ErrorWithJob logs an error message with job ID context
func ErrorWithJob(jobID int, msg string, args ...any) {
	logger := globalLogger.Load()
	if logger == nil {
		return
	}
	allArgs := append([]any{"job_id", jobID}, args...)
	logger.Error(msg, allArgs...)
}

// WarnWithJob logs a warning message with job ID context
func WarnWithJob(jobID int, msg string, args ...any) {
	logger := globalLogger.Load()
	if logger == nil {
		return
	}
	allArgs := append([]any{"job_id", jobID}, args...)
	logger.Warn(msg, allArgs...)
}

// DebugWithJob logs a debug message with job ID context
func DebugWithJob(jobID int, msg string, args ...any) {
	logger := globalLogger.Load()
	if logger == nil {
		return
	}
	allArgs := append([]any{"job_id", jobID}, args...)
	logger.Debug(msg, allArgs...)
}

// LogChunkBoundary logs chunk boundary information with nanosecond precision timestamps
func LogChunkBoundary(jobID, chunkIndex, totalChunks int, fromNano, toNano int64) {
	logger := globalLogger.Load()
	if logger == nil {
		return
	}
	fromTime := time.Unix(0, fromNano).UTC()
	toTime := time.Unix(0, toNano).UTC()
	logger.Info("chunk boundary",
		"job_id", jobID,
		"chunk_index", chunkIndex,
		"total_chunks", totalChunks,
		"from_nano", fromNano,
		"to_nano", toNano,
		"from", fromTime.Format(time.RFC3339),
		"to", toTime.Format(time.RFC3339),
	)
}

// IsEnabled returns true if logging is enabled
func IsEnabled() bool {
	return globalLogger.Load() != nil
}
