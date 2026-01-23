package logging

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Logger writes log messages to a file
type Logger struct {
	mu   sync.Mutex
	file *os.File
}

// Global logger instance (accessed atomically for thread-safety)
var globalLogger atomic.Pointer[Logger]

// Init initializes the global logger with the specified file path
// If path is empty, logging is disabled
func Init(path string) error {
	if path == "" {
		return nil
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	logger := &Logger{file: file}
	globalLogger.Store(logger)

	// Write header
	Info("=== Timeline Downloader Log Started ===")

	return nil
}

// Close closes the global logger, ensuring all pending writes complete first.
// Sets file to nil under lock to prevent race with concurrent log() calls.
func Close() {
	logger := globalLogger.Swap(nil)
	if logger != nil {
		logger.mu.Lock()
		if logger.file != nil {
			logger.file.Close()
			logger.file = nil // Prevent writes to closed file
		}
		logger.mu.Unlock()
	}
}

// Info logs an info message
func Info(format string, args ...any) {
	logger := globalLogger.Load()
	if logger == nil {
		return
	}
	logger.log("INFO", format, args...)
}

// Error logs an error message
func Error(format string, args ...any) {
	logger := globalLogger.Load()
	if logger == nil {
		return
	}
	logger.log("ERROR", format, args...)
}

// Warn logs a warning message
func Warn(format string, args ...any) {
	logger := globalLogger.Load()
	if logger == nil {
		return
	}
	logger.log("WARN", format, args...)
}

// Debug logs a debug message
func Debug(format string, args ...any) {
	logger := globalLogger.Load()
	if logger == nil {
		return
	}
	logger.log("DEBUG", format, args...)
}

func (l *Logger) log(level, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if file was closed (race with Close())
	if l.file == nil {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.file, "[%s] %s: %s\n", timestamp, level, msg)
}

// IsEnabled returns true if logging is enabled
func IsEnabled() bool {
	return globalLogger.Load() != nil
}
