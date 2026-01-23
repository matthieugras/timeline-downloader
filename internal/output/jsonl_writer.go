package output

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/matthieugras/timeline-downloader/internal/logging"
)

// EventWriter is the interface for writing timeline events
type EventWriter interface {
	Write(data json.RawMessage) error
	Close() error
}

// TimestampExtractor extracts a timestamp from an event for filtering.
// Return zero time to skip the event (missing timestamp).
type TimestampExtractor func(data json.RawMessage) (time.Time, error)

// actionTimeEvent is used to extract ActionTime from raw event JSON
type actionTimeEvent struct {
	ActionTimeIsoString string `json:"ActionTimeIsoString"`
}

// getEventActionTime extracts the ActionTime from a raw timeline event JSON.
func getEventActionTime(data json.RawMessage) (time.Time, error) {
	var event actionTimeEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return time.Time{}, err
	}

	if event.ActionTimeIsoString == "" {
		return time.Time{}, nil
	}

	return time.Parse(time.RFC3339, event.ActionTimeIsoString)
}

// JSONLWriter writes JSON objects as newline-delimited JSON (JSONL).
// If a timestamp extractor is provided, events are filtered by [from, to).
// If no extractor is provided, all events are written without filtering.
type JSONLWriter struct {
	file             *os.File
	writer           *bufio.Writer // Buffered writer for better I/O performance
	from             time.Time
	to               time.Time
	timestampExtract TimestampExtractor // nil = no filtering
	mu               sync.Mutex

	writtenCount  int
	filteredCount int
	closed        bool
}

// NewJSONLWriter creates a new JSONL writer at the specified path with ActionTime filtering.
// Only events with ActionTime >= from AND ActionTime < to will be written.
// Used for device timeline downloads.
func NewJSONLWriter(path string, from, to time.Time) (*JSONLWriter, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create output directory: %w", err)
		}
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}

	return &JSONLWriter{
		file:             file,
		writer:           bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		from:             from,
		to:               to,
		timestampExtract: getEventActionTime, // Filter by ActionTime
	}, nil
}

// NewIdentityJSONLWriter creates a new JSONL writer for identity timeline events.
// No filtering is performed - the API guarantees events are already within [from, to).
func NewIdentityJSONLWriter(path string) (*JSONLWriter, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create output directory: %w", err)
		}
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}

	return &JSONLWriter{
		file:             file,
		writer:           bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		timestampExtract: nil,                                // No filtering
	}, nil
}

// Write writes a JSON event.
// If a timestamp extractor is configured, events are filtered by [from, to).
// Events with unparseable or missing timestamps are skipped when filtering is enabled.
func (w *JSONLWriter) Write(data json.RawMessage) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	// Apply filtering only if extractor is configured
	if w.timestampExtract != nil {
		ts, err := w.timestampExtract(data)
		if err != nil {
			logging.Warn("Event skipped: unparseable timestamp: %v", err)
			w.filteredCount++
			return nil
		}
		if ts.IsZero() {
			logging.Debug("Event skipped: missing timestamp")
			w.filteredCount++
			return nil
		}
		// Filter: include if ts >= from AND ts < to
		if ts.Before(w.from) || !ts.Before(w.to) {
			w.filteredCount++
			return nil
		}
	}

	return w.writeData(data)
}

// writeData writes data to the buffer (must be called with lock held)
func (w *JSONLWriter) writeData(data json.RawMessage) error {
	if _, err := w.writer.Write(data); err != nil {
		return err
	}
	if err := w.writer.WriteByte('\n'); err != nil {
		return err
	}
	w.writtenCount++
	return nil
}

// WriteAny writes any value as JSON
func (w *JSONLWriter) WriteAny(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}
	return w.Write(data)
}

// Count returns the number of items written
func (w *JSONLWriter) Count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writtenCount
}

// FilteredCount returns the number of events that were filtered out
func (w *JSONLWriter) FilteredCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.filteredCount
}

// Close flushes the buffer and closes the writer
func (w *JSONLWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

	// Flush buffered data before closing
	if err := w.writer.Flush(); err != nil {
		w.file.Close() // Still try to close file
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	return w.file.Close()
}

// FileManager manages output files for multiple devices
type FileManager struct {
	outputDir string
}

// NewFileManager creates a new file manager
func NewFileManager(outputDir string) (*FileManager, error) {
	// Ensure output directory exists
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	return &FileManager{
		outputDir: outputDir,
	}, nil
}

// GetWriter returns a new writer for a device with date filtering.
// The caller is responsible for closing the writer when done.
func (fm *FileManager) GetWriter(hostname, machineID string, from, to time.Time) (*JSONLWriter, string, error) {
	// Generate filename: <hostname>_<machineId>_timeline.jsonl
	filename := fmt.Sprintf("%s_%s_timeline.jsonl",
		sanitizeFilename(hostname),
		machineID)

	path := filepath.Join(fm.outputDir, filename)

	writer, err := NewJSONLWriter(path, from, to)
	if err != nil {
		return nil, "", err
	}

	return writer, path, nil
}

// GetChunkWriter returns a new writer for a chunk file with date filtering.
// The caller is responsible for closing the writer when done.
// File naming: {hostname}_{machineId}_timeline_chunk_{N}.jsonl
func (fm *FileManager) GetChunkWriter(hostname, machineID string, chunkIndex int, from, to time.Time) (*JSONLWriter, string, error) {
	// Generate chunk filename
	filename := fmt.Sprintf("%s_%s_timeline_chunk_%d.jsonl",
		sanitizeFilename(hostname),
		machineID,
		chunkIndex)

	path := filepath.Join(fm.outputDir, filename)

	writer, err := NewJSONLWriter(path, from, to)
	if err != nil {
		return nil, "", err
	}

	return writer, path, nil
}

// GetFinalPath returns the final output path for a device (used after merging chunks)
func (fm *FileManager) GetFinalPath(hostname, machineID string) string {
	filename := fmt.Sprintf("%s_%s_timeline.jsonl",
		sanitizeFilename(hostname),
		machineID)
	return filepath.Join(fm.outputDir, filename)
}

// GetIdentityWriter returns a new writer for an identity timeline.
// No date filtering is performed - the API guarantees events are within [from, to).
// File naming: {accountName}_{accountDomain}_identity_timeline.jsonl
// The caller is responsible for closing the writer when done.
func (fm *FileManager) GetIdentityWriter(accountName, accountDomain string) (*JSONLWriter, string, error) {
	filename := fmt.Sprintf("%s_%s_identity_timeline.jsonl",
		sanitizeFilename(accountName),
		sanitizeFilename(accountDomain))

	path := filepath.Join(fm.outputDir, filename)

	writer, err := NewIdentityJSONLWriter(path)
	if err != nil {
		return nil, "", err
	}

	return writer, path, nil
}

// GetIdentityChunkWriter returns a new writer for an identity chunk file.
// No date filtering is performed - the API guarantees events are within [from, to).
// File naming: {accountName}_{accountDomain}_identity_timeline_chunk_{N}.jsonl
// The caller is responsible for closing the writer when done.
func (fm *FileManager) GetIdentityChunkWriter(accountName, accountDomain string, chunkIndex int) (*JSONLWriter, string, error) {
	filename := fmt.Sprintf("%s_%s_identity_timeline_chunk_%d.jsonl",
		sanitizeFilename(accountName),
		sanitizeFilename(accountDomain),
		chunkIndex)

	path := filepath.Join(fm.outputDir, filename)

	writer, err := NewIdentityJSONLWriter(path)
	if err != nil {
		return nil, "", err
	}

	return writer, path, nil
}

// GetIdentityFinalPath returns the final output path for an identity (used after merging chunks)
func (fm *FileManager) GetIdentityFinalPath(accountName, accountDomain string) string {
	filename := fmt.Sprintf("%s_%s_identity_timeline.jsonl",
		sanitizeFilename(accountName),
		sanitizeFilename(accountDomain))
	return filepath.Join(fm.outputDir, filename)
}

// OutputDir returns the output directory
func (fm *FileManager) OutputDir() string {
	return fm.outputDir
}

// sanitizeFilename replaces invalid filename characters with underscores
func sanitizeFilename(name string) string {
	invalid := []string{"/", "\\", ":", "*", "?", "\"", "<", ">", "|", " "}
	result := name
	for _, char := range invalid {
		result = strings.ReplaceAll(result, char, "_")
	}
	return result
}
