package output

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
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

// TruncatableWriter extends EventWriter with truncation capability.
// Used by identity timeline downloads to handle skip limit restarts.
type TruncatableWriter interface {
	EventWriter
	TruncateLastLines(n int) error
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
	gzipWriter       *gzip.Writer  // nil if not compressing
	writer           *bufio.Writer // Buffered writer for better I/O performance
	from             time.Time
	to               time.Time
	timestampExtract TimestampExtractor // nil = no filtering
	mu               sync.Mutex

	writtenCount  int
	filteredCount int
	closed        bool
}

// NewJSONLWriter creates a new JSONL writer at the specified path.
// If filterByActionTime is true, events are filtered by ActionTime in [from, to).
// If filterByActionTime is false, all events are written (from/to ignored).
// If useGzip is true, the output is compressed with gzip.
func NewJSONLWriter(path string, from, to time.Time, filterByActionTime bool, useGzip bool) (*JSONLWriter, error) {
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

	var extractor TimestampExtractor
	if filterByActionTime {
		extractor = getEventActionTime
	}

	var gzipWriter *gzip.Writer
	var baseWriter io.Writer = file

	if useGzip {
		gzipWriter = gzip.NewWriter(file)
		baseWriter = gzipWriter
	}

	return &JSONLWriter{
		file:             file,
		gzipWriter:       gzipWriter,
		writer:           bufio.NewWriterSize(baseWriter, 64*1024), // 64KB buffer
		from:             from,
		to:               to,
		timestampExtract: extractor,
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

	// Close gzip writer if used (flushes compression buffer)
	if w.gzipWriter != nil {
		if err := w.gzipWriter.Close(); err != nil {
			w.file.Close()
			return fmt.Errorf("failed to close gzip writer: %w", err)
		}
	}

	return w.file.Close()
}

// TruncateLastLines removes the last n lines from the output file.
// Only works for uncompressed files (chunk files).
// Flushes buffer, seeks backwards to find n newlines, truncates file.
func (w *JSONLWriter) TruncateLastLines(n int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	if n <= 0 {
		return nil
	}

	if w.gzipWriter != nil {
		return fmt.Errorf("truncation not supported for gzip-compressed files")
	}

	// Flush the buffer to ensure all data is written to file
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	// Get current file size
	fileSize, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end: %w", err)
	}

	if fileSize == 0 {
		return nil
	}

	// Each line in JSONL format ends with \n, so the file looks like:
	// line1\nline2\nline3\n
	// To remove the last n lines, we need to find n+1 newlines from the end
	// (the extra one accounts for the trailing newline of the last remaining line)
	// and truncate right after that newline.
	//
	// Example: "line1\nline2\nline3\n" (positions 0-17)
	// - newlines at positions: 5, 11, 17
	// - to remove 1 line: find 2nd newline from end (pos 11), truncate at 12 -> "line1\nline2\n"
	// - to remove 2 lines: find 3rd newline from end (pos 5), truncate at 6 -> "line1\n"
	const bufSize = 8192
	buf := make([]byte, bufSize)
	newlinesFound := 0
	truncatePos := int64(0)
	pos := fileSize
	needNewlines := n + 1 // Need n+1 newlines to remove n lines

	for pos > 0 && newlinesFound < needNewlines {
		readSize := min(int64(bufSize), pos)
		pos -= readSize

		if _, err := w.file.Seek(pos, io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek: %w", err)
		}

		bytesRead, err := w.file.Read(buf[:readSize])
		if err != nil {
			return fmt.Errorf("failed to read: %w", err)
		}

		// Scan backwards through this chunk
		for i := bytesRead - 1; i >= 0 && newlinesFound < needNewlines; i-- {
			if buf[i] == '\n' {
				newlinesFound++
				if newlinesFound == needNewlines {
					// Found the (n+1)th newline from end, truncate right after it
					truncatePos = pos + int64(i) + 1
					break
				}
			}
		}
	}

	// If we found fewer newlines than needed, truncate entire file
	if newlinesFound < needNewlines {
		truncatePos = 0
	}

	// Truncate the file
	if err := w.file.Truncate(truncatePos); err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}

	// Seek to new end position
	if _, err := w.file.Seek(truncatePos, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to truncate position: %w", err)
	}

	// Reset the bufio.Writer to the new position
	w.writer.Reset(w.file)

	// Update written count
	w.writtenCount -= n
	if w.writtenCount < 0 {
		w.writtenCount = 0
	}

	return nil
}

// FileManager manages output files for multiple devices
type FileManager struct {
	outputDir string
	gzip      bool
}

// NewFileManager creates a new file manager
func NewFileManager(outputDir string, gzip bool) (*FileManager, error) {
	// Ensure output directory exists
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	return &FileManager{
		outputDir: outputDir,
		gzip:      gzip,
	}, nil
}

// Gzip returns whether gzip compression is enabled
func (fm *FileManager) Gzip() bool {
	return fm.gzip
}

// entityFilename generates a filename for an entity (device or identity).
// For identities, the display name is truncated and uses "_identity_timeline" infix.
// For devices, the display name is used as-is with "_timeline" infix.
// If chunkIndex >= 0, appends "_chunk_{N}" before the extension (chunks are never gzipped).
// If useGzip is true and chunkIndex < 0, uses .jsonl.gz extension.
func entityFilename(displayName, primaryKey string, isIdentity bool, chunkIndex int, useGzip bool) string {
	var sanitizedName, infix string
	if isIdentity {
		sanitizedName = sanitizeFilename(truncateDisplayName(displayName, maxDisplayNameLen))
		infix = "_identity_timeline"
	} else {
		sanitizedName = sanitizeFilename(displayName)
		infix = "_timeline"
	}
	primaryKeySafe := sanitizeFilename(primaryKey)

	if chunkIndex >= 0 {
		// Chunk files are never gzipped
		return fmt.Sprintf("%s_%s%s_chunk_%d.jsonl", sanitizedName, primaryKeySafe, infix, chunkIndex)
	}

	ext := ".jsonl"
	if useGzip {
		ext = ".jsonl.gz"
	}
	return fmt.Sprintf("%s_%s%s%s", sanitizedName, primaryKeySafe, infix, ext)
}

// GetWriter returns a new writer for an entity.
// For devices (isIdentity=false): filters events by [from, to) using ActionTime
// For identities (isIdentity=true): no filtering (from/to ignored)
// The caller is responsible for closing the writer when done.
func (fm *FileManager) GetWriter(displayName, primaryKey string, from, to time.Time, isIdentity bool) (*JSONLWriter, string, error) {
	filename := entityFilename(displayName, primaryKey, isIdentity, -1, fm.gzip)
	path := filepath.Join(fm.outputDir, filename)

	writer, err := NewJSONLWriter(path, from, to, !isIdentity, fm.gzip)
	if err != nil {
		return nil, "", err
	}
	return writer, path, nil
}

// GetChunkWriter returns a new writer for a chunk file.
// For devices (isIdentity=false): filters events by [from, to)
// For identities (isIdentity=true): no filtering (from/to ignored)
// Chunk files are never gzipped to allow efficient streaming merges.
// The caller is responsible for closing the writer when done.
func (fm *FileManager) GetChunkWriter(displayName, primaryKey string, chunkIndex int, from, to time.Time, isIdentity bool) (*JSONLWriter, string, error) {
	filename := entityFilename(displayName, primaryKey, isIdentity, chunkIndex, false) // chunks never gzipped
	path := filepath.Join(fm.outputDir, filename)

	writer, err := NewJSONLWriter(path, from, to, !isIdentity, false) // chunks never gzipped
	if err != nil {
		return nil, "", err
	}
	return writer, path, nil
}

// GetFinalPath returns the final output path for an entity (used after merging chunks)
func (fm *FileManager) GetFinalPath(displayName, primaryKey string, isIdentity bool) string {
	filename := entityFilename(displayName, primaryKey, isIdentity, -1, fm.gzip)
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

// truncateDisplayName truncates a display name to maxLen characters for use in filenames.
// Simple cutoff without ellipsis to avoid special characters in filenames.
func truncateDisplayName(name string, maxLen int) string {
	if len(name) <= maxLen {
		return name
	}
	return name[:maxLen]
}

// maxDisplayNameLen is the maximum length for display names in output filenames
const maxDisplayNameLen = 30
