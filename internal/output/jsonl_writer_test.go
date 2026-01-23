package output

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTruncateLastLines(t *testing.T) {
	tests := []struct {
		name          string
		initialLines  []string
		truncateN     int
		expectedLines []string
		wantErr       bool
	}{
		{
			name:          "truncate last 2 lines",
			initialLines:  []string{"line1", "line2", "line3", "line4", "line5"},
			truncateN:     2,
			expectedLines: []string{"line1", "line2", "line3"},
		},
		{
			name:          "truncate last 1 line",
			initialLines:  []string{"line1", "line2", "line3"},
			truncateN:     1,
			expectedLines: []string{"line1", "line2"},
		},
		{
			name:          "truncate all lines",
			initialLines:  []string{"line1", "line2", "line3"},
			truncateN:     3,
			expectedLines: []string{},
		},
		{
			name:          "truncate more than total lines",
			initialLines:  []string{"line1", "line2"},
			truncateN:     5,
			expectedLines: []string{},
		},
		{
			name:          "truncate zero lines",
			initialLines:  []string{"line1", "line2", "line3"},
			truncateN:     0,
			expectedLines: []string{"line1", "line2", "line3"},
		},
		{
			name:          "truncate empty file",
			initialLines:  []string{},
			truncateN:     2,
			expectedLines: []string{},
		},
		{
			name:          "truncate single line file",
			initialLines:  []string{"only-line"},
			truncateN:     1,
			expectedLines: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp file
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "test.jsonl")

			// Create writer (no filtering, no gzip)
			writer, err := NewJSONLWriter(path, time.Time{}, time.Time{}, false, false)
			if err != nil {
				t.Fatalf("Failed to create writer: %v", err)
			}

			// Write initial lines
			for _, line := range tt.initialLines {
				data, _ := json.Marshal(map[string]string{"content": line})
				if err := writer.Write(data); err != nil {
					t.Fatalf("Failed to write line: %v", err)
				}
			}

			// Truncate
			err = writer.TruncateLastLines(tt.truncateN)
			if (err != nil) != tt.wantErr {
				t.Fatalf("TruncateLastLines() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Close writer
			if err := writer.Close(); err != nil {
				t.Fatalf("Failed to close writer: %v", err)
			}

			// Read and verify content
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("Failed to read file: %v", err)
			}

			// Parse lines
			var actualLines []string
			if len(content) > 0 {
				lines := strings.Split(strings.TrimSuffix(string(content), "\n"), "\n")
				for _, line := range lines {
					if line == "" {
						continue
					}
					var obj map[string]string
					if err := json.Unmarshal([]byte(line), &obj); err != nil {
						t.Fatalf("Failed to parse line: %v", err)
					}
					actualLines = append(actualLines, obj["content"])
				}
			}

			// Verify
			if len(actualLines) != len(tt.expectedLines) {
				t.Errorf("Line count mismatch: got %d, want %d", len(actualLines), len(tt.expectedLines))
				t.Errorf("Got lines: %v", actualLines)
				t.Errorf("Want lines: %v", tt.expectedLines)
				return
			}

			for i, expected := range tt.expectedLines {
				if actualLines[i] != expected {
					t.Errorf("Line %d mismatch: got %q, want %q", i, actualLines[i], expected)
				}
			}
		})
	}
}

func TestTruncateLastLines_WriterCount(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.jsonl")

	writer, err := NewJSONLWriter(path, time.Time{}, time.Time{}, false, false)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write 5 lines
	for i := 0; i < 5; i++ {
		data, _ := json.Marshal(map[string]int{"num": i})
		if err := writer.Write(data); err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
	}

	if writer.Count() != 5 {
		t.Errorf("Count before truncate: got %d, want 5", writer.Count())
	}

	// Truncate 2 lines
	if err := writer.TruncateLastLines(2); err != nil {
		t.Fatalf("TruncateLastLines failed: %v", err)
	}

	if writer.Count() != 3 {
		t.Errorf("Count after truncate: got %d, want 3", writer.Count())
	}

	writer.Close()
}

func TestTruncateLastLines_ContinueWriting(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.jsonl")

	writer, err := NewJSONLWriter(path, time.Time{}, time.Time{}, false, false)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write initial lines
	for i := 1; i <= 5; i++ {
		data, _ := json.Marshal(map[string]int{"num": i})
		writer.Write(data)
	}

	// Truncate last 2
	if err := writer.TruncateLastLines(2); err != nil {
		t.Fatalf("TruncateLastLines failed: %v", err)
	}

	// Write more lines
	for i := 6; i <= 8; i++ {
		data, _ := json.Marshal(map[string]int{"num": i})
		writer.Write(data)
	}

	writer.Close()

	// Read and verify
	content, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimSuffix(string(content), "\n"), "\n")

	expected := []int{1, 2, 3, 6, 7, 8}
	if len(lines) != len(expected) {
		t.Fatalf("Line count: got %d, want %d", len(lines), len(expected))
	}

	for i, line := range lines {
		var obj map[string]int
		json.Unmarshal([]byte(line), &obj)
		if obj["num"] != expected[i] {
			t.Errorf("Line %d: got num=%d, want %d", i, obj["num"], expected[i])
		}
	}
}

func TestTruncateLastLines_GzipError(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.jsonl.gz")

	writer, err := NewJSONLWriter(path, time.Time{}, time.Time{}, false, true)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Close()

	// Write a line
	data, _ := json.Marshal(map[string]string{"test": "data"})
	writer.Write(data)

	// Truncate should fail for gzip
	err = writer.TruncateLastLines(1)
	if err == nil {
		t.Error("Expected error for gzip truncation, got nil")
	}
	if !strings.Contains(err.Error(), "gzip") {
		t.Errorf("Error should mention gzip: %v", err)
	}
}

func TestTruncateLastLines_LargeFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.jsonl")

	writer, err := NewJSONLWriter(path, time.Time{}, time.Time{}, false, false)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write many lines to exceed buffer size
	lineCount := 10000
	for i := 0; i < lineCount; i++ {
		data, _ := json.Marshal(map[string]int{"num": i})
		writer.Write(data)
	}

	// Truncate last 100 lines
	truncateCount := 100
	if err := writer.TruncateLastLines(truncateCount); err != nil {
		t.Fatalf("TruncateLastLines failed: %v", err)
	}

	writer.Close()

	// Verify line count
	content, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimSuffix(string(content), "\n"), "\n")

	expectedCount := lineCount - truncateCount
	if len(lines) != expectedCount {
		t.Errorf("Line count: got %d, want %d", len(lines), expectedCount)
	}

	// Verify last line is correct (should be num: 9899)
	var lastObj map[string]int
	json.Unmarshal([]byte(lines[len(lines)-1]), &lastObj)
	expectedLastNum := lineCount - truncateCount - 1
	if lastObj["num"] != expectedLastNum {
		t.Errorf("Last line num: got %d, want %d", lastObj["num"], expectedLastNum)
	}
}
