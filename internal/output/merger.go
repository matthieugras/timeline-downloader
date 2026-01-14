package output

import (
	"fmt"
	"io"
	"os"
)

// MergeProgressCallback is called periodically during merge with bytes copied and total bytes
type MergeProgressCallback func(bytesCopied, totalBytes int64)

// countingWriter wraps a writer and tracks bytes written
type countingWriter struct {
	writer      io.Writer
	bytesCopied int64
	totalBytes  int64
	callback    MergeProgressCallback
	callbackAt  int64 // Call callback every N bytes
	lastCall    int64 // Bytes at last callback
}

func newCountingWriter(w io.Writer, totalBytes int64, callback MergeProgressCallback) *countingWriter {
	return &countingWriter{
		writer:     w,
		totalBytes: totalBytes,
		callback:   callback,
		callbackAt: 1024 * 1024, // Call every 1MB
	}
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.writer.Write(p)
	c.bytesCopied += int64(n)

	// Call callback periodically
	if c.callback != nil && c.bytesCopied-c.lastCall >= c.callbackAt {
		c.callback(c.bytesCopied, c.totalBytes)
		c.lastCall = c.bytesCopied
	}

	return n, err
}

// MergeChunkFiles merges multiple chunk JSONL files into a single output file.
// Files are concatenated in order using streaming byte copy (no memory buffering).
// Returns the total bytes written.
func MergeChunkFiles(chunkFiles []string, outputPath string, deleteChunks bool) (int64, error) {
	return MergeChunkFilesWithProgress(chunkFiles, outputPath, deleteChunks, nil)
}

// MergeChunkFilesWithProgress merges chunk files with progress reporting.
// The callback is called periodically with bytes copied and total bytes.
func MergeChunkFilesWithProgress(chunkFiles []string, outputPath string, deleteChunks bool, callback MergeProgressCallback) (int64, error) {
	if len(chunkFiles) == 0 {
		return 0, nil
	}

	// Calculate total bytes upfront
	var totalBytes int64
	for _, chunkFile := range chunkFiles {
		info, err := os.Stat(chunkFile)
		if err != nil {
			return 0, fmt.Errorf("failed to stat chunk file %s: %w", chunkFile, err)
		}
		totalBytes += info.Size()
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create output file %s: %w", outputPath, err)
	}
	defer outFile.Close()

	// Use counting writer if callback provided
	var writer io.Writer = outFile
	if callback != nil {
		writer = newCountingWriter(outFile, totalBytes, callback)
		// Initial callback
		callback(0, totalBytes)
	}

	var bytesWritten int64
	for _, chunkFile := range chunkFiles {
		n, err := appendFileToWriter(writer, chunkFile)
		if err != nil {
			return bytesWritten, err
		}
		bytesWritten += n
	}

	// Final callback
	if callback != nil {
		callback(bytesWritten, totalBytes)
	}

	// Delete chunk files if requested
	if deleteChunks {
		for _, chunkFile := range chunkFiles {
			os.Remove(chunkFile)
		}
	}

	return bytesWritten, nil
}

// CalculateTotalBytes returns the total size of all chunk files
func CalculateTotalBytes(chunkFiles []string) (int64, error) {
	var total int64
	for _, f := range chunkFiles {
		info, err := os.Stat(f)
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

// appendFileToWriter streams bytes from src file to dst writer
func appendFileToWriter(dst io.Writer, srcPath string) (int64, error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open chunk file %s: %w", srcPath, err)
	}
	defer src.Close()

	n, err := io.Copy(dst, src)
	if err != nil {
		return n, fmt.Errorf("failed to copy chunk file %s: %w", srcPath, err)
	}
	return n, nil
}
