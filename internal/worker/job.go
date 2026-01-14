package worker

import (
	"fmt"
	"time"

	"github.com/matthieugras/timeline-downloader/internal/api"
)

// JobType distinguishes download vs merge jobs
type JobType int

const (
	JobTypeDownload JobType = iota
	JobTypeMerge
)

// ChunkInfo holds information about a time chunk for parallel processing
type ChunkInfo struct {
	ChunkIndex  int    // 0-based index
	TotalChunks int    // Total chunks for this device
	DeviceKey   string // For grouping (hostname or machineID)
}

// MergeInfo holds information for merge jobs
type MergeInfo struct {
	ChunkFiles []string // Ordered list of chunk files to merge
	OutputPath string   // Final output path
	TotalBytes int64    // Total bytes to copy (for progress)
	Hostname   string   // For display
}

// ChunkLabel returns a display label like "1/4" for UI display
func (c *ChunkInfo) ChunkLabel() string {
	if c == nil {
		return ""
	}
	return fmt.Sprintf("%d/%d", c.ChunkIndex+1, c.TotalChunks)
}

// Job represents a device timeline download job
type Job struct {
	ID          int
	Type        JobType
	DeviceInput api.DeviceInput
	FromDate    time.Time
	ToDate      time.Time
	ChunkInfo   *ChunkInfo // nil if not chunked
	MergeInfo   *MergeInfo // nil unless JobTypeMerge
}

// JobResult represents the result of a job
type JobResult struct {
	Job        *Job
	Device     *api.Device
	EventCount int
	OutputFile string
	Error      error
	Duration   time.Duration
	Fatal      bool // If true, abort the entire collection
}

// JobStatus represents the status of a job
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
)

// WorkerStatus represents the status of a worker
type WorkerStatus struct {
	ID            int
	State         WorkerState
	CurrentDevice string
	JobID         int
	Progress      int       // Event count
	CurrentDate   time.Time // Current date being processed
	FromDate      time.Time // Job start date
	ToDate        time.Time // Job end date
	ChunkLabel    string    // e.g., "1/4" for chunk display
	BytesCopied   int64     // For merge progress
	TotalBytes    int64     // For merge progress
}

// WorkerState represents the state of a worker
type WorkerState int

const (
	WorkerStateIdle WorkerState = iota
	WorkerStateWorking
	WorkerStateBackingOff
	WorkerStateMerging
	WorkerStateDone
)

func (s WorkerState) String() string {
	switch s {
	case WorkerStateIdle:
		return "idle"
	case WorkerStateWorking:
		return "working"
	case WorkerStateBackingOff:
		return "backing off"
	case WorkerStateMerging:
		return "merging"
	case WorkerStateDone:
		return "done"
	default:
		return "unknown"
	}
}
