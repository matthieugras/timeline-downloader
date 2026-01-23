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

// EntityType distinguishes device vs identity jobs
type EntityType int

const (
	EntityTypeDevice EntityType = iota
	EntityTypeIdentity
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

// Job represents a timeline download job (device or identity)
type Job struct {
	ID            int
	Type          JobType
	EntityType    EntityType        // Device or Identity
	DeviceInput   api.DeviceInput   // Used when EntityType == EntityTypeDevice
	IdentityInput api.IdentityInput // Used when EntityType == EntityTypeIdentity
	FromDate      time.Time
	ToDate        time.Time
	ChunkInfo     *ChunkInfo // nil if not chunked
	MergeInfo     *MergeInfo // nil unless JobTypeMerge
}

// EntityKey returns a unique key for this job's entity (for caching/grouping).
// Keys are prefixed by type to avoid collisions.
func (j *Job) EntityKey() string {
	if j.EntityType == EntityTypeIdentity {
		return "identity:" + j.IdentityInput.Value
	}
	return "device:" + j.DeviceInput.Value
}

// EntityDisplayName returns a display name for the entity (for UI)
func (j *Job) EntityDisplayName() string {
	if j.EntityType == EntityTypeIdentity {
		return j.IdentityInput.Value
	}
	return j.DeviceInput.Value
}

// JobResult represents the result of a job
type JobResult struct {
	Job           *Job
	Device        *api.Device   // Set for device jobs
	Identity      *api.Identity // Set for identity jobs
	EventCount    int
	OutputFile    string
	Error         error
	Duration      time.Duration
	Fatal         bool   // If true, abort the entire collection
	Skipped       bool   // If true, job was skipped (e.g., duplicate entity)
	SkippedReason string // Reason for skipping
}

// ResolvedName returns the resolved display name (hostname or account name)
func (r *JobResult) ResolvedName() string {
	if r.Device != nil {
		return r.Device.ComputerDNSName
	}
	if r.Identity != nil {
		if r.Identity.AccountDomain != "" {
			return r.Identity.AccountName + "@" + r.Identity.AccountDomain
		}
		return r.Identity.AccountName
	}
	return ""
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
