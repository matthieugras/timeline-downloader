package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/matthieugras/timeline-downloader/internal/api"
	"github.com/matthieugras/timeline-downloader/internal/logging"
	"github.com/matthieugras/timeline-downloader/internal/output"
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
	EntityKey   string // For grouping (device:machineId or identity:radiusUserId)
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

// Job is the interface for all download jobs
type Job interface {
	// Common properties
	JobID() int
	FromDate() time.Time
	ToDate() time.Time
	GetChunkInfo() *ChunkInfo

	// Entity-specific behavior
	EntityType() EntityType
	EntityKey() string
	EntityDisplayName() string
	TypeName() string         // "device" or "identity"
	TypeNameTitle() string    // "Device" or "Identity"
	PrimaryKeyLabel() string  // "MachineID" or "RadiusUserID"
	IsIdentity() bool
	InitialStatusDate() time.Time
	WriterDates() (time.Time, time.Time)
	InputValue() string

	// ResolveEntity resolves the entity using the provided client.
	// Pool handles caching - this is only called on cache miss.
	ResolveEntity(ctx context.Context, client *api.Client, tenantID string) (api.ResolvedEntity, error)

	// LogCachedEntity logs that a cached entity was used.
	LogCachedEntity(entity api.ResolvedEntity)

	// LogResolvedEntity logs that an entity was resolved.
	LogResolvedEntity(entity api.ResolvedEntity)

	// DownloadTimeline downloads the timeline for the given entity.
	DownloadTimeline(ctx context.Context, client *api.Client, entity api.ResolvedEntity, writer *output.JSONLWriter, progressCallback func(int, time.Time)) (int, error)
}

// baseJob contains fields common to all download jobs
type baseJob struct {
	id        int
	fromDate  time.Time
	toDate    time.Time
	chunkInfo *ChunkInfo
}

func (b *baseJob) JobID() int               { return b.id }
func (b *baseJob) FromDate() time.Time      { return b.fromDate }
func (b *baseJob) ToDate() time.Time        { return b.toDate }
func (b *baseJob) GetChunkInfo() *ChunkInfo { return b.chunkInfo }

// DeviceJob represents a device timeline download job
type DeviceJob struct {
	baseJob
	Input        api.DeviceInput
	TimelineOpts api.DeviceTimelineOptions
}

func (d *DeviceJob) EntityType() EntityType       { return EntityTypeDevice }
func (d *DeviceJob) EntityKey() string            { return "device:" + d.Input.Value }
func (d *DeviceJob) EntityDisplayName() string    { return d.Input.Value }
func (d *DeviceJob) TypeName() string             { return "device" }
func (d *DeviceJob) TypeNameTitle() string        { return "Device" }
func (d *DeviceJob) PrimaryKeyLabel() string      { return "MachineID" }
func (d *DeviceJob) IsIdentity() bool             { return false }
func (d *DeviceJob) InitialStatusDate() time.Time { return d.fromDate }
func (d *DeviceJob) WriterDates() (time.Time, time.Time) {
	return d.fromDate, d.toDate
}
func (d *DeviceJob) InputValue() string { return d.Input.Value }

func (d *DeviceJob) ResolveEntity(ctx context.Context, client *api.Client, tenantID string) (api.ResolvedEntity, error) {
	return client.ResolveDevice(ctx, d.Input)
}

func (d *DeviceJob) LogCachedEntity(entity api.ResolvedEntity) {
	device := entity.(*api.Device)
	logging.Info("using cached device",
		"hostname", device.ComputerDNSName,
		"machine_id", device.MachineID)
}

func (d *DeviceJob) LogResolvedEntity(entity api.ResolvedEntity) {
	device := entity.(*api.Device)
	logging.Info("resolved device",
		"hostname", device.ComputerDNSName,
		"machine_id", device.MachineID,
		"sense_version", device.SenseClientVersion)
}

func (d *DeviceJob) DownloadTimeline(ctx context.Context, client *api.Client, entity api.ResolvedEntity, writer *output.JSONLWriter, progressCallback func(int, time.Time)) (int, error) {
	return api.DownloadDeviceTimeline(ctx, client, entity.(*api.Device), d.fromDate, d.toDate, d.TimelineOpts, writer, progressCallback, d.id)
}

// IdentityJob represents an identity timeline download job
type IdentityJob struct {
	baseJob
	Input    api.IdentityInput
	PageSize int
}

func (i *IdentityJob) EntityType() EntityType       { return EntityTypeIdentity }
func (i *IdentityJob) EntityKey() string            { return "identity:" + i.Input.Value }
func (i *IdentityJob) EntityDisplayName() string    { return i.Input.Value }
func (i *IdentityJob) TypeName() string             { return "identity" }
func (i *IdentityJob) TypeNameTitle() string        { return "Identity" }
func (i *IdentityJob) PrimaryKeyLabel() string      { return "RadiusUserID" }
func (i *IdentityJob) IsIdentity() bool             { return true }
func (i *IdentityJob) InitialStatusDate() time.Time { return i.toDate } // newest→oldest
func (i *IdentityJob) WriterDates() (time.Time, time.Time) {
	return time.Time{}, time.Time{} // from/to ignored for identities
}
func (i *IdentityJob) InputValue() string { return i.Input.Value }

func (i *IdentityJob) ResolveEntity(ctx context.Context, client *api.Client, tenantID string) (api.ResolvedEntity, error) {
	return client.ResolveIdentityBySearch(ctx, i.Input.Value, tenantID)
}

func (i *IdentityJob) LogCachedEntity(entity api.ResolvedEntity) {
	identity := entity.(*api.Identity)
	logging.Info("using cached identity",
		"account_name", identity.AccountName,
		"account_domain", identity.AccountDomain)
}

func (i *IdentityJob) LogResolvedEntity(entity api.ResolvedEntity) {
	identity := entity.(*api.Identity)
	logging.Info("resolved identity",
		"search_term", i.Input.Value,
		"account_name", identity.AccountName,
		"account_domain", identity.AccountDomain)
}

func (i *IdentityJob) DownloadTimeline(ctx context.Context, client *api.Client, entity api.ResolvedEntity, writer *output.JSONLWriter, progressCallback func(int, time.Time)) (int, error) {
	// JSONLWriter implements TruncatableWriter
	return api.DownloadIdentityTimeline(ctx, client, entity.(*api.Identity), i.fromDate, i.toDate, i.PageSize, writer, progressCallback, i.id)
}

// MergeJob represents a chunk merge job (separate from download jobs)
type MergeJob struct {
	ID         int
	Type       JobType // Always JobTypeMerge
	Entity     EntityType
	MergeInfo  *MergeInfo
}

// JobResult represents the result of a job
type JobResult struct {
	Job           Job                // Interface type now
	MergeJob      *MergeJob          // Set for merge jobs (Job will be nil)
	Entity        api.ResolvedEntity // Set for both device and identity jobs
	EventCount    int
	OutputFile    string
	Error         error
	Duration      time.Duration
	Fatal         bool   // If true, abort the entire collection
	Skipped       bool   // If true, job was skipped (e.g., duplicate entity)
	SkippedReason string // Reason for skipping
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
	ID              int
	State           WorkerState
	CurrentEntity   string
	JobID           int
	Progress        int       // Event count
	CurrentDate     time.Time // Current date being processed
	FromDate        time.Time // Job start date
	ToDate          time.Time // Job end date
	ChunkLabel      string    // e.g., "1/4" for chunk display
	BytesCopied     int64     // For merge progress
	TotalBytes      int64     // For merge progress
	ReverseProgress bool      // True if progress moves from ToDate toward FromDate (e.g., identity timelines)
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
