package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/matthieugras/timeline-downloader/internal/api"
	"github.com/matthieugras/timeline-downloader/internal/backoff"
	"github.com/matthieugras/timeline-downloader/internal/logging"
	"github.com/matthieugras/timeline-downloader/internal/output"
	"golang.org/x/sync/singleflight"
)

// PoolConfig configures the worker pool
type PoolConfig struct {
	NumWorkers  int
	Client      *api.Client
	Backoff     *backoff.GlobalBackoff
	FileManager *output.FileManager
	FromDate    time.Time
	ToDate      time.Time
}

// chunkTracker tracks chunk completion for a single device
type chunkTracker struct {
	hostname    string
	machineID   string
	chunkFiles  []string // indexed by chunk index, empty string if failed
	totalChunks int
	completed   int // number of chunks completed (success or failure)
}

// deviceGroup tracks the state of all jobs for a single device.
// If any chunk fails, the entire device group is cancelled.
type deviceGroup struct {
	ctx        context.Context
	cancel     context.CancelFunc
	failed     bool   // true if any chunk failed
	failReason string // reason for failure (for logging)
}

// Pool manages a pool of workers using pond.
// When chunked jobs are used, the worker that completes the last chunk
// automatically performs the merge, eliminating coordination overhead.
type Pool struct {
	pond       pond.Pool
	numWorkers int

	// Dependencies
	client      *api.Client
	backoff     *backoff.GlobalBackoff
	fileManager *output.FileManager
	fromDate    time.Time
	toDate      time.Time

	// Results channel
	results chan JobResult

	// Device cache - resolves device once per DeviceInput.Value, reuses for all chunks
	deviceCacheMu   sync.RWMutex
	deviceCache     map[string]*api.Device // keyed by DeviceInput.Value
	deviceResolveGr singleflight.Group     // Deduplicates concurrent resolve requests

	// Chunk tracking for automatic merging
	chunkTrackersMu sync.Mutex
	chunkTrackers   map[string]*chunkTracker // keyed by DeviceKey

	// Device group tracking - allows cancelling all chunks for a device on failure
	deviceGroupsMu sync.Mutex
	deviceGroups   map[string]*deviceGroup // keyed by DeviceInput.Value

	// Status tracking
	statusMu      sync.RWMutex
	workerStatus  map[int]*WorkerStatus
	statusUpdates chan WorkerStatus
	workerIDPool  chan int // Pool of reusable worker IDs

	ctx    context.Context
	cancel context.CancelFunc
}

// NewPool creates a new worker pool using pond.
func NewPool(cfg PoolConfig) *Pool {
	ctx, cancel := context.WithCancel(context.Background())

	pondPool := pond.NewPool(cfg.NumWorkers)

	// Create pool of reusable worker IDs for status tracking
	workerIDPool := make(chan int, cfg.NumWorkers)
	for i := range cfg.NumWorkers {
		workerIDPool <- i
	}

	p := &Pool{
		pond:          pondPool,
		numWorkers:    cfg.NumWorkers,
		client:        cfg.Client,
		backoff:       cfg.Backoff,
		fileManager:   cfg.FileManager,
		fromDate:      cfg.FromDate,
		toDate:        cfg.ToDate,
		results:       make(chan JobResult, cfg.NumWorkers*2),
		deviceCache:   make(map[string]*api.Device),
		chunkTrackers: make(map[string]*chunkTracker),
		deviceGroups:  make(map[string]*deviceGroup),
		workerStatus:  make(map[int]*WorkerStatus),
		statusUpdates: make(chan WorkerStatus, cfg.NumWorkers*10),
		workerIDPool:  workerIDPool,
		ctx:           ctx,
		cancel:        cancel,
	}

	return p
}

// Submit adds a job to the pool.
func (p *Pool) Submit(job *Job) {
	p.pond.Submit(func() {
		p.executeJob(job)
	})
}

// executeJob processes a single job and sends results
func (p *Pool) executeJob(job *Job) {
	// Acquire a worker ID from the pool (blocks until one is available)
	workerID := <-p.workerIDPool
	defer func() {
		p.workerIDPool <- workerID
	}()

	// Set initial status
	p.updateStatusWithJob(workerID, WorkerStateWorking, job.DeviceInput.Value, job, 0, job.FromDate)
	defer p.updateStatusWithJob(workerID, WorkerStateIdle, "", nil, 0, time.Time{})

	result := p.processJob(workerID, job)

	// Send result
	select {
	case p.results <- result:
	case <-p.ctx.Done():
		return
	}

	// If this was a chunked job that succeeded, check if we should merge
	if job.ChunkInfo != nil {
		mergeResult := p.trackChunkAndMaybemerge(workerID, job, &result)
		if mergeResult != nil {
			select {
			case p.results <- *mergeResult:
			case <-p.ctx.Done():
			}
		}
	}
}

// trackChunkAndMaybemerge tracks chunk completion and performs merge if this was the last chunk.
// Returns a merge result if merge was performed, nil otherwise.
func (p *Pool) trackChunkAndMaybemerge(workerID int, job *Job, result *JobResult) *JobResult {
	key := job.ChunkInfo.DeviceKey

	p.chunkTrackersMu.Lock()

	tracker, exists := p.chunkTrackers[key]
	if !exists {
		tracker = &chunkTracker{
			chunkFiles:  make([]string, job.ChunkInfo.TotalChunks),
			totalChunks: job.ChunkInfo.TotalChunks,
		}
		p.chunkTrackers[key] = tracker
	}

	// Record this chunk's result
	if result.Error == nil && result.Device != nil {
		tracker.hostname = result.Device.ComputerDNSName
		tracker.machineID = result.Device.MachineID
		tracker.chunkFiles[job.ChunkInfo.ChunkIndex] = result.OutputFile
	}
	tracker.completed++

	// Check if this is the last chunk
	isLastChunk := tracker.completed == tracker.totalChunks
	if !isLastChunk {
		p.chunkTrackersMu.Unlock()
		return nil
	}

	// This is the last chunk - copy data and release lock before merging
	hostname := tracker.hostname
	machineID := tracker.machineID
	chunkFiles := make([]string, len(tracker.chunkFiles))
	copy(chunkFiles, tracker.chunkFiles)

	// Clean up tracker
	delete(p.chunkTrackers, key)
	p.chunkTrackersMu.Unlock()

	// Don't merge if we don't have hostname (all chunks failed)
	if hostname == "" {
		return nil
	}

	// Collect valid chunk files
	var validFiles []string
	for _, f := range chunkFiles {
		if f != "" {
			validFiles = append(validFiles, f)
		}
	}

	if len(validFiles) == 0 {
		return nil
	}

	// Check for partial completion - abort if some chunks failed
	if len(validFiles) != len(chunkFiles) {
		failedCount := len(chunkFiles) - len(validFiles)
		logging.Error("Partial merge aborted for %s: %d/%d chunks failed",
			hostname, failedCount, len(chunkFiles))
		return &JobResult{
			Job: &Job{
				ID:   -1,
				Type: JobTypeMerge,
				MergeInfo: &MergeInfo{
					Hostname: hostname,
				},
			},
			Error: fmt.Errorf("merge aborted: %d/%d chunks failed", failedCount, len(chunkFiles)),
		}
	}

	// Perform merge
	return p.performMerge(workerID, hostname, machineID, validFiles)
}

// performMerge merges chunk files into the final output file
func (p *Pool) performMerge(workerID int, hostname, machineID string, chunkFiles []string) *JobResult {
	start := time.Now()

	outputPath := p.fileManager.GetFinalPath(hostname, machineID)
	totalBytes, _ := output.CalculateTotalBytes(chunkFiles)

	logging.Info("Starting merge for %s: %d chunk files -> %s", hostname, len(chunkFiles), outputPath)

	// Update status to merging
	p.updateMergeStatus(workerID, hostname, 0, totalBytes)

	// Progress callback
	progressCallback := func(bytesCopied, total int64) {
		p.updateMergeStatus(workerID, hostname, bytesCopied, total)
	}

	bytesWritten, err := output.MergeChunkFilesWithProgress(
		chunkFiles,
		outputPath,
		true, // deleteChunks
		progressCallback,
	)

	// Create merge job for the result
	mergeJob := &Job{
		ID:   -1, // Merge jobs don't have IDs in this model
		Type: JobTypeMerge,
		MergeInfo: &MergeInfo{
			ChunkFiles: chunkFiles,
			OutputPath: outputPath,
			TotalBytes: totalBytes,
			Hostname:   hostname,
		},
	}

	result := JobResult{
		Job:        mergeJob,
		OutputFile: outputPath,
		Duration:   time.Since(start),
	}

	if err != nil {
		result.Error = fmt.Errorf("merge failed: %w", err)
		logging.Error("Merge failed for %s: %v", hostname, err)
	} else {
		logging.Info("Merged %s: %d bytes written to %s", hostname, bytesWritten, outputPath)
	}

	return &result
}

// SubmitAll submits multiple jobs
func (p *Pool) SubmitAll(jobs []*Job) {
	for _, job := range jobs {
		p.Submit(job)
	}
}

// Results returns channel of completed results
func (p *Pool) Results() <-chan JobResult {
	return p.results
}

// StatusUpdates returns channel of worker status updates
func (p *Pool) StatusUpdates() <-chan WorkerStatus {
	return p.statusUpdates
}

// GetWorkerStatus returns the current status of all workers
func (p *Pool) GetWorkerStatus() []WorkerStatus {
	p.statusMu.RLock()
	defer p.statusMu.RUnlock()

	status := make([]WorkerStatus, 0, len(p.workerStatus))
	for _, ws := range p.workerStatus {
		status = append(status, *ws)
	}
	return status
}

// StopAndWait gracefully shuts down the pool
func (p *Pool) StopAndWait() {
	p.cancel()
	p.pond.StopAndWait()
	close(p.results)
	close(p.statusUpdates)
}

// Stop immediately stops the pool
func (p *Pool) Stop() {
	p.cancel()
	p.pond.Stop()
}

// getOrCreateDeviceGroup returns the device group for the given device key,
// creating one if it doesn't exist.
func (p *Pool) getOrCreateDeviceGroup(deviceKey string) *deviceGroup {
	p.deviceGroupsMu.Lock()
	defer p.deviceGroupsMu.Unlock()

	if group, exists := p.deviceGroups[deviceKey]; exists {
		return group
	}

	// Create new device group with a cancellable context derived from the pool context
	ctx, cancel := context.WithCancel(p.ctx)
	group := &deviceGroup{
		ctx:    ctx,
		cancel: cancel,
	}
	p.deviceGroups[deviceKey] = group
	return group
}

// markDeviceFailed marks a device as failed and cancels all pending jobs for it.
// Returns true if this call marked it as failed (first failure), false if already failed.
func (p *Pool) markDeviceFailed(deviceKey string, reason string) bool {
	p.deviceGroupsMu.Lock()
	defer p.deviceGroupsMu.Unlock()

	group, exists := p.deviceGroups[deviceKey]
	if !exists {
		return false
	}

	if group.failed {
		return false // Already marked as failed
	}

	group.failed = true
	group.failReason = reason
	group.cancel() // Cancel the context to abort in-flight requests

	logging.Error("Device %s: all processing cancelled due to error: %s", deviceKey, reason)
	return true
}

// isDeviceFailed checks if the device has been marked as failed.
func (p *Pool) isDeviceFailed(deviceKey string) (bool, string) {
	p.deviceGroupsMu.Lock()
	defer p.deviceGroupsMu.Unlock()

	if group, exists := p.deviceGroups[deviceKey]; exists && group.failed {
		return true, group.failReason
	}
	return false, ""
}

func (p *Pool) updateStatusWithJob(id int, state WorkerState, device string, job *Job, progress int, currentDate time.Time) {
	status := WorkerStatus{
		ID:            id,
		State:         state,
		CurrentDevice: device,
		Progress:      progress,
		CurrentDate:   currentDate,
		FromDate:      p.fromDate,
		ToDate:        p.toDate,
	}

	if job != nil {
		status.JobID = job.ID
		status.FromDate = job.FromDate
		status.ToDate = job.ToDate
		if job.ChunkInfo != nil {
			status.ChunkLabel = job.ChunkInfo.ChunkLabel()
		}
	}

	p.statusMu.Lock()
	p.workerStatus[id] = &status
	p.statusMu.Unlock()

	// Non-blocking send to status updates channel
	select {
	case p.statusUpdates <- status:
	default:
	}
}

func (p *Pool) processJob(workerID int, job *Job) JobResult {
	start := time.Now()
	result := JobResult{Job: job}
	deviceKey := job.DeviceInput.Value

	// Get or create device group for cancellation support
	group := p.getOrCreateDeviceGroup(deviceKey)

	// Check if device has already failed (another chunk encountered an error)
	if failed, reason := p.isDeviceFailed(deviceKey); failed {
		result.Error = fmt.Errorf("skipped: device processing cancelled due to earlier error: %s", reason)
		result.Duration = time.Since(start)
		return result
	}

	// Use the device group's context (cancelled if any chunk for this device fails)
	ctx := group.ctx

	// Check context
	if ctx.Err() != nil {
		result.Error = ctx.Err()
		result.Duration = time.Since(start)
		return result
	}

	// Wait if backoff is active
	if p.backoff.IsBackingOff() {
		p.updateStatusWithJob(workerID, WorkerStateBackingOff, job.DeviceInput.Value, job, 0, job.FromDate)
		if err := p.backoff.WaitIfNeeded(ctx); err != nil {
			result.Error = err
			result.Duration = time.Since(start)
			return result
		}
		p.updateStatusWithJob(workerID, WorkerStateWorking, job.DeviceInput.Value, job, 0, job.FromDate)
	}

	// Step 1: Resolve device (use cache to avoid redundant API calls for chunked jobs)
	device, err := p.getOrResolveDevice(ctx, job.DeviceInput)
	if err != nil {
		result.Error = fmt.Errorf("device resolution failed: %w", err)
		logging.Error("Device resolution failed for %s: %v", job.DeviceInput.Value, err)
		result.Duration = time.Since(start)

		// Check if this is a fatal error (e.g., auth failure) - stop entire pool
		var apiErr *api.APIError
		if errors.As(err, &apiErr) && apiErr.Fatal {
			result.Fatal = true
			logging.Error("Fatal error encountered, stopping all jobs")
			p.cancel() // Stop the pool
		} else {
			// Non-fatal error - cancel only this device's jobs
			p.markDeviceFailed(deviceKey, err.Error())
		}
		return result
	}
	result.Device = device

	// Update status to show resolved DNS name instead of input value
	p.updateStatusWithJob(workerID, WorkerStateWorking, device.ComputerDNSName, job, 0, job.FromDate)

	// Step 2: Get writer for this device (with built-in date filtering)
	var writer *output.JSONLWriter
	var outputPath string

	if job.ChunkInfo != nil {
		// Chunked job: use chunk file
		writer, outputPath, err = p.fileManager.GetChunkWriter(
			device.ComputerDNSName,
			device.MachineID,
			job.ChunkInfo.ChunkIndex,
			job.FromDate,
			job.ToDate,
		)
		if err != nil {
			result.Error = fmt.Errorf("failed to create chunk file: %w", err)
			logging.Error("Failed to create chunk file for %s: %v", device.ComputerDNSName, err)
			result.Duration = time.Since(start)
			p.markDeviceFailed(deviceKey, err.Error())
			return result
		}
		logging.Info("Chunk file: %s (chunk %d/%d)", outputPath, job.ChunkInfo.ChunkIndex+1, job.ChunkInfo.TotalChunks)
	} else {
		// Regular job: use normal file
		writer, outputPath, err = p.fileManager.GetWriter(device.ComputerDNSName, device.MachineID, job.FromDate, job.ToDate)
		if err != nil {
			result.Error = fmt.Errorf("failed to create output file: %w", err)
			logging.Error("Failed to create output file for %s: %v", device.ComputerDNSName, err)
			result.Duration = time.Since(start)
			p.markDeviceFailed(deviceKey, err.Error())
			return result
		}
		logging.Info("Output file: %s", outputPath)
	}
	result.OutputFile = outputPath

	// Ensure writer is closed when done (flushes buffers)
	defer func() {
		if err := writer.Close(); err != nil {
			logging.Error("Failed to close writer for %s: %v", device.ComputerDNSName, err)
		}
	}()

	// Step 3: Download timeline with progress callback
	logging.Info("Downloading timeline for %s from %s to %s",
		device.ComputerDNSName, job.FromDate.Format("2006-01-02"), job.ToDate.Format("2006-01-02"))
	progressCallback := func(eventCount int, currentDate time.Time) {
		p.updateStatusWithJob(workerID, WorkerStateWorking, device.ComputerDNSName, job, eventCount, currentDate)
	}

	eventCount, err := p.client.DownloadTimeline(
		ctx,
		device,
		job.FromDate,
		job.ToDate,
		writer,
		progressCallback,
	)
	if err != nil {
		result.Error = fmt.Errorf("timeline download failed: %w", err)
		logging.Error("Timeline download failed for %s: %v", device.ComputerDNSName, err)
		result.Duration = time.Since(start)

		// Check if this is a fatal error (e.g., auth failure) - stop entire pool
		var apiErr *api.APIError
		if errors.As(err, &apiErr) && apiErr.Fatal {
			result.Fatal = true
			logging.Error("Fatal error encountered, stopping all jobs")
			p.cancel() // Stop the pool
		} else if !errors.Is(err, context.Canceled) {
			// Non-fatal, non-cancellation error - cancel only this device's jobs
			p.markDeviceFailed(deviceKey, err.Error())
		}
		return result
	}
	logging.Info("Downloaded %d events for %s", eventCount, device.ComputerDNSName)

	result.EventCount = eventCount
	result.Duration = time.Since(start)
	return result
}

func (p *Pool) updateMergeStatus(id int, hostname string, bytesCopied, totalBytes int64) {
	status := WorkerStatus{
		ID:            id,
		State:         WorkerStateMerging,
		CurrentDevice: hostname,
		BytesCopied:   bytesCopied,
		TotalBytes:    totalBytes,
	}

	p.statusMu.Lock()
	p.workerStatus[id] = &status
	p.statusMu.Unlock()

	// Non-blocking send to status updates channel
	select {
	case p.statusUpdates <- status:
	default:
	}
}

// getOrResolveDevice returns a cached device or resolves it via API.
// Uses singleflight to deduplicate concurrent requests for the same device.
func (p *Pool) getOrResolveDevice(ctx context.Context, input api.DeviceInput) (*api.Device, error) {
	key := input.Value

	// Check cache first (read lock)
	p.deviceCacheMu.RLock()
	if device, ok := p.deviceCache[key]; ok {
		p.deviceCacheMu.RUnlock()
		logging.Info("Using cached device: %s (MachineID: %s)", device.ComputerDNSName, device.MachineID)
		return device, nil
	}
	p.deviceCacheMu.RUnlock()

	// Use singleflight to deduplicate concurrent resolution requests
	result, err, _ := p.deviceResolveGr.Do(key, func() (any, error) {
		// Double-check cache inside singleflight (another request may have just completed)
		p.deviceCacheMu.RLock()
		if device, ok := p.deviceCache[key]; ok {
			p.deviceCacheMu.RUnlock()
			return device, nil
		}
		p.deviceCacheMu.RUnlock()

		// Resolve device
		logging.Info("Resolving device: %s", input.Value)
		device, err := p.client.ResolveDevice(ctx, input)
		if err != nil {
			return nil, err
		}
		logging.Info("Resolved device %s -> MachineID: %s, SenseVersion: %s",
			device.ComputerDNSName, device.MachineID, device.SenseClientVersion)

		// Store in cache
		p.deviceCacheMu.Lock()
		p.deviceCache[key] = device
		p.deviceCacheMu.Unlock()

		return device, nil
	})

	if err != nil {
		return nil, err
	}
	return result.(*api.Device), nil
}
