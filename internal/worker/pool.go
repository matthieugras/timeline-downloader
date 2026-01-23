package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
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
	Context     context.Context // Parent context for cancellation
}

// chunkTracker tracks chunk completion for a single entity (device or identity)
type chunkTracker struct {
	entityType EntityType
	// Device fields
	hostname  string
	machineID string
	// Identity fields
	accountName   string
	accountDomain string
	// Common fields
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

	// Entity cache - resolves device/identity once per input value, reuses for all chunks
	// Keys are prefixed: "device:hostname" or "identity:searchterm"
	entityCacheMu   sync.RWMutex
	entityCache     map[string]api.ResolvedEntity // *api.Device or *api.Identity
	entityResolveGr singleflight.Group            // Deduplicates concurrent resolve requests

	// Primary key deduplication - different search terms may resolve to the same entity
	// Keys are primary keys: machineId for devices, radiusUserId for identities
	seenPrimaryKeysMu sync.RWMutex
	seenPrimaryKeys   map[string]string // primaryKey -> first entityKey that resolved to it

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
	// Use provided context or default to Background
	parentCtx := cfg.Context
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	ctx, cancel := context.WithCancel(parentCtx)

	pondPool := pond.NewPool(cfg.NumWorkers)

	// Create pool of reusable worker IDs for status tracking
	workerIDPool := make(chan int, cfg.NumWorkers)
	for i := range cfg.NumWorkers {
		workerIDPool <- i
	}

	p := &Pool{
		pond:            pondPool,
		numWorkers:      cfg.NumWorkers,
		client:          cfg.Client,
		backoff:         cfg.Backoff,
		fileManager:     cfg.FileManager,
		fromDate:        cfg.FromDate,
		toDate:          cfg.ToDate,
		results:         make(chan JobResult, cfg.NumWorkers*2),
		entityCache:     make(map[string]api.ResolvedEntity),
		seenPrimaryKeys: make(map[string]string),
		chunkTrackers:   make(map[string]*chunkTracker),
		deviceGroups:    make(map[string]*deviceGroup),
		workerStatus:    make(map[int]*WorkerStatus),
		// Small buffer for status updates. Sends are blocking to ensure no updates are dropped.
		statusUpdates: make(chan WorkerStatus, cfg.NumWorkers*2),
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
	p.updateStatusWithJob(workerID, WorkerStateWorking, job.EntityDisplayName(), job, 0, job.FromDate)
	defer p.updateStatusWithJob(workerID, WorkerStateIdle, "", nil, 0, time.Time{})

	result := p.processJob(workerID, job)

	// Send result
	select {
	case p.results <- result:
	case <-p.ctx.Done():
		return
	}

	// If this was a chunked job, track completion and merge when all chunks are done.
	// This is called for both success and failure to ensure cleanup happens.
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
			entityType:  job.EntityType,
			chunkFiles:  make([]string, job.ChunkInfo.TotalChunks),
			totalChunks: job.ChunkInfo.TotalChunks,
		}
		p.chunkTrackers[key] = tracker
	}

	// Record this chunk's result
	if result.Error == nil {
		chunkIdx := job.ChunkInfo.ChunkIndex
		// Bounds check to prevent panic if ChunkIndex is invalid
		if chunkIdx < 0 || chunkIdx >= len(tracker.chunkFiles) {
			p.chunkTrackersMu.Unlock()
			logging.Error("Invalid chunk index %d for entity %s (expected 0-%d)",
				chunkIdx, key, len(tracker.chunkFiles)-1)
			return nil
		}
		if result.Device != nil {
			tracker.hostname = result.Device.ComputerDNSName
			tracker.machineID = result.Device.MachineID
			tracker.chunkFiles[chunkIdx] = result.OutputFile
		} else if result.Identity != nil {
			tracker.accountName = result.Identity.AccountName
			tracker.accountDomain = result.Identity.AccountDomain
			tracker.chunkFiles[chunkIdx] = result.OutputFile
		}
	}
	tracker.completed++

	// Check if this is the last chunk
	isLastChunk := tracker.completed == tracker.totalChunks
	if !isLastChunk {
		p.chunkTrackersMu.Unlock()
		return nil
	}

	// This is the last chunk - copy data and release lock before merging
	entityType := tracker.entityType
	hostname := tracker.hostname
	machineID := tracker.machineID
	accountName := tracker.accountName
	accountDomain := tracker.accountDomain
	chunkFiles := make([]string, len(tracker.chunkFiles))
	copy(chunkFiles, tracker.chunkFiles)
	entityKey := job.EntityKey()

	// Clean up tracker
	delete(p.chunkTrackers, key)
	p.chunkTrackersMu.Unlock()

	// Clean up device group (no longer needed after last chunk)
	p.cleanupDeviceGroup(entityKey)

	// Build display name for results (may be empty if all chunks were skipped/failed)
	displayName := hostname
	if entityType == EntityTypeIdentity {
		displayName = accountName
		if accountDomain != "" {
			displayName = accountName + "@" + accountDomain
		}
	}

	// Helper to create a skipped merge result - used when no merge is needed
	// but we must still send a result so the UI doesn't hang waiting for it
	createSkippedMergeResult := func(reason string) *JobResult {
		return &JobResult{
			Job: &Job{
				ID:         -1,
				Type:       JobTypeMerge,
				EntityType: entityType,
				MergeInfo: &MergeInfo{
					Hostname: displayName,
				},
			},
			Skipped:       true,
			SkippedReason: reason,
		}
	}

	// Don't merge if we don't have entity info (all chunks failed before resolution)
	if entityType == EntityTypeDevice && hostname == "" {
		return createSkippedMergeResult("all chunks failed before device resolution")
	}
	if entityType == EntityTypeIdentity && accountName == "" {
		return createSkippedMergeResult("all chunks failed before identity resolution")
	}

	// Collect valid chunk files
	var validFiles []string
	for _, f := range chunkFiles {
		if f != "" {
			validFiles = append(validFiles, f)
		}
	}

	// If no valid files (all chunks were skipped, e.g., duplicate entity), return skipped result
	if len(validFiles) == 0 {
		return createSkippedMergeResult("all chunks skipped (duplicate entity)")
	}

	// Check for partial completion - abort if some chunks failed
	if len(validFiles) != len(chunkFiles) {
		failedCount := len(chunkFiles) - len(validFiles)
		logging.Error("Partial merge aborted for %s: %d/%d chunks failed",
			displayName, failedCount, len(chunkFiles))
		return &JobResult{
			Job: &Job{
				ID:         -1,
				Type:       JobTypeMerge,
				EntityType: entityType,
				MergeInfo: &MergeInfo{
					Hostname: displayName,
				},
			},
			Error: fmt.Errorf("merge aborted: %d/%d chunks failed", failedCount, len(chunkFiles)),
		}
	}

	// Perform merge based on entity type
	if entityType == EntityTypeIdentity {
		return p.performIdentityMerge(workerID, accountName, accountDomain, validFiles)
	}
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

// performIdentityMerge merges identity chunk files into the final output file.
// Uses simple concatenation - deduplication is handled during download.
func (p *Pool) performIdentityMerge(workerID int, accountName, accountDomain string, chunkFiles []string) *JobResult {
	start := time.Now()

	displayName := accountName
	if accountDomain != "" {
		displayName = accountName + "@" + accountDomain
	}

	outputPath := p.fileManager.GetIdentityFinalPath(accountName, accountDomain)
	totalBytes, _ := output.CalculateTotalBytes(chunkFiles)

	logging.Info("Starting merge for %s: %d chunk files -> %s", displayName, len(chunkFiles), outputPath)

	// Update status to merging
	p.updateMergeStatus(workerID, displayName, 0, totalBytes)

	// Progress callback
	progressCallback := func(bytesCopied, total int64) {
		p.updateMergeStatus(workerID, displayName, bytesCopied, total)
	}

	// Simple merge - no dedup needed (handled during download)
	bytesWritten, err := output.MergeChunkFilesWithProgress(
		chunkFiles,
		outputPath,
		true, // deleteChunks
		progressCallback,
	)

	// Create merge job for the result
	mergeJob := &Job{
		ID:         -1, // Merge jobs don't have IDs in this model
		Type:       JobTypeMerge,
		EntityType: EntityTypeIdentity,
		MergeInfo: &MergeInfo{
			ChunkFiles: chunkFiles,
			OutputPath: outputPath,
			TotalBytes: totalBytes,
			Hostname:   displayName,
		},
	}

	result := JobResult{
		Job:        mergeJob,
		OutputFile: outputPath,
		Duration:   time.Since(start),
	}

	if err != nil {
		result.Error = fmt.Errorf("merge failed: %w", err)
		logging.Error("Merge failed for %s: %v", displayName, err)
	} else {
		logging.Info("Merged %s: %d bytes written to %s", displayName, bytesWritten, outputPath)
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

// Done returns a channel that is closed when the pool's context is cancelled.
// Use this to detect shutdown (e.g., fatal error or interrupt).
func (p *Pool) Done() <-chan struct{} {
	return p.ctx.Done()
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
func (p *Pool) getOrCreateDeviceGroup(entityKey string) *deviceGroup {
	p.deviceGroupsMu.Lock()
	defer p.deviceGroupsMu.Unlock()

	if group, exists := p.deviceGroups[entityKey]; exists {
		return group
	}

	// Create new device group with a cancellable context derived from the pool context
	ctx, cancel := context.WithCancel(p.ctx)
	group := &deviceGroup{
		ctx:    ctx,
		cancel: cancel,
	}
	p.deviceGroups[entityKey] = group
	return group
}

// markDeviceFailed marks a device as failed and cancels all pending jobs for it.
// Returns true if this call marked it as failed (first failure), false if already failed.
func (p *Pool) markDeviceFailed(entityKey string, reason string) bool {
	p.deviceGroupsMu.Lock()
	defer p.deviceGroupsMu.Unlock()

	group, exists := p.deviceGroups[entityKey]
	if !exists {
		return false
	}

	if group.failed {
		return false // Already marked as failed
	}

	group.failed = true
	group.failReason = reason
	group.cancel() // Cancel the context to abort in-flight requests

	logging.Error("Device %s: all processing cancelled due to error: %s", entityKey, reason)
	return true
}

// isDeviceFailed checks if the device has been marked as failed.
func (p *Pool) isDeviceFailed(entityKey string) (bool, string) {
	p.deviceGroupsMu.Lock()
	defer p.deviceGroupsMu.Unlock()

	if group, exists := p.deviceGroups[entityKey]; exists && group.failed {
		return true, group.failReason
	}
	return false, ""
}

// cleanupDeviceGroup removes a device group entry and associated cache entries
// after all processing is complete. This prevents unbounded memory growth.
func (p *Pool) cleanupDeviceGroup(entityKey string) {
	// Clean up device group
	p.deviceGroupsMu.Lock()
	if group, exists := p.deviceGroups[entityKey]; exists {
		// Cancel context to release resources (no-op if already cancelled)
		group.cancel()
		delete(p.deviceGroups, entityKey)
	}
	p.deviceGroupsMu.Unlock()

	// Clean up chunk tracker (uses entityKey as key since chunker.go prefixes keys)
	p.chunkTrackersMu.Lock()
	delete(p.chunkTrackers, entityKey)
	p.chunkTrackersMu.Unlock()

	// Clean up entity cache and primary key tracking
	p.entityCacheMu.Lock()
	if entity, exists := p.entityCache[entityKey]; exists {
		// Get primary key before deleting from cache
		primaryKey := entity.PrimaryKey()
		delete(p.entityCache, entityKey)

		// Clean up seenPrimaryKeys if this entityKey registered it
		if primaryKey != "" {
			p.seenPrimaryKeysMu.Lock()
			if registeredBy, ok := p.seenPrimaryKeys[primaryKey]; ok && registeredBy == entityKey {
				delete(p.seenPrimaryKeys, primaryKey)
			}
			p.seenPrimaryKeysMu.Unlock()
		}
	}
	p.entityCacheMu.Unlock()
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

	// Blocking send to ensure UI receives all status updates.
	// Context check prevents blocking forever during shutdown.
	select {
	case p.statusUpdates <- status:
	case <-p.ctx.Done():
	}
}

// checkAndUpdateBackoffState updates worker state based on current backoff status.
// This ensures workers show "backing off" state even during active downloads.
func (p *Pool) checkAndUpdateBackoffState(workerID int, device string, job *Job, progress int, currentDate time.Time) {
	if p.backoff.IsBackingOff() {
		p.updateStatusWithJob(workerID, WorkerStateBackingOff, device, job, progress, currentDate)
	} else {
		p.updateStatusWithJob(workerID, WorkerStateWorking, device, job, progress, currentDate)
	}
}

// handleJobError handles errors from job processing, determining if the error is fatal
// (stops entire pool) or entity-specific (cancels only this entity's jobs).
// Returns true if the result.Fatal flag was set.
// If skipCancelled is true, context.Canceled errors won't mark the entity as failed.
func (p *Pool) handleJobError(result *JobResult, err error, entityKey string, skipCancelled bool) bool {
	var apiErr *api.APIError
	if errors.As(err, &apiErr) && apiErr.Fatal {
		result.Fatal = true
		logging.Error("Fatal error encountered, stopping all jobs")
		p.cancel() // Stop the pool
		return true
	}

	// Don't mark as failed for cancellation (unless skipCancelled is false)
	if skipCancelled && errors.Is(err, context.Canceled) {
		return false
	}

	// Non-fatal error - cancel only this entity's jobs
	p.markDeviceFailed(entityKey, err.Error())
	return false
}

func (p *Pool) processJob(workerID int, job *Job) JobResult {
	// Dispatch based on entity type
	if job.EntityType == EntityTypeIdentity {
		return p.processIdentityJob(workerID, job)
	}
	return p.processDeviceJob(workerID, job)
}

func (p *Pool) processDeviceJob(workerID int, job *Job) JobResult {
	start := time.Now()
	result := JobResult{Job: job}
	entityKey := job.EntityKey()

	// Get or create device group for cancellation support
	group := p.getOrCreateDeviceGroup(entityKey)

	// Check if device has already failed (another chunk encountered an error)
	if failed, reason := p.isDeviceFailed(entityKey); failed {
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
		p.handleJobError(&result, err, entityKey, false)
		return result
	}
	result.Device = device

	// Check for duplicate primary key (different search terms resolving to same device)
	if originalKey, isNew := p.checkAndRegisterPrimaryKey(device.PrimaryKey(), entityKey); !isNew {
		reason := fmt.Sprintf("duplicate: MachineID %s already processed via %s", device.PrimaryKey(), originalKey)
		logging.Info("Skipping device %s: %s", job.DeviceInput.Value, reason)
		result.Skipped = true
		result.SkippedReason = reason
		result.Duration = time.Since(start)
		return result
	}

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
			p.markDeviceFailed(entityKey, err.Error())
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
			p.markDeviceFailed(entityKey, err.Error())
			return result
		}
		logging.Info("Output file: %s", outputPath)
	}
	result.OutputFile = outputPath

	// Ensure writer is closed when done (flushes buffers).
	// If context was cancelled, remove the partial file to prevent corrupt data.
	defer func() {
		if err := writer.Close(); err != nil {
			logging.Error("Failed to close writer for %s: %v", device.ComputerDNSName, err)
		}
		// Clean up partial file on cancellation
		if ctx.Err() != nil {
			if err := os.Remove(outputPath); err == nil {
				logging.Debug("Cleaned up partial file on cancellation: %s", outputPath)
			}
		}
	}()

	// Step 3: Download timeline with progress callback
	logging.Info("Downloading timeline for %s from %s to %s",
		device.ComputerDNSName, job.FromDate.Format("2006-01-02"), job.ToDate.Format("2006-01-02"))
	progressCallback := func(eventCount int, currentDate time.Time) {
		p.checkAndUpdateBackoffState(workerID, device.ComputerDNSName, job, eventCount, currentDate)
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
		p.handleJobError(&result, err, entityKey, true) // Skip marking failed on context cancellation
		return result
	}
	logging.Info("Downloaded %d events for %s", eventCount, device.ComputerDNSName)

	result.EventCount = eventCount
	result.Duration = time.Since(start)

	// Clean up device group for non-chunked jobs (chunked jobs clean up in trackChunkAndMaybemerge)
	if job.ChunkInfo == nil {
		p.cleanupDeviceGroup(entityKey)
	}

	return result
}

func (p *Pool) processIdentityJob(workerID int, job *Job) JobResult {
	start := time.Now()
	result := JobResult{Job: job}
	entityKey := job.EntityKey()

	// Get or create device group for cancellation support (reusing device group infrastructure)
	group := p.getOrCreateDeviceGroup(entityKey)

	// Check if entity has already failed (another chunk encountered an error)
	if failed, reason := p.isDeviceFailed(entityKey); failed {
		result.Error = fmt.Errorf("skipped: identity processing cancelled due to earlier error: %s", reason)
		result.Duration = time.Since(start)
		return result
	}

	// Use the entity group's context (cancelled if any chunk for this entity fails)
	ctx := group.ctx

	// Check context
	if ctx.Err() != nil {
		result.Error = ctx.Err()
		result.Duration = time.Since(start)
		return result
	}

	// Wait if backoff is active
	if p.backoff.IsBackingOff() {
		p.updateStatusWithJob(workerID, WorkerStateBackingOff, job.EntityDisplayName(), job, 0, job.FromDate)
		if err := p.backoff.WaitIfNeeded(ctx); err != nil {
			result.Error = err
			result.Duration = time.Since(start)
			return result
		}
		p.updateStatusWithJob(workerID, WorkerStateWorking, job.EntityDisplayName(), job, 0, job.FromDate)
	}

	// Step 1: Resolve identity (use cache to avoid redundant API calls for chunked jobs)
	identity, err := p.getOrResolveIdentity(ctx, job.IdentityInput)
	if err != nil {
		result.Error = fmt.Errorf("identity resolution failed: %w", err)
		logging.Error("Identity resolution failed for %s: %v", job.IdentityInput.Value, err)
		result.Duration = time.Since(start)
		p.handleJobError(&result, err, entityKey, false)
		return result
	}
	result.Identity = identity

	// Check for duplicate primary key (different search terms resolving to same identity)
	if originalKey, isNew := p.checkAndRegisterPrimaryKey(identity.PrimaryKey(), entityKey); !isNew {
		reason := fmt.Sprintf("duplicate: RadiusUserID %s already processed via %s", identity.PrimaryKey(), originalKey)
		logging.Info("Skipping identity %s: %s", job.IdentityInput.Value, reason)
		result.Skipped = true
		result.SkippedReason = reason
		result.Duration = time.Since(start)
		return result
	}

	// Build display name for UI
	displayName := identity.EntityDisplayName()

	// Update status to show resolved name
	p.updateStatusWithJob(workerID, WorkerStateWorking, displayName, job, 0, job.FromDate)

	// Step 2: Get writer for this identity (no date filtering - API guarantees events are in range)
	var writer *output.JSONLWriter
	var outputPath string

	if job.ChunkInfo != nil {
		// Chunked job: use chunk file
		writer, outputPath, err = p.fileManager.GetIdentityChunkWriter(
			identity.AccountName,
			identity.AccountDomain,
			job.ChunkInfo.ChunkIndex,
		)
		if err != nil {
			result.Error = fmt.Errorf("failed to create chunk file: %w", err)
			logging.Error("Failed to create chunk file for %s: %v", displayName, err)
			result.Duration = time.Since(start)
			p.markDeviceFailed(entityKey, err.Error())
			return result
		}
		logging.Info("Chunk file: %s (chunk %d/%d)", outputPath, job.ChunkInfo.ChunkIndex+1, job.ChunkInfo.TotalChunks)
	} else {
		// Regular job: use normal file
		writer, outputPath, err = p.fileManager.GetIdentityWriter(identity.AccountName, identity.AccountDomain)
		if err != nil {
			result.Error = fmt.Errorf("failed to create output file: %w", err)
			logging.Error("Failed to create output file for %s: %v", displayName, err)
			result.Duration = time.Since(start)
			p.markDeviceFailed(entityKey, err.Error())
			return result
		}
		logging.Info("Output file: %s", outputPath)
	}
	result.OutputFile = outputPath

	// Ensure writer is closed when done (flushes buffers).
	// If context was cancelled, remove the partial file to prevent corrupt data.
	defer func() {
		if err := writer.Close(); err != nil {
			logging.Error("Failed to close writer for %s: %v", displayName, err)
		}
		// Clean up partial file on cancellation
		if ctx.Err() != nil {
			if err := os.Remove(outputPath); err == nil {
				logging.Debug("Cleaned up partial file on cancellation: %s", outputPath)
			}
		}
	}()

	// Step 3: Download identity timeline with progress callback
	logging.Info("Downloading identity timeline for %s from %s to %s",
		displayName, job.FromDate.Format("2006-01-02"), job.ToDate.Format("2006-01-02"))
	progressCallback := func(eventCount int, currentDate time.Time) {
		p.checkAndUpdateBackoffState(workerID, displayName, job, eventCount, currentDate)
	}

	eventCount, err := p.client.DownloadIdentityTimeline(
		ctx,
		identity,
		job.FromDate,
		job.ToDate,
		writer,
		progressCallback,
	)
	if err != nil {
		result.Error = fmt.Errorf("identity timeline download failed: %w", err)
		logging.Error("Identity timeline download failed for %s: %v", displayName, err)
		result.Duration = time.Since(start)
		p.handleJobError(&result, err, entityKey, true) // Skip marking failed on context cancellation
		return result
	}
	logging.Info("Downloaded %d events for %s", eventCount, displayName)

	result.EventCount = eventCount
	result.Duration = time.Since(start)

	// Clean up device group for non-chunked jobs (chunked jobs clean up in trackChunkAndMaybemerge)
	if job.ChunkInfo == nil {
		p.cleanupDeviceGroup(entityKey)
	}

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

	// Blocking send to ensure UI receives all status updates.
	// Context check prevents blocking forever during shutdown.
	select {
	case p.statusUpdates <- status:
	case <-p.ctx.Done():
	}
}

// getOrResolveDevice returns a cached device or resolves it via API.
// Uses singleflight to deduplicate concurrent requests for the same device.
func (p *Pool) getOrResolveDevice(ctx context.Context, input api.DeviceInput) (*api.Device, error) {
	key := "device:" + input.Value

	// Check cache first (read lock)
	p.entityCacheMu.RLock()
	if cached, ok := p.entityCache[key]; ok {
		p.entityCacheMu.RUnlock()
		device := cached.(*api.Device)
		logging.Info("Using cached device: %s (MachineID: %s)", device.ComputerDNSName, device.MachineID)
		return device, nil
	}
	p.entityCacheMu.RUnlock()

	// Use singleflight to deduplicate concurrent resolution requests
	result, err, _ := p.entityResolveGr.Do(key, func() (any, error) {
		// Double-check cache inside singleflight (another request may have just completed)
		p.entityCacheMu.RLock()
		if cached, ok := p.entityCache[key]; ok {
			p.entityCacheMu.RUnlock()
			return cached, nil
		}
		p.entityCacheMu.RUnlock()

		// Resolve device
		logging.Info("Resolving device: %s", input.Value)
		device, err := p.client.ResolveDevice(ctx, input)
		if err != nil {
			return nil, err
		}
		logging.Info("Resolved device %s -> MachineID: %s, SenseVersion: %s",
			device.ComputerDNSName, device.MachineID, device.SenseClientVersion)

		// Store in cache
		p.entityCacheMu.Lock()
		p.entityCache[key] = device
		p.entityCacheMu.Unlock()

		return device, nil
	})

	if err != nil {
		return nil, err
	}
	return result.(*api.Device), nil
}

// getOrResolveIdentity returns a cached identity or resolves it via API.
// Uses singleflight to deduplicate concurrent requests for the same identity.
func (p *Pool) getOrResolveIdentity(ctx context.Context, input api.IdentityInput) (*api.Identity, error) {
	key := "identity:" + input.Value

	// Check cache first (read lock)
	p.entityCacheMu.RLock()
	if cached, ok := p.entityCache[key]; ok {
		p.entityCacheMu.RUnlock()
		identity := cached.(*api.Identity)
		logging.Info("Using cached identity: %s@%s", identity.AccountName, identity.AccountDomain)
		return identity, nil
	}
	p.entityCacheMu.RUnlock()

	// Use singleflight to deduplicate concurrent resolution requests
	result, err, _ := p.entityResolveGr.Do(key, func() (any, error) {
		// Double-check cache inside singleflight (another request may have just completed)
		p.entityCacheMu.RLock()
		if cached, ok := p.entityCache[key]; ok {
			p.entityCacheMu.RUnlock()
			return cached, nil
		}
		p.entityCacheMu.RUnlock()

		// Resolve identity
		logging.Info("Resolving identity: %s", input.Value)
		identity, err := p.client.ResolveIdentityBySearch(ctx, input.Value)
		if err != nil {
			return nil, err
		}
		logging.Info("Resolved identity %s -> %s@%s",
			input.Value, identity.AccountName, identity.AccountDomain)

		// Store in cache
		p.entityCacheMu.Lock()
		p.entityCache[key] = identity
		p.entityCacheMu.Unlock()

		return identity, nil
	})

	if err != nil {
		return nil, err
	}
	return result.(*api.Identity), nil
}

// checkAndRegisterPrimaryKey checks if an entity's primary key has already been seen
// by a DIFFERENT entity key (different search term resolving to same entity).
// If not seen or seen by the same entityKey, it registers and returns ("", true).
// If already seen by a different entityKey, it returns (originalEntityKey, false).
func (p *Pool) checkAndRegisterPrimaryKey(primaryKey, entityKey string) (string, bool) {
	if primaryKey == "" {
		// No primary key available, allow processing
		return "", true
	}

	p.seenPrimaryKeysMu.Lock()
	defer p.seenPrimaryKeysMu.Unlock()

	if originalKey, exists := p.seenPrimaryKeys[primaryKey]; exists {
		// If the same entityKey registered this primary key, allow processing
		// (this handles chunked jobs where multiple chunks share the same entityKey)
		if originalKey == entityKey {
			return "", true
		}
		// Different entityKey resolved to the same primary key - this is a duplicate
		return originalKey, false
	}

	p.seenPrimaryKeys[primaryKey] = entityKey
	return "", true
}
