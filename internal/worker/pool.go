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
	NumWorkers           int
	Client               *api.Client
	Backoff              *backoff.GlobalBackoff
	FileManager          *output.FileManager
	FromDate             time.Time
	ToDate               time.Time
	TenantID             string          // Tenant ID for identity resolution
	Context              context.Context // Parent context for cancellation
	TotalExpectedResults int             // Size the results buffer to hold all results
}

// chunkTracker tracks chunk completion for a single entity (device or identity)
type chunkTracker struct {
	entityType  EntityType
	displayName string   // EntityDisplayName() - works for both Device and Identity
	primaryKey  string   // PrimaryKey() - works for both Device and Identity
	chunkFiles  []string // indexed by chunk index, empty string if failed
	totalChunks int
	completed   int // number of chunks completed (success or failure)
}

// entityGroup tracks the state of all jobs for a single entity.
// If any chunk fails, the entire entity group is cancelled.
type entityGroup struct {
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
	tenantID    string

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
	chunkTrackers   map[string]*chunkTracker // keyed by entityKey

	// Entity group tracking - allows cancelling all chunks for an entity on failure
	entityGroupsMu sync.Mutex
	entityGroups   map[string]*entityGroup // keyed by entityKey

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
		tenantID:        cfg.TenantID,
		results:         make(chan JobResult, cfg.TotalExpectedResults),
		entityCache:     make(map[string]api.ResolvedEntity),
		seenPrimaryKeys: make(map[string]string),
		chunkTrackers:   make(map[string]*chunkTracker),
		entityGroups:    make(map[string]*entityGroup),
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
func (p *Pool) Submit(job Job) {
	p.pond.Submit(func() {
		p.executeJob(job)
	})
}

// executeJob processes a single job and sends results
func (p *Pool) executeJob(job Job) {
	// Acquire a worker ID from the pool (blocks until one is available)
	workerID := <-p.workerIDPool
	defer func() {
		p.workerIDPool <- workerID
	}()

	// Set initial status
	p.updateStatusWithJob(workerID, WorkerStateWorking, job.EntityDisplayName(), job, 0, job.InitialStatusDate())
	defer p.updateStatusWithJob(workerID, WorkerStateIdle, "", nil, 0, time.Time{})

	result := p.processDownloadJob(workerID, job)

	// Send result (channel is sized to hold all results, so this never blocks)
	p.results <- result

	// If this was a chunked job, track completion and merge when all chunks are done.
	// This is called for both success and failure to ensure cleanup happens.
	chunkInfo := job.GetChunkInfo()
	if chunkInfo != nil {
		mergeResult := p.trackChunkAndMaybeMerge(workerID, job, &result)
		if mergeResult != nil {
			p.results <- *mergeResult
		}
	}
}

// trackChunkAndMaybeMerge tracks chunk completion and performs merge if this was the last chunk.
// Returns a merge result if merge was performed, nil otherwise.
func (p *Pool) trackChunkAndMaybeMerge(workerID int, job Job, result *JobResult) *JobResult {
	chunkInfo := job.GetChunkInfo()
	key := chunkInfo.EntityKey

	p.chunkTrackersMu.Lock()

	tracker, exists := p.chunkTrackers[key]
	if !exists {
		tracker = &chunkTracker{
			entityType:  job.EntityType(),
			chunkFiles:  make([]string, chunkInfo.TotalChunks),
			totalChunks: chunkInfo.TotalChunks,
		}
		p.chunkTrackers[key] = tracker
	}

	// Record this chunk's result
	if result.Error == nil {
		chunkIdx := chunkInfo.ChunkIndex
		// Bounds check to prevent panic if ChunkIndex is invalid
		if chunkIdx < 0 || chunkIdx >= len(tracker.chunkFiles) {
			p.chunkTrackersMu.Unlock()
			logging.Error("Invalid chunk index %d for entity %s (expected 0-%d)",
				chunkIdx, key, len(tracker.chunkFiles)-1)
			return nil
		}
		tracker.displayName = result.Entity.EntityDisplayName()
		tracker.primaryKey = result.Entity.PrimaryKey()
		tracker.chunkFiles[chunkIdx] = result.OutputFile
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
	displayName := tracker.displayName
	primaryKey := tracker.primaryKey
	chunkFiles := make([]string, len(tracker.chunkFiles))
	copy(chunkFiles, tracker.chunkFiles)
	entityKey := job.EntityKey()

	// Clean up tracker
	delete(p.chunkTrackers, key)
	p.chunkTrackersMu.Unlock()

	// Clean up device group (no longer needed after last chunk)
	p.cleanupEntityGroup(entityKey)

	// Helper to create a skipped merge result - used when no merge is needed
	// but we must still send a result so the UI doesn't hang waiting for it
	createSkippedMergeResult := func(reason string) *JobResult {
		return &JobResult{
			MergeJob: &MergeJob{
				ID:     -1,
				Type:   JobTypeMerge,
				Entity: entityType,
				MergeInfo: &MergeInfo{
					Hostname: displayName,
				},
			},
			Skipped:       true,
			SkippedReason: reason,
		}
	}

	// Don't merge if we don't have entity info (all chunks failed before resolution)
	if displayName == "" {
		return createSkippedMergeResult("all chunks failed before entity resolution")
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
			MergeJob: &MergeJob{
				ID:     -1,
				Type:   JobTypeMerge,
				Entity: entityType,
				MergeInfo: &MergeInfo{
					Hostname: displayName,
				},
			},
			Error: fmt.Errorf("merge aborted: %d/%d chunks failed", failedCount, len(chunkFiles)),
		}
	}

	// Perform merge
	return p.performMerge(workerID, entityType, displayName, primaryKey, validFiles)
}

// performMerge merges chunk files into the final output file.
// For identity timelines, chunk files are reversed before merging because
// identity events are returned in descending order (newest first).
func (p *Pool) performMerge(workerID int, entityType EntityType, displayName, primaryKey string, chunkFiles []string) *JobResult {
	start := time.Now()

	// For identity timelines, reverse chunk order (events are descending, so latest chunk first)
	filesToMerge := chunkFiles
	if entityType == EntityTypeIdentity {
		filesToMerge = make([]string, len(chunkFiles))
		for i, f := range chunkFiles {
			filesToMerge[len(chunkFiles)-1-i] = f
		}
	}

	isIdentity := entityType == EntityTypeIdentity
	outputPath := p.fileManager.GetFinalPath(displayName, primaryKey, isIdentity)
	totalBytes, _ := output.CalculateTotalBytes(filesToMerge)

	logging.Info("Starting merge for %s: %d chunk files -> %s", displayName, len(filesToMerge), outputPath)

	// Update status to merging
	p.updateMergeStatus(workerID, displayName, 0, totalBytes)

	// Progress callback
	progressCallback := func(bytesCopied, total int64) {
		p.updateMergeStatus(workerID, displayName, bytesCopied, total)
	}

	bytesWritten, err := output.MergeChunkFilesWithProgress(
		filesToMerge,
		outputPath,
		true, // deleteChunks
		p.fileManager.Gzip(),
		progressCallback,
	)

	// Create merge job for the result
	mergeJob := &MergeJob{
		ID:     -1, // Merge jobs don't have IDs in this model
		Type:   JobTypeMerge,
		Entity: entityType,
		MergeInfo: &MergeInfo{
			ChunkFiles: filesToMerge,
			OutputPath: outputPath,
			TotalBytes: totalBytes,
			Hostname:   displayName,
		},
	}

	result := JobResult{
		MergeJob:   mergeJob,
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
func (p *Pool) SubmitAll(jobs []Job) {
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

// getOrCreateEntityGroup returns the device group for the given device key,
// creating one if it doesn't exist.
func (p *Pool) getOrCreateEntityGroup(entityKey string) *entityGroup {
	p.entityGroupsMu.Lock()
	defer p.entityGroupsMu.Unlock()

	if group, exists := p.entityGroups[entityKey]; exists {
		return group
	}

	// Create new device group with a cancellable context derived from the pool context
	ctx, cancel := context.WithCancel(p.ctx)
	group := &entityGroup{
		ctx:    ctx,
		cancel: cancel,
	}
	p.entityGroups[entityKey] = group
	return group
}

// markEntityFailed marks a device as failed and cancels all pending jobs for it.
// Returns true if this call marked it as failed (first failure), false if already failed.
func (p *Pool) markEntityFailed(entityKey string, reason string) bool {
	p.entityGroupsMu.Lock()
	defer p.entityGroupsMu.Unlock()

	group, exists := p.entityGroups[entityKey]
	if !exists {
		return false
	}

	if group.failed {
		return false // Already marked as failed
	}

	group.failed = true
	group.failReason = reason
	group.cancel() // Cancel the context to abort in-flight requests

	logging.Error("Entity %s: all processing cancelled due to error: %s", entityKey, reason)
	return true
}

// isEntityFailed checks if the device has been marked as failed.
func (p *Pool) isEntityFailed(entityKey string) (bool, string) {
	p.entityGroupsMu.Lock()
	defer p.entityGroupsMu.Unlock()

	if group, exists := p.entityGroups[entityKey]; exists && group.failed {
		return true, group.failReason
	}
	return false, ""
}

// cleanupEntityGroup removes a entity group entry and associated cache entries
// after all processing is complete. This prevents unbounded memory growth.
func (p *Pool) cleanupEntityGroup(entityKey string) {
	// Clean up entity group
	p.entityGroupsMu.Lock()
	if group, exists := p.entityGroups[entityKey]; exists {
		// Cancel context to release resources (no-op if already cancelled)
		group.cancel()
		delete(p.entityGroups, entityKey)
	}
	p.entityGroupsMu.Unlock()

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

func (p *Pool) updateStatusWithJob(id int, state WorkerState, entity string, job Job, progress int, currentDate time.Time) {
	status := WorkerStatus{
		ID:            id,
		State:         state,
		CurrentEntity: entity,
		Progress:      progress,
		CurrentDate:   currentDate,
		FromDate:      p.fromDate,
		ToDate:        p.toDate,
	}

	if job != nil {
		status.JobID = job.JobID()
		status.FromDate = job.FromDate()
		status.ToDate = job.ToDate()
		if chunkInfo := job.GetChunkInfo(); chunkInfo != nil {
			status.ChunkLabel = chunkInfo.ChunkLabel()
		}
		// Identity timelines process newest→oldest (reverse progress)
		status.ReverseProgress = job.IsIdentity()
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
func (p *Pool) checkAndUpdateBackoffState(workerID int, entity string, job Job, progress int, currentDate time.Time) {
	if p.backoff.IsBackingOff() {
		p.updateStatusWithJob(workerID, WorkerStateBackingOff, entity, job, progress, currentDate)
	} else {
		p.updateStatusWithJob(workerID, WorkerStateWorking, entity, job, progress, currentDate)
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
	p.markEntityFailed(entityKey, err.Error())
	return false
}

// getOrResolveEntity returns a cached entity or resolves it via the job.
// Uses singleflight to deduplicate concurrent requests for the same entity.
func (p *Pool) getOrResolveEntity(ctx context.Context, job Job) (api.ResolvedEntity, error) {
	cacheKey := job.EntityKey()

	// Check cache first (read lock)
	p.entityCacheMu.RLock()
	if cached, ok := p.entityCache[cacheKey]; ok {
		p.entityCacheMu.RUnlock()
		job.LogCachedEntity(cached)
		return cached, nil
	}
	p.entityCacheMu.RUnlock()

	// Use singleflight to deduplicate concurrent resolution requests
	result, err, _ := p.entityResolveGr.Do(cacheKey, func() (any, error) {
		// Double-check cache inside singleflight
		p.entityCacheMu.RLock()
		if cached, ok := p.entityCache[cacheKey]; ok {
			p.entityCacheMu.RUnlock()
			return cached, nil
		}
		p.entityCacheMu.RUnlock()

		// Resolve entity
		logging.Info("Resolving %s: %s", job.TypeName(), job.InputValue())
		entity, err := job.ResolveEntity(ctx, p.client, p.tenantID)
		if err != nil {
			return nil, err
		}
		job.LogResolvedEntity(entity)

		// Store in cache
		p.entityCacheMu.Lock()
		p.entityCache[cacheKey] = entity
		p.entityCacheMu.Unlock()

		return entity, nil
	})

	if err != nil {
		return nil, err
	}
	return result.(api.ResolvedEntity), nil
}

// processDownloadJob processes a download job for any entity type.
func (p *Pool) processDownloadJob(workerID int, job Job) JobResult {
	start := time.Now()
	result := JobResult{Job: job}
	entityKey := job.EntityKey()

	// Get or create entity group for cancellation support
	group := p.getOrCreateEntityGroup(entityKey)

	// Check if entity has already failed
	if failed, reason := p.isEntityFailed(entityKey); failed {
		result.Error = fmt.Errorf("skipped: %s processing cancelled due to earlier error: %s", job.TypeName(), reason)
		result.Duration = time.Since(start)
		return result
	}

	ctx := group.ctx
	if ctx.Err() != nil {
		result.Error = ctx.Err()
		result.Duration = time.Since(start)
		return result
	}

	// Wait if backoff is active
	if p.backoff.IsBackingOff() {
		p.updateStatusWithJob(workerID, WorkerStateBackingOff, job.InputValue(), job, 0, job.InitialStatusDate())
		if err := p.backoff.WaitIfNeeded(ctx); err != nil {
			result.Error = err
			result.Duration = time.Since(start)
			return result
		}
		p.updateStatusWithJob(workerID, WorkerStateWorking, job.InputValue(), job, 0, job.InitialStatusDate())
	}

	// Resolve entity
	entity, err := p.getOrResolveEntity(ctx, job)
	if err != nil {
		result.Error = fmt.Errorf("%s resolution failed: %w", job.TypeName(), err)
		logging.Error("%s resolution failed for %s: %v", job.TypeNameTitle(), job.InputValue(), err)
		result.Duration = time.Since(start)
		p.handleJobError(&result, err, entityKey, false)
		return result
	}
	result.Entity = entity

	displayName := entity.EntityDisplayName()

	// Check for duplicate primary key
	if originalKey, isNew := p.checkAndRegisterPrimaryKey(entity.PrimaryKey(), entityKey); !isNew {
		reason := fmt.Sprintf("duplicate: %s %s already processed via %s", job.PrimaryKeyLabel(), entity.PrimaryKey(), originalKey)
		logging.Info("Skipping %s %s: %s", job.TypeName(), job.InputValue(), reason)
		result.Skipped = true
		result.SkippedReason = reason
		result.Duration = time.Since(start)
		return result
	}

	p.updateStatusWithJob(workerID, WorkerStateWorking, displayName, job, 0, job.InitialStatusDate())

	// Get writer
	var writer *output.JSONLWriter
	var outputPath string
	fromDate, toDate := job.WriterDates()
	chunkInfo := job.GetChunkInfo()

	if chunkInfo != nil {
		writer, outputPath, err = p.fileManager.GetChunkWriter(
			displayName, entity.PrimaryKey(), chunkInfo.ChunkIndex,
			fromDate, toDate, job.IsIdentity(),
		)
		if err != nil {
			result.Error = fmt.Errorf("failed to create chunk file: %w", err)
			logging.Error("Failed to create chunk file for %s: %v", displayName, err)
			result.Duration = time.Since(start)
			p.markEntityFailed(entityKey, err.Error())
			return result
		}
		logging.Info("Chunk file: %s (chunk %d/%d)", outputPath, chunkInfo.ChunkIndex+1, chunkInfo.TotalChunks)
	} else {
		writer, outputPath, err = p.fileManager.GetWriter(displayName, entity.PrimaryKey(), fromDate, toDate, job.IsIdentity())
		if err != nil {
			result.Error = fmt.Errorf("failed to create output file: %w", err)
			logging.Error("Failed to create output file for %s: %v", displayName, err)
			result.Duration = time.Since(start)
			p.markEntityFailed(entityKey, err.Error())
			return result
		}
		logging.Info("Output file: %s", outputPath)
	}
	result.OutputFile = outputPath

	var ctxCancelledBeforeCleanup bool
	defer func() {
		if err := writer.Close(); err != nil {
			logging.Error("Failed to close writer for %s: %v", displayName, err)
		}
		if ctxCancelledBeforeCleanup {
			if err := os.Remove(outputPath); err == nil {
				logging.Debug("Cleaned up partial file on cancellation: %s", outputPath)
			}
		}
	}()

	// Download timeline
	logging.Info("Downloading %s timeline for %s from %s to %s",
		job.TypeName(), displayName, job.FromDate().Format("2006-01-02"), job.ToDate().Format("2006-01-02"))
	progressCallback := func(eventCount int, currentDate time.Time) {
		p.checkAndUpdateBackoffState(workerID, displayName, job, eventCount, currentDate)
	}

	eventCount, err := job.DownloadTimeline(ctx, p.client, entity, writer, progressCallback)
	if err != nil {
		result.Error = fmt.Errorf("%s timeline download failed: %w", job.TypeName(), err)
		logging.Error("%s timeline download failed for %s: %v", job.TypeNameTitle(), displayName, err)
		result.Duration = time.Since(start)
		ctxCancelledBeforeCleanup = ctx.Err() != nil
		p.handleJobError(&result, err, entityKey, true)
		return result
	}
	logging.Info("Downloaded %d events for %s", eventCount, displayName)

	result.EventCount = eventCount
	result.Duration = time.Since(start)
	ctxCancelledBeforeCleanup = ctx.Err() != nil

	if chunkInfo == nil {
		p.cleanupEntityGroup(entityKey)
	}

	return result
}

func (p *Pool) updateMergeStatus(id int, hostname string, bytesCopied, totalBytes int64) {
	status := WorkerStatus{
		ID:            id,
		State:         WorkerStateMerging,
		CurrentEntity: hostname,
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
