package worker

import (
	"time"

	"github.com/matthieugras/timeline-downloader/internal/api"
)

// SplitIntoChunks splits a device time range into multiple chunk jobs for parallel processing.
// If chunkDays <= 0, returns a single job (no chunking).
// Each chunk job has its own FromDate/ToDate and ChunkInfo.
func SplitIntoChunks(device api.DeviceInput, from, to time.Time, chunkDays int, startJobID int) []*Job {
	// If chunking is disabled, return single job
	if chunkDays <= 0 {
		return []*Job{{
			ID:          startJobID,
			DeviceInput: device,
			FromDate:    from,
			ToDate:      to,
			ChunkInfo:   nil,
		}}
	}

	// Calculate chunk duration
	chunkDuration := time.Duration(chunkDays) * 24 * time.Hour

	// Calculate total number of chunks
	totalDuration := to.Sub(from)
	totalChunks := int(totalDuration / chunkDuration)
	if totalDuration%chunkDuration > 0 {
		totalChunks++
	}

	// Ensure at least one chunk
	if totalChunks == 0 {
		totalChunks = 1
	}

	jobs := make([]*Job, 0, totalChunks)
	deviceKey := device.Value

	for i := 0; i < totalChunks; i++ {
		chunkFrom := from.Add(time.Duration(i) * chunkDuration)
		chunkTo := from.Add(time.Duration(i+1) * chunkDuration)

		// Don't exceed the original end date
		if chunkTo.After(to) {
			chunkTo = to
		}

		// Skip empty chunks
		if !chunkFrom.Before(chunkTo) {
			continue
		}

		jobs = append(jobs, &Job{
			ID:          startJobID + i,
			DeviceInput: device,
			FromDate:    chunkFrom,
			ToDate:      chunkTo,
			ChunkInfo: &ChunkInfo{
				ChunkIndex:  i,
				TotalChunks: totalChunks,
				DeviceKey:   deviceKey,
			},
		})
	}

	return jobs
}

// CalculateChunkCount returns the number of chunks that would be created for a time range.
func CalculateChunkCount(from, to time.Time, chunkDays int) int {
	if chunkDays <= 0 {
		return 1
	}

	chunkDuration := time.Duration(chunkDays) * 24 * time.Hour
	totalDuration := to.Sub(from)
	totalChunks := int(totalDuration / chunkDuration)
	if totalDuration%chunkDuration > 0 {
		totalChunks++
	}

	if totalChunks == 0 {
		return 1
	}

	return totalChunks
}
