package worker

import (
	"time"

	"github.com/matthieugras/timeline-downloader/internal/api"
)

// TimeChunk represents a single time range chunk
type TimeChunk struct {
	Index int
	Total int
	From  time.Time
	To    time.Time
}

// splitTimeRange splits a time range into chunks of the specified duration.
// If chunkDays <= 0, returns a single chunk covering the entire range.
func splitTimeRange(from, to time.Time, chunkDays int) []TimeChunk {
	if chunkDays <= 0 {
		return []TimeChunk{{Index: 0, Total: 1, From: from, To: to}}
	}

	chunkDuration := time.Duration(chunkDays) * 24 * time.Hour
	totalDuration := to.Sub(from)
	totalChunks := int(totalDuration / chunkDuration)
	if totalDuration%chunkDuration > 0 {
		totalChunks++
	}
	if totalChunks == 0 {
		totalChunks = 1
	}

	chunks := make([]TimeChunk, 0, totalChunks)
	for i := range totalChunks {
		chunkFrom := from.Add(time.Duration(i) * chunkDuration)
		chunkTo := from.Add(time.Duration(i+1) * chunkDuration)
		if chunkTo.After(to) {
			chunkTo = to
		}
		if !chunkFrom.Before(chunkTo) {
			continue
		}
		chunks = append(chunks, TimeChunk{
			Index: i,
			Total: totalChunks,
			From:  chunkFrom,
			To:    chunkTo,
		})
	}
	return chunks
}

// SplitIntoChunks splits a device time range into multiple chunk jobs for parallel processing.
// If chunkDays <= 0, returns a single job (no chunking).
func SplitIntoChunks(device api.DeviceInput, from, to time.Time, chunkDays int, startJobID int) []*Job {
	chunks := splitTimeRange(from, to, chunkDays)
	deviceKey := "device:" + device.Value

	jobs := make([]*Job, 0, len(chunks))
	for _, chunk := range chunks {
		var chunkInfo *ChunkInfo
		if chunkDays > 0 {
			chunkInfo = &ChunkInfo{
				ChunkIndex:  chunk.Index,
				TotalChunks: chunk.Total,
				DeviceKey:   deviceKey,
			}
		}
		jobs = append(jobs, &Job{
			ID:          startJobID + chunk.Index,
			DeviceInput: device,
			FromDate:    chunk.From,
			ToDate:      chunk.To,
			ChunkInfo:   chunkInfo,
		})
	}
	return jobs
}

// SplitIdentityIntoChunks splits an identity time range into multiple chunk jobs for parallel processing.
// If chunkDays <= 0, returns a single job (no chunking).
func SplitIdentityIntoChunks(identity api.IdentityInput, from, to time.Time, chunkDays int, startJobID int) []*Job {
	chunks := splitTimeRange(from, to, chunkDays)
	identityKey := "identity:" + identity.Value

	jobs := make([]*Job, 0, len(chunks))
	for _, chunk := range chunks {
		var chunkInfo *ChunkInfo
		if chunkDays > 0 {
			chunkInfo = &ChunkInfo{
				ChunkIndex:  chunk.Index,
				TotalChunks: chunk.Total,
				DeviceKey:   identityKey,
			}
		}
		jobs = append(jobs, &Job{
			ID:            startJobID + chunk.Index,
			EntityType:    EntityTypeIdentity,
			IdentityInput: identity,
			FromDate:      chunk.From,
			ToDate:        chunk.To,
			ChunkInfo:     chunkInfo,
		})
	}
	return jobs
}

// CalculateChunkCount returns the number of chunks that would be created for a time range.
func CalculateChunkCount(from, to time.Time, chunkDays int) int {
	return len(splitTimeRange(from, to, chunkDays))
}
