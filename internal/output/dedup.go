package output

import (
	"encoding/json"
	"time"

	"github.com/zeebo/xxh3"
)

// Hash128 represents a 128-bit hash value
type Hash128 [2]uint64

// hashEvent computes xxh3 128-bit hash of event JSON after removing Id and RowNumber fields.
// These fields are not stable across pagination sessions.
func hashEvent(event []byte) Hash128 {
	// Parse JSON into a map to remove unwanted fields
	var m map[string]any
	if err := json.Unmarshal(event, &m); err != nil {
		// If parsing fails, hash the raw bytes
		h := xxh3.Hash128(event)
		return Hash128{h.Hi, h.Lo}
	}

	// Remove fields that aren't stable across pagination
	delete(m, "Id")
	delete(m, "RowNumber")

	// Re-serialize to canonical JSON (Go's json.Marshal sorts keys)
	canonical, err := json.Marshal(m)
	if err != nil {
		// Fall back to raw bytes
		h := xxh3.Hash128(event)
		return Hash128{h.Hi, h.Lo}
	}

	h := xxh3.Hash128(canonical)
	return Hash128{h.Hi, h.Lo}
}

// HashEvent computes xxh3 128-bit hash of event JSON after removing Id and RowNumber fields.
// Exported version for use by identity timeline download.
func HashEvent(event []byte) Hash128 {
	return hashEvent(event)
}

// ExtractIdentityTimestamp extracts the Timestamp field from identity timeline JSON.
// Returns zero time if parsing fails or Timestamp is missing.
func ExtractIdentityTimestamp(event []byte) (time.Time, error) {
	var wrapper struct {
		Timestamp string `json:"Timestamp"`
	}
	if err := json.Unmarshal(event, &wrapper); err != nil {
		return time.Time{}, err
	}
	if wrapper.Timestamp == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, wrapper.Timestamp)
}
