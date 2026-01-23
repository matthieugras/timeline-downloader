package output

import (
	"encoding/json"
	"time"
)

// StripUnstableFields removes Id and RowNumber fields that aren't stable across pagination.
// Returns the cleaned JSON, or the original data if parsing fails.
func StripUnstableFields(event []byte) []byte {
	var m map[string]any
	if err := json.Unmarshal(event, &m); err != nil {
		return event
	}

	delete(m, "Id")
	delete(m, "RowNumber")

	cleaned, err := json.Marshal(m)
	if err != nil {
		return event
	}
	return cleaned
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
