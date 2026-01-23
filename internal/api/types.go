package api

// ResolvedEntity is a sealed interface implemented by Device and Identity.
// Used for type-safe entity caching in the worker pool.
type ResolvedEntity interface {
	isResolvedEntity() // unexported method makes this a sealed interface
	EntityDisplayName() string
	PrimaryKey() string // unique identifier for deduplication (machineId for devices, radiusUserId for identities)
}

// APIError represents an error from the API
type APIError struct {
	StatusCode int
	Message    string
	Retryable  bool
	Fatal      bool // If true, abort the entire collection
}

func (e *APIError) Error() string {
	return e.Message
}

// NewAPIError creates a new API error
func NewAPIError(statusCode int, message string) *APIError {
	retryable := statusCode == 429 || (statusCode >= 500 && statusCode < 600)
	return &APIError{
		StatusCode: statusCode,
		Message:    message,
		Retryable:  retryable,
	}
}
