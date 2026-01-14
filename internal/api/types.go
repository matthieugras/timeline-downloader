package api

import (
	"encoding/json"
	"regexp"
	"time"
)

// Device represents a machine from Microsoft Defender
type Device struct {
	MachineID          string    `json:"MachineId"`
	SenseMachineID     string    `json:"SenseMachineId"`
	ComputerDNSName    string    `json:"ComputerDnsName"`
	Hostname           string    `json:"Hostname"`
	SenseClientVersion string    `json:"SenseClientVersion"`
	OSPlatform         string    `json:"OsPlatform"`
	OSBuild            int       `json:"OsBuild"`
	LastSeen           time.Time `json:"LastSeen"`
	FirstSeen          time.Time `json:"FirstSeen"`
	HealthStatus       string    `json:"HealthStatus"`
	RiskScore          string    `json:"RiskScore"`
	Domain             string    `json:"Domain"`
}

// MachineSearchResult represents a single machine from the search API
type MachineSearchResult struct {
	MachineID          string    `json:"MachineId"`
	SenseMachineID     string    `json:"SenseMachineId"`
	ComputerDNSName    string    `json:"ComputerDnsName"`
	OSPlatform         string    `json:"OsPlatform"`
	LastSeen           time.Time `json:"LastSeen"`
	FirstSeen          time.Time `json:"FirstSeen"`
	HealthStatus       string    `json:"HealthStatus"`
	RiskScore          string    `json:"RiskScore"`
	Domain             string    `json:"Domain"`
}

// TimelineResponse represents the response from the timeline API
type TimelineResponse struct {
	Items []json.RawMessage `json:"Items"`
	Next  string            `json:"Next"`
	Prev  string            `json:"Prev"`
}

// TimelineEvent represents a single timeline event (for parsing specific fields)
type TimelineEvent struct {
	EventTime  time.Time `json:"EventTime"`
	EventType  string    `json:"EventType"`
	ActionType string    `json:"ActionType"`
}

// DeviceInput represents user-provided device identifier
type DeviceInput struct {
	Value       string
	IsMachineID bool // true if Value is a machine ID, false if hostname
}

// machineIDPattern matches 40-character hex strings (SHA1 hash format)
var machineIDPattern = regexp.MustCompile(`^[a-fA-F0-9]{40}$`)

// NewDeviceInput creates a DeviceInput from a string value.
// It automatically detects whether the value is a machine ID (40 hex chars) or hostname.
func NewDeviceInput(value string) DeviceInput {
	return DeviceInput{
		Value:       value,
		IsMachineID: machineIDPattern.MatchString(value),
	}
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
