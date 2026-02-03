package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"time"

	"github.com/matthieugras/timeline-downloader/internal/logging"
	"github.com/matthieugras/timeline-downloader/internal/output"
)

const (
	// timelineAPIPrefix is the required prefix for timeline API calls
	timelineAPIPrefix = "/apiproxy/mtp/mdeTimelineExperience"

	// machineSearchURL is the URL for searching machines by hostname
	machineSearchURL = "/apiproxy/mtp/ndr/machines"

	// machineGetURL is the URL for getting machine details
	machineGetURL = "/apiproxy/mtp/getMachine/machines"
)

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

// DeviceTimelineOptions configures the device timeline API request
type DeviceTimelineOptions struct {
	GenerateIdentityEvents bool
	IncludeIdentityEvents  bool
	SupportMdiOnlyEvents   bool
	IncludeSentinelEvents  bool
	PageSize               int
	Search                 string
}

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

// Implement ResolvedEntity for Device
func (*Device) isResolvedEntity() {}

// EntityDisplayName returns the display name for the device
func (d *Device) EntityDisplayName() string {
	return d.ComputerDNSName
}

// PrimaryKey returns the unique identifier for deduplication (MachineId)
func (d *Device) PrimaryKey() string {
	return d.MachineID
}

// MachineSearchResult represents a single machine from the search API
type MachineSearchResult struct {
	MachineID       string    `json:"MachineId"`
	SenseMachineID  string    `json:"SenseMachineId"`
	ComputerDNSName string    `json:"ComputerDnsName"`
	OSPlatform      string    `json:"OsPlatform"`
	LastSeen        time.Time `json:"LastSeen"`
	FirstSeen       time.Time `json:"FirstSeen"`
	HealthStatus    string    `json:"HealthStatus"`
	RiskScore       string    `json:"RiskScore"`
	Domain          string    `json:"Domain"`
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

// ResolveHostname resolves a hostname to a Device with full details
// This is a 2-step process:
// 1. Search for machine by hostname prefix
// 2. Get full machine details including SenseClientVersion
func (c *Client) ResolveHostname(ctx context.Context, hostname string) (*Device, error) {
	// Step 1: Search for machine by hostname prefix
	searchPath := fmt.Sprintf("%s?hideLowFidelityDevices=true&lookingBackIndays=180&machineSearchPrefix=%s&pageIndex=1&pageSize=25&sortByField=riskscore&sortOrder=Descending",
		machineSearchURL, url.QueryEscape(hostname))

	resp, err := c.doRequestWithRetry(ctx, "GET", searchPath, c.maxRetries)
	if err != nil {
		return nil, fmt.Errorf("machine search failed: %w", err)
	}

	var searchResults []MachineSearchResult
	if err := parseJSONResponse(resp, &searchResults); err != nil {
		return nil, fmt.Errorf("failed to parse search response: %w", err)
	}

	// Check for uniqueness - find exact match first
	var exactMatches []MachineSearchResult
	for _, machine := range searchResults {
		// Check if the hostname matches exactly (case-insensitive DNS name comparison)
		if machine.ComputerDNSName == hostname ||
			machine.ComputerDNSName == hostname+"."+machine.Domain {
			exactMatches = append(exactMatches, machine)
		}
	}

	// If we found exact matches, use those; otherwise fall back to all results
	if len(exactMatches) > 0 {
		searchResults = exactMatches
	}

	if len(searchResults) == 0 {
		return nil, fmt.Errorf("no device found matching hostname: %s", hostname)
	}
	if len(searchResults) > 1 {
		// List the matching devices for debugging
		names := make([]string, len(searchResults))
		for i, m := range searchResults {
			names[i] = m.ComputerDNSName
		}
		return nil, fmt.Errorf("hostname not unique: %s matches %d devices: %v", hostname, len(searchResults), names)
	}

	// Step 2: Get full device details using the SenseMachineId
	return c.GetDevice(ctx, searchResults[0].SenseMachineID)
}

// GetDevice retrieves full device details by machine ID
func (c *Client) GetDevice(ctx context.Context, machineID string) (*Device, error) {
	path := fmt.Sprintf("%s?machineId=%s&idType=SenseMachineId&readFromCache=true&lookingBackIndays=180",
		machineGetURL, url.QueryEscape(machineID))

	resp, err := c.doRequestWithRetry(ctx, "GET", path, c.maxRetries)
	if err != nil {
		return nil, fmt.Errorf("get device failed: %w", err)
	}

	var device Device
	if err := parseJSONResponse(resp, &device); err != nil {
		return nil, fmt.Errorf("failed to parse device response: %w", err)
	}

	if device.SenseClientVersion == "" {
		return nil, fmt.Errorf("device %s has no SenseClientVersion", device.ComputerDNSName)
	}

	if device.MachineID == "" {
		return nil, fmt.Errorf("device %s has no MachineID (required for deduplication)", device.ComputerDNSName)
	}

	return &device, nil
}

// ResolveDevice resolves a device input to full device details
func (c *Client) ResolveDevice(ctx context.Context, input DeviceInput) (*Device, error) {
	if input.IsMachineID {
		return c.GetDevice(ctx, input.Value)
	}
	return c.ResolveHostname(ctx, input.Value)
}

// DownloadDeviceTimeline downloads all timeline events for a device within the specified time range
func DownloadDeviceTimeline(
	ctx context.Context,
	client *Client,
	device *Device,
	fromDate, toDate time.Time,
	opts DeviceTimelineOptions,
	writer output.EventWriter,
	progressCallback func(eventCount int, currentDate time.Time),
	jobID int,
) (int, error) {
	eventCount := 0

	// Build initial URL
	currentURL := buildDeviceTimelineURL(device, fromDate, toDate, opts)

	for currentURL != "" {
		select {
		case <-ctx.Done():
			return eventCount, ctx.Err()
		default:
		}

		logging.DebugWithJob(jobID, "timeline request",
			"path", currentURL)
		resp, err := client.doRequestWithRetry(ctx, "GET", currentURL, client.maxRetries)
		if err != nil {
			return eventCount, fmt.Errorf("timeline request failed: %w", err)
		}

		var timeline TimelineResponse
		if err := parseJSONResponse(resp, &timeline); err != nil {
			return eventCount, fmt.Errorf("failed to parse timeline response: %w", err)
		}

		logging.DebugWithJob(jobID, "timeline response",
			"events_returned", len(timeline.Items),
			"has_next", timeline.Next != "")

		// Write events to JSONL
		for _, item := range timeline.Items {
			if err := writer.Write(item); err != nil {
				return eventCount, fmt.Errorf("failed to write event: %w", err)
			}
			eventCount++
		}

		// Handle pagination - follow Next links
		if timeline.Next == "" {
			// Final progress update at toDate
			if progressCallback != nil {
				progressCallback(eventCount, toDate)
			}
			break
		}

		currentURL = timelineAPIPrefix + timeline.Next
		logging.DebugWithJob(jobID, "next pagination path",
			"path", timeline.Next)

		// Parse fromDate from Next link - this is our current progress through the date range
		nextFromDate, err := parseFromDateFromURL(currentURL)
		if err != nil {
			logging.WarnWithJob(jobID, "could not parse fromDate from pagination URL",
				"error", err,
				"fallback", "using original fromDate for progress")
			nextFromDate = fromDate
		}

		// Report progress using the parsed date from pagination
		if progressCallback != nil {
			progressCallback(eventCount, nextFromDate)
		}

		if nextFromDate.After(toDate) {
			logging.DebugWithJob(jobID, "stopping pagination",
				"next_from_date", nextFromDate,
				"to_date", toDate)
			break
		}
	}

	return eventCount, nil
}

// buildDeviceTimelineURL constructs the initial timeline API URL
// Limits the initial request to max 7 days; pagination handles the rest
func buildDeviceTimelineURL(device *Device, fromDate, toDate time.Time, opts DeviceTimelineOptions) string {
	// Cap initial request to 7 days max
	maxToDate := fromDate.AddDate(0, 0, 7)
	if toDate.After(maxToDate) {
		toDate = maxToDate
	}

	// Build query parameters using url.Values for proper encoding and readability
	q := url.Values{}
	q.Set("machineDnsName", device.ComputerDNSName)
	q.Set("SenseClientVersion", device.SenseClientVersion)
	q.Set("generateIdentityEvents", fmt.Sprintf("%t", opts.GenerateIdentityEvents))
	q.Set("includeIdentityEvents", fmt.Sprintf("%t", opts.IncludeIdentityEvents))
	q.Set("supportMdiOnlyEvents", fmt.Sprintf("%t", opts.SupportMdiOnlyEvents))
	q.Set("fromDate", fromDate.UTC().Format("2006-01-02T15:04:05.0000000Z"))
	q.Set("toDate", toDate.UTC().Format("2006-01-02T15:04:05.0000000Z"))
	q.Set("doNotUseCache", "false")
	q.Set("forceUseCache", "false")
	q.Set("pageSize", fmt.Sprintf("%d", opts.PageSize))
	q.Set("includeSentinelEvents", fmt.Sprintf("%t", opts.IncludeSentinelEvents))
	q.Set("IsScrollingForward", "true")
	if opts.Search != "" {
		q.Set("search", opts.Search)
	}

	return fmt.Sprintf("/apiproxy/mtp/mdeTimelineExperience/machines/%s/events/?%s",
		device.MachineID, q.Encode())
}

// parseFromDateFromURL extracts the fromDate query parameter from a pagination URL
func parseFromDateFromURL(link string) (time.Time, error) {
	parsed, err := url.Parse(link)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse URL: %w", err)
	}

	fromDateStr := parsed.Query().Get("fromDate")
	if fromDateStr == "" {
		return time.Time{}, fmt.Errorf("fromDate parameter missing from URL")
	}

	// Try multiple date formats
	formats := []string{
		"2006-01-02T15:04:05.0000000Z",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, fromDateStr); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("failed to parse fromDate: %s", fromDateStr)
}
