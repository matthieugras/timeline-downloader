package api

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/matthieugras/timeline-downloader/internal/logging"
	"github.com/matthieugras/timeline-downloader/internal/output"
)

// timelineAPIPrefix is the required prefix for timeline API calls
const timelineAPIPrefix = "/apiproxy/mtp/mdeTimelineExperience"

// DownloadTimeline downloads all timeline events for a device within the specified time range
func (c *Client) DownloadTimeline(
	ctx context.Context,
	device *Device,
	fromDate, toDate time.Time,
	writer output.EventWriter,
	progressCallback func(eventCount int, currentDate time.Time),
) (int, error) {
	eventCount := 0

	// Build initial URL
	currentURL := c.buildTimelineURL(device, fromDate, toDate)

	for currentURL != "" {
		select {
		case <-ctx.Done():
			return eventCount, ctx.Err()
		default:
		}

		logging.Debug("Timeline request path: %s", currentURL)
		resp, err := c.doRequestWithRetry(ctx, "GET", currentURL, c.maxRetries)
		if err != nil {
			return eventCount, fmt.Errorf("timeline request failed: %w", err)
		}

		var timeline TimelineResponse
		if err := parseJSONResponse(resp, &timeline); err != nil {
			return eventCount, fmt.Errorf("failed to parse timeline response: %w", err)
		}

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
		logging.Debug("Next pagination path: %s", timeline.Next)

		// Parse fromDate from Next link - this is our current progress through the date range
		nextFromDate := parseFromDateFromURL(currentURL)

		// Report progress using the parsed date from pagination
		if progressCallback != nil {
			if !nextFromDate.IsZero() {
				progressCallback(eventCount, nextFromDate)
			} else {
				// Warn if we couldn't parse the date - API format may have changed
				logging.Warn("Could not parse fromDate from pagination URL, using original fromDate for progress")
				progressCallback(eventCount, fromDate)
			}
		}

		if !nextFromDate.IsZero() && nextFromDate.After(toDate) {
			logging.Debug("Stopping pagination: nextFromDate %v is after toDate %v", nextFromDate, toDate)
			break
		}
	}

	return eventCount, nil
}

// buildTimelineURL constructs the initial timeline API URL
// Limits the initial request to max 7 days; pagination handles the rest
func (c *Client) buildTimelineURL(device *Device, fromDate, toDate time.Time) string {
	// Cap initial request to 7 days max
	maxToDate := fromDate.AddDate(0, 0, 7)
	if toDate.After(maxToDate) {
		toDate = maxToDate
	}

	opts := c.timelineOptions

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

	return fmt.Sprintf("/apiproxy/mtp/mdeTimelineExperience/machines/%s/events/?%s",
		device.MachineID, q.Encode())
}

// parseFromDateFromURL extracts the fromDate query parameter from a pagination URL
func parseFromDateFromURL(link string) time.Time {
	parsed, err := url.Parse(link)
	if err != nil {
		return time.Time{}
	}

	fromDateStr := parsed.Query().Get("fromDate")
	if fromDateStr == "" {
		return time.Time{}
	}

	// Try multiple date formats
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.0000000Z",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, fromDateStr); err == nil {
			return t
		}
	}

	return time.Time{}
}
