package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/matthieugras/timeline-downloader/internal/backoff"
	"github.com/matthieugras/timeline-downloader/internal/config"
)

// mockRoundTripper intercepts HTTP requests and returns mock responses
type mockRoundTripper struct {
	handler func(req *http.Request) (*http.Response, error)
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.handler(req)
}

// mockAuthenticator implements auth.Authenticator for testing
type mockAuthenticator struct{}

func (m *mockAuthenticator) Authenticate(ctx context.Context, req *http.Request) error {
	req.Header.Set("Authorization", "Bearer test-token")
	return nil
}

func (m *mockAuthenticator) ForceRefresh(ctx context.Context) error {
	return nil
}

func (m *mockAuthenticator) Close() error {
	return nil
}

// mockTruncatableWriter implements output.TruncatableWriter for testing
type mockTruncatableWriter struct {
	mu            sync.Mutex
	events        []json.RawMessage
	truncateCalls []int // records n values passed to TruncateLastLines
	writeErr      error
	truncateErr   error
}

func (w *mockTruncatableWriter) Write(data json.RawMessage) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writeErr != nil {
		return w.writeErr
	}
	w.events = append(w.events, data)
	return nil
}

func (w *mockTruncatableWriter) Close() error {
	return nil
}

func (w *mockTruncatableWriter) TruncateLastLines(n int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.truncateCalls = append(w.truncateCalls, n)
	if w.truncateErr != nil {
		return w.truncateErr
	}
	if n > len(w.events) {
		w.events = nil
	} else {
		w.events = w.events[:len(w.events)-n]
	}
	return nil
}

func (w *mockTruncatableWriter) Events() []json.RawMessage {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := make([]json.RawMessage, len(w.events))
	copy(result, w.events)
	return result
}

func (w *mockTruncatableWriter) TruncateCalls() []int {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := make([]int, len(w.truncateCalls))
	copy(result, w.truncateCalls)
	return result
}

// createTestClient creates a client with mock HTTP transport
func createTestClient(handler func(req *http.Request) (*http.Response, error)) *Client {
	httpClient := &http.Client{
		Transport: &mockRoundTripper{handler: handler},
		Timeout:   10 * time.Second,
	}
	bo := backoff.New(config.DefaultBackoffConfig())
	return NewClient(httpClient, &mockAuthenticator{}, bo, 1)
}

// makeTimelineResponse creates a mock timeline API response
func makeTimelineResponse(events []map[string]any) *http.Response {
	resp := IdentityTimelineResponse{
		Data:   make([]json.RawMessage, len(events)),
		Count:  len(events),
		Errors: nil,
	}
	for i, e := range events {
		data, _ := json.Marshal(e)
		resp.Data[i] = data
	}
	body, _ := json.Marshal(resp)
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(jsonReader(body)),
		Header:     make(http.Header),
	}
}

// jsonReader wraps bytes in a reader
func jsonReader(data []byte) io.Reader {
	return &bytesReader{data: data}
}

type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// makeEvent creates a test event with the given timestamp
func makeEvent(ts time.Time, id string) map[string]any {
	return map[string]any{
		"Timestamp": ts.Format(time.RFC3339),
		"EventId":   id,
	}
}

func testIdentity() *Identity {
	return &Identity{
		UserIdentifiers: UserIdentifiers{
			RadiusUserID: "test-radius-id",
			TenantID:     "test-tenant-id",
		},
		DisplayName:   "Test User",
		AccountName:   "testuser",
		AccountDomain: "example.com",
	}
}

func TestDownloadIdentityTimeline_SimplePagination(t *testing.T) {
	// Test basic pagination without hitting skip limit
	baseTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	fromDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	toDate := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	requestCount := 0
	client := createTestClient(func(req *http.Request) (*http.Response, error) {
		requestCount++
		// First request: return 3 events (less than page size, indicating end)
		events := []map[string]any{
			makeEvent(baseTime, "event1"),
			makeEvent(baseTime.Add(-time.Hour), "event2"),
			makeEvent(baseTime.Add(-2*time.Hour), "event3"),
		}
		return makeTimelineResponse(events), nil
	})

	writer := &mockTruncatableWriter{}
	count, err := DownloadIdentityTimeline(
		context.Background(),
		client,
		testIdentity(),
		fromDate, toDate,
		100, // page size larger than events returned
		writer,
		nil,
		0, // jobID for test
	)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 events, got %d", count)
	}
	if len(writer.Events()) != 3 {
		t.Errorf("Expected 3 events written, got %d", len(writer.Events()))
	}
	if requestCount != 1 {
		t.Errorf("Expected 1 request, got %d", requestCount)
	}
	if len(writer.TruncateCalls()) != 0 {
		t.Errorf("Expected no truncate calls, got %d", len(writer.TruncateCalls()))
	}
}

func TestDownloadIdentityTimeline_MultiplePages(t *testing.T) {
	// Test pagination across multiple pages
	baseTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	fromDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	toDate := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	requestCount := 0
	client := createTestClient(func(req *http.Request) (*http.Response, error) {
		requestCount++
		var reqBody IdentityTimelineRequest
		body, _ := io.ReadAll(req.Body)
		json.Unmarshal(body, &reqBody)

		switch reqBody.Skip {
		case 0:
			// First page: 5 events
			events := make([]map[string]any, 5)
			for i := 0; i < 5; i++ {
				events[i] = makeEvent(baseTime.Add(-time.Duration(i)*time.Hour), fmt.Sprintf("page1-event%d", i))
			}
			return makeTimelineResponse(events), nil
		case 5:
			// Second page: 3 events (end)
			events := make([]map[string]any, 3)
			for i := 0; i < 3; i++ {
				events[i] = makeEvent(baseTime.Add(-time.Duration(5+i)*time.Hour), fmt.Sprintf("page2-event%d", i))
			}
			return makeTimelineResponse(events), nil
		}
		return makeTimelineResponse(nil), nil
	})

	writer := &mockTruncatableWriter{}
	count, err := DownloadIdentityTimeline(
		context.Background(),
		client,
		testIdentity(),
		fromDate, toDate,
		5, // page size
		writer,
		nil,
		0, // jobID for test
	)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if count != 8 {
		t.Errorf("Expected 8 events, got %d", count)
	}
	if requestCount != 2 {
		t.Errorf("Expected 2 requests, got %d", requestCount)
	}
}

func TestDownloadIdentityTimeline_SkipLimitTruncation(t *testing.T) {
	// Test that skip limit triggers truncation and restart
	// We need skip to exceed 9000 to trigger truncation.
	// With pageSize=1000, after 10 requests: skip would become 10000 > 9000
	boundaryTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	fromDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	toDate := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	requestNum := 0
	restarted := false
	client := createTestClient(func(req *http.Request) (*http.Response, error) {
		requestNum++
		var reqBody IdentityTimelineRequest
		body, _ := io.ReadAll(req.Body)
		json.Unmarshal(body, &reqBody)

		// Check if this is after restart (timeframe changed to boundary+1s)
		if reqBody.Filters.Timeframe.Between[1] == boundaryTime.Add(time.Second).Unix() {
			restarted = true
			// After restart: return final events (less than pageSize to end)
			events := []map[string]any{
				makeEvent(boundaryTime, "restart-event1"), // boundary event (refetched)
				makeEvent(boundaryTime, "restart-event2"), // boundary event (refetched)
				makeEvent(boundaryTime.Add(-time.Hour), "final-event"),
			}
			return makeTimelineResponse(events), nil
		}

		// Before restart: simulate pagination until skip limit
		// Return 1000 events each time, with last 2 at boundary time
		events := make([]map[string]any, 1000)
		for i := range 998 {
			// Use different timestamps, all newer than boundaryTime
			eventTime := boundaryTime.Add(time.Duration(1000-i) * time.Minute)
			events[i] = makeEvent(eventTime, fmt.Sprintf("event-%d-%d", requestNum, i))
		}
		// Last 2 events at boundary time
		events[998] = makeEvent(boundaryTime, "boundary-event1")
		events[999] = makeEvent(boundaryTime, "boundary-event2")
		return makeTimelineResponse(events), nil
	})

	writer := &mockTruncatableWriter{}
	progressCalls := 0
	count, err := DownloadIdentityTimeline(
		context.Background(),
		client,
		testIdentity(),
		fromDate, toDate,
		1000,
		writer,
		func(eventCount int, currentDate time.Time) {
			progressCalls++
		},
		0, // jobID for test
	)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !restarted {
		t.Error("Expected restart after skip limit, but no restart occurred")
	}

	// Verify truncation was called
	truncateCalls := writer.TruncateCalls()
	if len(truncateCalls) == 0 {
		t.Error("Expected at least one truncate call")
	}

	// The truncate should have been called with accumulated boundaryCount
	// Each of the 10 pages has 2 boundary events at boundaryTime, so 20 total
	if len(truncateCalls) > 0 && truncateCalls[0] != 20 {
		t.Errorf("Expected truncate with n=20 (10 pages x 2 boundary events), got n=%d", truncateCalls[0])
	}

	// Events should include refetched boundary events
	if count < 3 {
		t.Errorf("Expected at least 3 events in final count, got %d", count)
	}

	// Progress callback should have been called
	if progressCalls == 0 {
		t.Error("Expected progress callbacks")
	}
}

func TestDownloadIdentityTimeline_BoundaryTracking(t *testing.T) {
	// Test that boundary events are correctly tracked
	// All events at same timestamp should be counted and truncated together
	boundaryTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	fromDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	toDate := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	requestNum := 0
	client := createTestClient(func(req *http.Request) (*http.Response, error) {
		requestNum++

		if requestNum == 1 {
			// First page: 4 events (less than pageSize to end pagination)
			// Last 3 events at boundary time - these should all be tracked
			events := []map[string]any{
				makeEvent(boundaryTime.Add(time.Hour), "newer1"),
				makeEvent(boundaryTime, "boundary1"),
				makeEvent(boundaryTime, "boundary2"),
				makeEvent(boundaryTime, "boundary3"),
			}
			return makeTimelineResponse(events), nil
		}
		return makeTimelineResponse(nil), nil
	})

	writer := &mockTruncatableWriter{}
	count, err := DownloadIdentityTimeline(
		context.Background(),
		client,
		testIdentity(),
		fromDate, toDate,
		10, // pageSize > returned events, so it ends after first page
		writer,
		nil,
		0, // jobID for test
	)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if count != 4 {
		t.Errorf("Expected 4 events, got %d", count)
	}
	if requestNum != 1 {
		t.Errorf("Expected 1 request (no pagination), got %d", requestNum)
	}
	// No truncation should occur since we didn't hit skip limit
	if len(writer.TruncateCalls()) != 0 {
		t.Errorf("Expected no truncate calls, got %d", len(writer.TruncateCalls()))
	}
}

func TestDownloadIdentityTimeline_EmptyResponse(t *testing.T) {
	// Test handling of empty response
	fromDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	toDate := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	client := createTestClient(func(req *http.Request) (*http.Response, error) {
		return makeTimelineResponse(nil), nil
	})

	writer := &mockTruncatableWriter{}
	count, err := DownloadIdentityTimeline(
		context.Background(),
		client,
		testIdentity(),
		fromDate, toDate,
		100,
		writer,
		nil,
		0, // jobID for test
	)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 events, got %d", count)
	}
}

func TestDownloadIdentityTimeline_ContextCancellation(t *testing.T) {
	// Test that context cancellation is handled
	fromDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	toDate := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	client := createTestClient(func(req *http.Request) (*http.Response, error) {
		t.Error("Request should not be made after context cancellation")
		return nil, nil
	})

	writer := &mockTruncatableWriter{}
	_, err := DownloadIdentityTimeline(
		ctx,
		client,
		testIdentity(),
		fromDate, toDate,
		100,
		writer,
		nil,
		0, // jobID for test
	)

	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestDownloadIdentityTimeline_MissingTimestamp(t *testing.T) {
	// Test that events with missing timestamps are skipped
	baseTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	fromDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	toDate := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	client := createTestClient(func(req *http.Request) (*http.Response, error) {
		events := []map[string]any{
			makeEvent(baseTime, "event1"),
			{"EventId": "no-timestamp"}, // Missing timestamp
			makeEvent(baseTime.Add(-time.Hour), "event3"),
		}
		return makeTimelineResponse(events), nil
	})

	writer := &mockTruncatableWriter{}
	count, err := DownloadIdentityTimeline(
		context.Background(),
		client,
		testIdentity(),
		fromDate, toDate,
		100,
		writer,
		nil,
		0, // jobID for test
	)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Should only count events with valid timestamps
	if count != 2 {
		t.Errorf("Expected 2 events (skipping one without timestamp), got %d", count)
	}
}

func TestDownloadIdentityTimeline_WriteError(t *testing.T) {
	// Test handling of write errors
	baseTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	fromDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	toDate := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	client := createTestClient(func(req *http.Request) (*http.Response, error) {
		events := []map[string]any{
			makeEvent(baseTime, "event1"),
		}
		return makeTimelineResponse(events), nil
	})

	writer := &mockTruncatableWriter{
		writeErr: fmt.Errorf("disk full"),
	}
	_, err := DownloadIdentityTimeline(
		context.Background(),
		client,
		testIdentity(),
		fromDate, toDate,
		100,
		writer,
		nil,
		0, // jobID for test
	)

	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestDownloadIdentityTimeline_TruncateError(t *testing.T) {
	// Test handling of truncation errors
	boundaryTime := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	fromDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	toDate := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	requestNum := 0
	client := createTestClient(func(req *http.Request) (*http.Response, error) {
		requestNum++
		var reqBody IdentityTimelineRequest
		body, _ := io.ReadAll(req.Body)
		json.Unmarshal(body, &reqBody)

		// Return exactly 1000 events each request to trigger skip limit
		events := make([]map[string]any, 1000)
		for i := 0; i < 1000; i++ {
			// Put all events at boundary time to ensure truncation happens
			events[i] = makeEvent(boundaryTime, fmt.Sprintf("event-%d", i))
		}
		return makeTimelineResponse(events), nil
	})

	writer := &mockTruncatableWriter{
		truncateErr: fmt.Errorf("truncation failed"),
	}
	_, err := DownloadIdentityTimeline(
		context.Background(),
		client,
		testIdentity(),
		fromDate, toDate,
		1000,
		writer,
		nil,
		0, // jobID for test
	)

	// Should eventually hit skip limit and try to truncate, which will fail
	if err == nil {
		t.Error("Expected error from truncation failure")
	}
}

func TestDownloadIdentityTimeline_PathologicalCase(t *testing.T) {
	// Test the pathological case where we're stuck at the same timestamp
	// This happens when >10000 events have the same timestamp
	stuckTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	fromDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	toDate := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	restartCount := 0
	client := createTestClient(func(req *http.Request) (*http.Response, error) {
		var reqBody IdentityTimelineRequest
		body, _ := io.ReadAll(req.Body)
		json.Unmarshal(body, &reqBody)

		// Track restarts by checking timeframe changes
		if reqBody.Filters.Timeframe.Between[1] == stuckTime.Add(time.Second).Unix() {
			restartCount++
		}

		// If we've detected the pathological case and moved past stuckTime
		if reqBody.Filters.Timeframe.Between[1] == stuckTime.Unix() {
			// Return older events to end the download
			events := []map[string]any{
				makeEvent(stuckTime.Add(-time.Hour), "older-event"),
			}
			return makeTimelineResponse(events), nil
		}

		// Always return events at stuckTime to simulate pathological case
		events := make([]map[string]any, 1000)
		for i := range 1000 {
			events[i] = makeEvent(stuckTime, fmt.Sprintf("stuck-event-%d", i))
		}
		return makeTimelineResponse(events), nil
	})

	writer := &mockTruncatableWriter{}
	count, err := DownloadIdentityTimeline(
		context.Background(),
		client,
		testIdentity(),
		fromDate, toDate,
		1000,
		writer,
		nil,
		0, // jobID for test
	)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should have detected the pathological case and moved on
	// restartCount should be at least 1 (first restart), then detect stuck
	if restartCount < 1 {
		t.Error("Expected at least one restart before detecting pathological case")
	}

	// Should have gotten the final "older-event"
	if count == 0 {
		t.Error("Expected some events to be downloaded")
	}
}

func TestDownloadIdentityTimeline_ProgressCallback(t *testing.T) {
	// Test that progress callback is called correctly
	baseTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	fromDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	toDate := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	client := createTestClient(func(req *http.Request) (*http.Response, error) {
		events := []map[string]any{
			makeEvent(baseTime, "event1"),
			makeEvent(baseTime.Add(-time.Hour), "event2"),
		}
		return makeTimelineResponse(events), nil
	})

	var progressEvents []int
	var progressDates []time.Time
	writer := &mockTruncatableWriter{}
	count, err := DownloadIdentityTimeline(
		context.Background(),
		client,
		testIdentity(),
		fromDate, toDate,
		100,
		writer,
		func(eventCount int, currentDate time.Time) {
			progressEvents = append(progressEvents, eventCount)
			progressDates = append(progressDates, currentDate)
		},
		0, // jobID for test
	)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 events, got %d", count)
	}
	if len(progressEvents) != 1 {
		t.Errorf("Expected 1 progress callback, got %d", len(progressEvents))
	}
	if len(progressEvents) > 0 && progressEvents[0] != 2 {
		t.Errorf("Expected progress with 2 events, got %d", progressEvents[0])
	}
	// Progress date should be batchMinTime (the oldest event)
	if len(progressDates) > 0 && !progressDates[0].Equal(baseTime.Add(-time.Hour)) {
		t.Errorf("Expected progress date to be batchMinTime, got %v", progressDates[0])
	}
}

func TestDownloadIdentityTimeline_PageSizeCap(t *testing.T) {
	// Test that page size is capped at IdentityMaxPageSize (1000)
	fromDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	toDate := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	var capturedCount int
	client := createTestClient(func(req *http.Request) (*http.Response, error) {
		var reqBody IdentityTimelineRequest
		body, _ := io.ReadAll(req.Body)
		json.Unmarshal(body, &reqBody)
		capturedCount = reqBody.Count
		return makeTimelineResponse(nil), nil
	})

	writer := &mockTruncatableWriter{}
	DownloadIdentityTimeline(
		context.Background(),
		client,
		testIdentity(),
		fromDate, toDate,
		5000, // Request larger than max
		writer,
		nil,
		0, // jobID for test
	)

	if capturedCount != IdentityMaxPageSize {
		t.Errorf("Expected page size capped at %d, got %d", IdentityMaxPageSize, capturedCount)
	}
}

func TestDownloadIdentityTimeline_UnstableFieldsStripped(t *testing.T) {
	// Test that Id and RowNumber fields are stripped from events
	baseTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	fromDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	toDate := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

	client := createTestClient(func(req *http.Request) (*http.Response, error) {
		events := []map[string]any{
			{
				"Timestamp": baseTime.Format(time.RFC3339),
				"EventId":   "event1",
				"Id":        "unstable-id", // Should be stripped
				"RowNumber": 12345,         // Should be stripped
				"Data":      "should-remain",
			},
		}
		return makeTimelineResponse(events), nil
	})

	writer := &mockTruncatableWriter{}
	count, err := DownloadIdentityTimeline(
		context.Background(),
		client,
		testIdentity(),
		fromDate, toDate,
		100,
		writer,
		nil,
		0, // jobID for test
	)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1 event, got %d", count)
	}

	// Check the written event doesn't have Id or RowNumber
	events := writer.Events()
	if len(events) != 1 {
		t.Fatalf("Expected 1 written event, got %d", len(events))
	}

	var written map[string]any
	json.Unmarshal(events[0], &written)

	if _, hasId := written["Id"]; hasId {
		t.Error("Expected Id field to be stripped")
	}
	if _, hasRowNumber := written["RowNumber"]; hasRowNumber {
		t.Error("Expected RowNumber field to be stripped")
	}
	if written["Data"] != "should-remain" {
		t.Error("Expected Data field to remain")
	}
}
