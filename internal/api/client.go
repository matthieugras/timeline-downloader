package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/matthieugras/timeline-downloader/internal/auth"
	"github.com/matthieugras/timeline-downloader/internal/backoff"
	"github.com/matthieugras/timeline-downloader/internal/logging"
)

const (
	baseURL = "https://security.microsoft.com"
)

// TimelineOptions configures the timeline API request parameters
type TimelineOptions struct {
	GenerateIdentityEvents bool
	IncludeIdentityEvents  bool
	SupportMdiOnlyEvents   bool
	IncludeSentinelEvents  bool
	PageSize               int
}

// DefaultTimelineOptions returns the default timeline options
func DefaultTimelineOptions() TimelineOptions {
	return TimelineOptions{
		GenerateIdentityEvents: true,
		IncludeIdentityEvents:  true,
		SupportMdiOnlyEvents:   true,
		IncludeSentinelEvents:  true,
		PageSize:               999,
	}
}

// Client is the API client for Microsoft Defender
type Client struct {
	httpClient      *http.Client
	auth            auth.Authenticator
	backoff         *backoff.GlobalBackoff
	timelineOptions TimelineOptions
	maxRetries      int
}

// NewClient creates a new API client.
// If httpClient is nil, a default client with 60s timeout is created.
func NewClient(httpClient *http.Client, authenticator auth.Authenticator, bo *backoff.GlobalBackoff, opts TimelineOptions, maxRetries int) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}
	if maxRetries <= 0 {
		maxRetries = 6 // Default
	}
	return &Client{
		httpClient:      httpClient,
		auth:            authenticator,
		backoff:         bo,
		timelineOptions: opts,
		maxRetries:      maxRetries,
	}
}

// doRequest performs an authenticated HTTP request with backoff handling
func (c *Client) doRequest(ctx context.Context, method, path string) (*http.Response, error) {
	// Wait if global backoff is active
	if err := c.backoff.WaitIfNeeded(ctx); err != nil {
		return nil, err
	}

	url := baseURL + path
	logging.Debug("API Request: %s %s", method, url)

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", auth.DefaultUserAgent)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	// Add authentication (Bearer token OR cookies+XSRF depending on auth method)
	if err := c.auth.Authenticate(ctx, req); err != nil {
		// Wrap in APIError with Fatal=true so workers know to stop
		apiErr := NewAPIError(http.StatusUnauthorized, fmt.Sprintf("authentication failed: %v", err))
		apiErr.Fatal = true
		return nil, apiErr
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logging.Error("Request failed: %s %s - %v", method, url, err)
		return nil, fmt.Errorf("request failed: %w", err)
	}
	logging.Debug("API Response: %s %s -> %d", method, url, resp.StatusCode)

	// Handle 401 - try refreshing auth once
	if resp.StatusCode == http.StatusUnauthorized {
		resp.Body.Close()

		// Force auth refresh
		if err := c.auth.ForceRefresh(ctx); err != nil {
			apiErr := NewAPIError(http.StatusUnauthorized, fmt.Sprintf("auth refresh failed: %v", err))
			apiErr.Fatal = true
			return nil, apiErr
		}

		// Retry request
		req, err = http.NewRequestWithContext(ctx, method, url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create retry request: %w", err)
		}
		req.Header.Set("User-Agent", auth.DefaultUserAgent)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		if err := c.auth.Authenticate(ctx, req); err != nil {
			apiErr := NewAPIError(http.StatusUnauthorized, fmt.Sprintf("retry authentication failed: %v", err))
			apiErr.Fatal = true
			return nil, apiErr
		}

		resp, err = c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("retry request failed: %w", err)
		}

		// If still 401 after refresh, return fatal error to abort collection
		if resp.StatusCode == http.StatusUnauthorized {
			resp.Body.Close()
			apiErr := NewAPIError(resp.StatusCode, "authentication failed after refresh - aborting collection")
			apiErr.Retryable = false
			apiErr.Fatal = true
			return nil, apiErr
		}
	}

	// Check for rate limiting / server errors (429, 5xx)
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		c.backoff.ReportError() // Trigger global backoff
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		err := NewAPIError(resp.StatusCode, fmt.Sprintf("API error (status %d): %s", resp.StatusCode, string(body)))
		err.Retryable = true
		return nil, err
	}

	// Check for 403 - could be a "stealth" rate limit or a real access denied
	if resp.StatusCode == http.StatusForbidden {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		bodyStr := string(body)

		// Microsoft sometimes returns 403 with this message for rate limiting
		if strings.Contains(bodyStr, "User is not exposed to machine") {
			c.backoff.ReportError() // Trigger global backoff
			err := NewAPIError(resp.StatusCode, fmt.Sprintf("API error (status %d): %s", resp.StatusCode, bodyStr))
			err.Retryable = true
			return nil, err
		}

		// Real 403 - do NOT trigger backoff, let it fail fast
		return nil, NewAPIError(resp.StatusCode, fmt.Sprintf("API error (status %d): %s", resp.StatusCode, bodyStr))
	}

	// Check for other errors
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, NewAPIError(resp.StatusCode, fmt.Sprintf("API error (status %d): %s", resp.StatusCode, string(body)))
	}

	c.backoff.ReportSuccess()
	return resp, nil
}

// doRequestWithRetry performs a request with retries for transient errors
func (c *Client) doRequestWithRetry(ctx context.Context, method, path string, maxRetries int) (*http.Response, error) {
	var lastErr error

	for attempt := range maxRetries {
		if attempt > 0 {
			// Check if last error was a rate-limit (Retryable = true means global backoff was triggered)
			var apiErr *APIError
			wasRateLimited := errors.As(lastErr, &apiErr) && apiErr.Retryable

			if !wasRateLimited {
				// Not rate-limited: use local exponential backoff (2s, 4s, 8s, 16s, 32s...)
				waitTime := time.Duration(1<<attempt) * time.Second
				logging.Debug("Retry attempt %d/%d, waiting %v", attempt+1, maxRetries, waitTime)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(waitTime):
				}
			} else {
				// Rate-limited: global backoff handles the wait via WaitIfNeeded in doRequest
				logging.Debug("Retry attempt %d/%d (rate-limited, using global backoff)", attempt+1, maxRetries)
			}
		}

		resp, err := c.doRequest(ctx, method, path)
		if err == nil {
			return resp, nil
		}

		lastErr = err

		// Check if error is retryable
		if apiErr, ok := err.(*APIError); ok && apiErr.Retryable {
			continue
		}

		// Non-retryable error
		return nil, err
	}

	return nil, fmt.Errorf("request failed after %d attempts: %w", maxRetries, lastErr)
}

// parseJSONResponse reads and parses a JSON response
func parseJSONResponse(resp *http.Response, v any) error {
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if err := json.Unmarshal(body, v); err != nil {
		// Log the full response body for debugging
		logging.Error("Failed to parse JSON response from %s (status %d)", resp.Request.URL.String(), resp.StatusCode)
		logging.Error("Response body (first 2000 chars): %s", truncateString(string(body), 2000))
		return fmt.Errorf("failed to parse response: %w", err)
	}

	return nil
}

// truncateString truncates a string to maxLen characters
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "...[truncated]"
}
