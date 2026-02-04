package api

import (
	"bytes"
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

// Client is the API client for Microsoft Defender
type Client struct {
	httpClient *http.Client
	auth       auth.Authenticator
	backoff    *backoff.GlobalBackoff
	maxRetries int
}

// NewClient creates a new API client.
// If httpClient is nil, a default client with 60s timeout is created.
func NewClient(httpClient *http.Client, authenticator auth.Authenticator, bo *backoff.GlobalBackoff, maxRetries int) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}
	if maxRetries <= 0 {
		maxRetries = 6 // Default
	}
	return &Client{
		httpClient: httpClient,
		auth:       authenticator,
		backoff:    bo,
		maxRetries: maxRetries,
	}
}

// doRequest performs an authenticated HTTP GET request with backoff handling
func (c *Client) doRequest(ctx context.Context, method, path string) (*http.Response, error) {
	return c.doRequestWithBody(ctx, method, path, nil)
}

// doRequestWithBody performs an authenticated HTTP request with optional body and backoff handling.
// This is the common implementation shared by GET and POST requests.
func (c *Client) doRequestWithBody(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	return c.doRequestWithBodyAndHeaders(ctx, method, path, body, nil)
}

// doRequestWithBodyAndHeaders performs an authenticated HTTP request with optional body, headers, and backoff handling.
// This is the common implementation shared by GET and POST requests.
func (c *Client) doRequestWithBodyAndHeaders(ctx context.Context, method, path string, body io.Reader, headers map[string]string) (*http.Response, error) {
	// Wait if global backoff is active
	if err := c.backoff.WaitIfNeeded(ctx); err != nil {
		return nil, err
	}

	url := baseURL + path
	logging.Debug("API request",
		"method", method,
		"url", url)

	// Buffer the body so it can be re-read on retry (e.g., after 401 token refresh)
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = io.ReadAll(body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body: %w", err)
		}
	}

	// Create request factory for initial + potential retry
	createRequest := func() (*http.Request, error) {
		var bodyReader io.Reader
		if bodyBytes != nil {
			bodyReader = bytes.NewReader(bodyBytes)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("User-Agent", auth.DefaultUserAgent)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		// Apply additional headers
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		if err := c.auth.Authenticate(ctx, req); err != nil {
			apiErr := NewAPIError(http.StatusUnauthorized, fmt.Sprintf("authentication failed: %v", err))
			apiErr.Fatal = true
			return nil, apiErr
		}
		return req, nil
	}

	req, err := createRequest()
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logging.Error("request failed",
			"method", method,
			"url", url,
			"error", err)
		return nil, fmt.Errorf("request failed: %w", err)
	}
	logging.Debug("API response",
		"method", method,
		"url", url,
		"status", resp.StatusCode)

	// Handle 401 - try refreshing auth once
	if resp.StatusCode == http.StatusUnauthorized {
		resp.Body.Close()

		if err := c.auth.ForceRefresh(ctx); err != nil {
			apiErr := NewAPIError(http.StatusUnauthorized, fmt.Sprintf("auth refresh failed: %v", err))
			apiErr.Fatal = true
			return nil, apiErr
		}

		req, err = createRequest()
		if err != nil {
			return nil, err
		}

		resp, err = c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("retry request failed: %w", err)
		}

		if resp.StatusCode == http.StatusUnauthorized {
			resp.Body.Close()
			apiErr := NewAPIError(resp.StatusCode, "authentication failed after refresh - aborting collection")
			apiErr.Retryable = false
			apiErr.Fatal = true
			return nil, apiErr
		}
	}

	// Handle response status codes
	return c.handleResponseStatus(resp)
}

// handleResponseStatus checks response status and handles errors, rate limiting, etc.
func (c *Client) handleResponseStatus(resp *http.Response) (*http.Response, error) {
	// Check for rate limiting / server errors (429, 5xx)
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		c.backoff.ReportError()
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

		if strings.Contains(bodyStr, "User is not exposed to machine") {
			c.backoff.ReportError()
			err := NewAPIError(resp.StatusCode, fmt.Sprintf("API error (status %d): %s", resp.StatusCode, bodyStr))
			err.Retryable = true
			return nil, err
		}

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

// requestFunc is a function that performs a single request attempt.
// Used by doWithRetry to abstract over GET vs POST requests.
type requestFunc func() (*http.Response, error)

// doWithRetry performs a request with retries for transient errors.
// This is the unified retry logic used by both GET and POST requests.
//
// Backoff strategy:
// - Global backoff (rate limiting): Handled by WaitIfNeeded at the start of each attempt
// - Local backoff (transient errors): Exponential backoff for non-rate-limited errors
func (c *Client) doWithRetry(ctx context.Context, maxRetries int, reqFunc requestFunc) (*http.Response, error) {
	var lastErr error

	for attempt := range maxRetries {
		if attempt > 0 {
			// Always wait for global backoff first (returns immediately if not active)
			if err := c.backoff.WaitIfNeeded(ctx); err != nil {
				return nil, err
			}

			// For non-rate-limited errors, add local exponential backoff
			// Rate-limited errors already triggered global backoff above
			var apiErr *APIError
			wasRateLimited := errors.As(lastErr, &apiErr) && apiErr.Retryable
			if !wasRateLimited {
				waitTime := time.Duration(1<<attempt) * time.Second
				logging.Debug("retry attempt",
					"attempt", attempt+1,
					"max_retries", maxRetries,
					"wait_time", waitTime)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(waitTime):
				}
			} else {
				logging.Debug("retry attempt after global backoff",
					"attempt", attempt+1,
					"max_retries", maxRetries)
			}
		}

		resp, err := reqFunc()
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

// doRequestWithRetry performs a GET/DELETE/etc request with retries for transient errors
func (c *Client) doRequestWithRetry(ctx context.Context, method, path string, maxRetries int) (*http.Response, error) {
	return c.doWithRetry(ctx, maxRetries, func() (*http.Response, error) {
		return c.doRequest(ctx, method, path)
	})
}

// doPostRequestWithHeaders performs an authenticated HTTP POST request with JSON body and additional headers
func (c *Client) doPostRequestWithHeaders(ctx context.Context, path string, body any, headers map[string]string) (*http.Response, error) {
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}
	return c.doRequestWithBodyAndHeaders(ctx, "POST", path, bytes.NewReader(bodyBytes), headers)
}

// doPostRequestWithRetry performs a POST request with retries for transient errors
func (c *Client) doPostRequestWithRetry(ctx context.Context, path string, body any, maxRetries int) (*http.Response, error) {
	return c.doPostRequestWithRetryAndHeaders(ctx, path, body, maxRetries, nil)
}

// doPostRequestWithRetryAndHeaders performs a POST request with retries and additional headers
func (c *Client) doPostRequestWithRetryAndHeaders(ctx context.Context, path string, body any, maxRetries int, headers map[string]string) (*http.Response, error) {
	return c.doWithRetry(ctx, maxRetries, func() (*http.Response, error) {
		return c.doPostRequestWithHeaders(ctx, path, body, headers)
	})
}

// parseJSONResponse reads and parses a JSON response using streaming decoder.
// This avoids loading the entire response body into memory twice (once as []byte,
// once as the decoded struct), reducing memory usage for large responses.
func parseJSONResponse(resp *http.Response, v any) error {
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	decoder := json.NewDecoder(bytes.NewReader(respBody))
	if err := decoder.Decode(v); err != nil {

		logging.Error("failed to parse JSON response",
			"url", resp.Request.URL.String(),
			"status", resp.StatusCode,
			"response_body", string(respBody),
			"error", err)

		return fmt.Errorf("failed to parse response: %w", err)
	}

	return nil
}
