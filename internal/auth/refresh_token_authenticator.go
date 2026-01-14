package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	// tokenRefreshBuffer is how long before expiry to refresh the token
	tokenRefreshBuffer = 5 * time.Minute

	// DefaultClientID is Microsoft Teams
	DefaultClientID = "1fec8e78-bce4-4aaf-ab1b-5451cc387264"

	// SecurityCenterResourceID is the resource ID for Microsoft 365 Security Center
	SecurityCenterResourceID = "80ccca67-54bd-44ab-8625-4b79c4dc7775"

	// defaultScope for Microsoft Security Center (using resource ID format for v2.0 endpoint)
	defaultScope = SecurityCenterResourceID + "/.default offline_access"
)

// RefreshTokenAuthenticator handles OAuth token management with automatic refresh
type RefreshTokenAuthenticator struct {
	mu           sync.RWMutex
	httpClient   *http.Client
	tenantID     string
	clientID     string
	refreshToken string
	accessToken  string
	expiresAt    time.Time
	userAgent    string
	refreshGroup singleflight.Group // Deduplicates concurrent refresh requests
}

// tokenResponse represents the OAuth token endpoint response
type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Scope        string `json:"scope"`
}

// tokenErrorResponse represents Azure AD error responses (MSAL-compatible format)
type tokenErrorResponse struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
	ErrorCodes       []int  `json:"error_codes"`
	Timestamp        string `json:"timestamp"`
	TraceID          string `json:"trace_id"`
	CorrelationID    string `json:"correlation_id"`
}

// RefreshTokenError represents a token acquisition error with Azure AD details
type RefreshTokenError struct {
	ErrorCode        string
	ErrorDescription string
	ErrorCodes       []int
	StatusCode       int
}

func (e *RefreshTokenError) Error() string {
	return fmt.Sprintf("refresh token error (%s): %s", e.ErrorCode, e.ErrorDescription)
}

// IsExpiredToken returns true if the error indicates an expired or revoked token
func (e *RefreshTokenError) IsExpiredToken() bool {
	return e.ErrorCode == "invalid_grant"
}

// IsInteractionRequired returns true if user interaction is required
func (e *RefreshTokenError) IsInteractionRequired() bool {
	return e.ErrorCode == "interaction_required"
}

// NewRefreshTokenAuthenticator creates a new refresh token authenticator.
// If httpClient is nil, a default client with 30s timeout is created.
func NewRefreshTokenAuthenticator(httpClient *http.Client, tenantID, clientID, refreshToken string) *RefreshTokenAuthenticator {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &RefreshTokenAuthenticator{
		httpClient:   httpClient,
		tenantID:     tenantID,
		clientID:     clientID,
		refreshToken: refreshToken,
		userAgent:    DefaultUserAgent,
	}
}

// GetToken returns a valid access token, refreshing if necessary.
// Uses singleflight to deduplicate concurrent refresh requests.
func (a *RefreshTokenAuthenticator) GetToken(ctx context.Context) (string, error) {
	a.mu.RLock()
	if a.accessToken != "" && time.Now().Add(tokenRefreshBuffer).Before(a.expiresAt) {
		token := a.accessToken
		a.mu.RUnlock()
		return token, nil
	}
	a.mu.RUnlock()

	// Use singleflight to deduplicate concurrent refresh requests
	// All goroutines needing a refresh will share the same HTTP call
	result, err, _ := a.refreshGroup.Do("refresh", func() (any, error) {
		return a.refreshAccessToken(ctx)
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

// ForceRefresh forces a token refresh regardless of expiry.
// Implements the Authenticator interface.
func (a *RefreshTokenAuthenticator) ForceRefresh(ctx context.Context) error {
	_, err := a.refreshAccessToken(ctx)
	return err
}

func (a *RefreshTokenAuthenticator) refreshAccessToken(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Double-check after acquiring write lock
	if a.accessToken != "" && time.Now().Add(tokenRefreshBuffer).Before(a.expiresAt) {
		return a.accessToken, nil
	}

	// Build token request
	tokenURL := fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token", a.tenantID)

	data := url.Values{}
	data.Set("grant_type", "refresh_token")
	data.Set("client_id", a.clientID)
	data.Set("refresh_token", a.refreshToken)
	data.Set("scope", defaultScope)

	req, err := http.NewRequestWithContext(ctx, "POST", tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("failed to create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", a.userAgent)

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to execute token request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", parseRefreshTokenError(body, resp.StatusCode)
	}

	var tokenResp tokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("failed to parse token response: %w", err)
	}

	// Update token state
	a.accessToken = tokenResp.AccessToken
	a.expiresAt = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)

	// Update refresh token if a new one was issued (they rotate)
	if tokenResp.RefreshToken != "" {
		a.refreshToken = tokenResp.RefreshToken
	}

	return a.accessToken, nil
}

// GetRefreshToken returns the current refresh token (useful for saving state)
func (a *RefreshTokenAuthenticator) GetRefreshToken() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.refreshToken
}

// Authenticate implements the Authenticator interface.
// It adds the Bearer token to the request's Authorization header.
func (a *RefreshTokenAuthenticator) Authenticate(ctx context.Context, req *http.Request) error {
	token, err := a.GetToken(ctx)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return nil
}

// parseRefreshTokenError parses Azure AD error responses into a RefreshTokenError
func parseRefreshTokenError(body []byte, statusCode int) error {
	var errResp tokenErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		// If we can't parse the error, return a generic error
		return fmt.Errorf("token refresh failed (status %d): %s", statusCode, string(body))
	}

	return &RefreshTokenError{
		ErrorCode:        errResp.Error,
		ErrorDescription: errResp.ErrorDescription,
		ErrorCodes:       errResp.ErrorCodes,
		StatusCode:       statusCode,
	}
}
