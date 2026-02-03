package auth

import (
	"context"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	// xsrfRefreshBuffer is how long before XSRF expiry to trigger refresh
	xsrfRefreshBuffer = 30 * time.Second

	// xsrfTTL is how long the XSRF token is valid (based on XDRInternals behavior)
	xsrfTTL = 5 * time.Minute

	// loginBootstrapURL is used to initialize the session
	loginBootstrapURL = "https://login.microsoftonline.com/error"

	// securityPortalBaseURL is the Microsoft Security Portal
	securityPortalBaseURL = "https://security.microsoft.com/"
)

// ESTSAuthenticator implements cookie-based authentication using the ESTSAUTHPERSISTENT cookie.
// This is the same authentication method used by XDRInternals PowerShell module.
// Thread-safe: uses transient clients for login/refresh flows, stores only extracted state.
type ESTSAuthenticator struct {
	mu sync.RWMutex

	// Configuration (immutable after construction)
	httpClient *http.Client // Stored for timeout/transport settings only, never modified
	estsCookie string
	tenantID   string
	userAgent  string

	// State (protected by mutex)
	xsrfToken     string    // URL-decoded X-XSRF-TOKEN header value
	xsrfExpiresAt time.Time // When to refresh
	sccauthValue  string    // sccauth cookie value
	xsrfCookieVal string    // URL-encoded xsrf-token cookie value
	initialized   bool

	refreshGroup singleflight.Group // Deduplicates concurrent refresh requests
}

// ESTSError represents an ESTS authentication error
type ESTSError struct {
	ErrorCode   string `json:"sErrorCode"`
	Message     string
	NeedsAction string // Human-readable action required
}

func (e *ESTSError) Error() string {
	if e.NeedsAction != "" {
		return fmt.Sprintf("ESTS auth error (%s): %s. Action: %s", e.ErrorCode, e.Message, e.NeedsAction)
	}
	return fmt.Sprintf("ESTS auth error (%s): %s", e.ErrorCode, e.Message)
}

// NewESTSAuthenticator creates a new ESTS cookie authenticator.
// The httpClient is stored for its timeout/transport settings but is never modified.
// Login and refresh flows use transient clients with their own cookie jars.
func NewESTSAuthenticator(httpClient *http.Client, estsCookie, tenantID string) *ESTSAuthenticator {
	return &ESTSAuthenticator{
		httpClient: httpClient,
		estsCookie: estsCookie,
		tenantID:   tenantID,
		userAgent:  DefaultUserAgent,
	}
}

// Authenticate implements the Authenticator interface.
// It adds session cookies and X-XSRF-TOKEN header to the request.
// Uses singleflight to deduplicate concurrent refresh requests.
func (ea *ESTSAuthenticator) Authenticate(ctx context.Context, req *http.Request) error {
	ea.mu.RLock()
	needsRefresh := !ea.initialized || ea.xsrfToken == "" || time.Now().Add(xsrfRefreshBuffer).After(ea.xsrfExpiresAt)
	ea.mu.RUnlock()

	if needsRefresh {
		// Use singleflight to deduplicate concurrent refresh requests
		_, err, _ := ea.refreshGroup.Do("refresh", func() (any, error) {
			return nil, ea.refreshSession(ctx)
		})
		if err != nil {
			return err
		}
	}

	ea.mu.RLock()
	defer ea.mu.RUnlock()

	// Add session cookies to request
	req.AddCookie(&http.Cookie{Name: "sccauth", Value: ea.sccauthValue})
	req.AddCookie(&http.Cookie{Name: "xsrf-token", Value: ea.xsrfCookieVal})

	// Add XSRF header (URL-decoded value)
	req.Header.Set("X-XSRF-TOKEN", ea.xsrfToken)

	return nil
}

// ForceRefresh implements the Authenticator interface.
// It forces a session refresh.
func (ea *ESTSAuthenticator) ForceRefresh(ctx context.Context) error {
	ea.mu.Lock()
	// Invalidate current session
	ea.xsrfExpiresAt = time.Time{}
	ea.mu.Unlock()

	return ea.refreshSession(ctx)
}

// refreshSession refreshes the XSRF token and session cookies.
// Uses a transient client with hydrated cookies for the refresh request.
func (ea *ESTSAuthenticator) refreshSession(ctx context.Context) error {
	ea.mu.Lock()
	defer ea.mu.Unlock()

	// Double-check after acquiring lock
	if ea.initialized && ea.xsrfToken != "" && time.Now().Add(xsrfRefreshBuffer).Before(ea.xsrfExpiresAt) {
		return nil
	}

	// If not initialized, do full session establishment
	if !ea.initialized {
		return ea.establishSessionLocked(ctx)
	}

	// Create transient cookie jar and hydrate with current session cookies
	jar, err := cookiejar.New(nil)
	if err != nil {
		return fmt.Errorf("failed to create cookie jar: %w", err)
	}

	securityURL, _ := url.Parse("https://security.microsoft.com/")
	jar.SetCookies(securityURL, []*http.Cookie{
		{Name: "sccauth", Value: ea.sccauthValue},
		{Name: "xsrf-token", Value: ea.xsrfCookieVal},
	})

	// Create transient client using stored client's timeout/transport settings
	transientClient := ea.createTransientClient(jar)

	// GET security.microsoft.com to refresh cookies
	portalURL := securityPortalBaseURL
	if ea.tenantID != "" {
		portalURL += "?tid=" + ea.tenantID
	}

	req, err := http.NewRequestWithContext(ctx, "GET", portalURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create refresh request: %w", err)
	}
	req.Header.Set("User-Agent", ea.userAgent)

	resp, err := transientClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to refresh session: %w", err)
	}
	defer resp.Body.Close()

	// Extract updated cookies from transient jar
	if err := ea.extractCookiesFromJar(jar); err != nil {
		return err
	}

	return nil
}

// establishSessionLocked performs the full OIDC flow to establish a session.
// Uses a transient client for the login flow, then extracts cookies into struct state.
// Must be called with mutex held.
func (ea *ESTSAuthenticator) establishSessionLocked(ctx context.Context) error {
	// 1. Create transient cookie jar for the login flow
	jar, err := cookiejar.New(nil)
	if err != nil {
		return fmt.Errorf("failed to create cookie jar: %w", err)
	}

	// 2. Create transient client using stored client's timeout/transport settings
	transientClient := ea.createTransientClient(jar)

	// 3. Bootstrap: GET login.microsoftonline.com/error to initialize session cookies
	bootstrapReq, err := http.NewRequestWithContext(ctx, "GET", loginBootstrapURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create bootstrap request: %w", err)
	}
	bootstrapReq.Header.Set("User-Agent", ea.userAgent)

	resp, err := transientClient.Do(bootstrapReq)
	if err != nil {
		return fmt.Errorf("failed to bootstrap session: %w", err)
	}
	resp.Body.Close()

	// 4. Add ESTSAUTHPERSISTENT cookie to login.microsoftonline.com
	loginURL, _ := url.Parse("https://login.microsoftonline.com/")
	jar.SetCookies(loginURL, []*http.Cookie{{
		Name:  "ESTSAUTHPERSISTENT",
		Value: ea.estsCookie,
	}})

	// 5. GET security.microsoft.com (triggers OIDC flow via redirects)
	portalURL := securityPortalBaseURL
	if ea.tenantID != "" {
		portalURL += "?tid=" + ea.tenantID
	}

	authReq, err := http.NewRequestWithContext(ctx, "GET", portalURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create auth request: %w", err)
	}
	authReq.Header.Set("User-Agent", ea.userAgent)

	resp, err = transientClient.Do(authReq)
	if err != nil {
		return fmt.Errorf("failed to initiate auth flow: %w", err)
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("failed to read auth response: %w", err)
	}

	// 5.5. Handle MCAS intermediate flow if detected
	if ea.isMCASResponse(body) {
		body, err = ea.handleMCASFlow(ctx, transientClient, body)
		if err != nil {
			return err
		}
	}

	// 6. Parse HTML form for OIDC tokens
	formData, err := ea.parseOIDCForm(body)
	if err != nil {
		return err
	}

	// 7. POST form data back to security.microsoft.com to exchange for session cookies
	postReq, err := http.NewRequestWithContext(ctx, "POST", portalURL, strings.NewReader(formData.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create POST request: %w", err)
	}
	postReq.Header.Set("User-Agent", ea.userAgent)
	postReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err = transientClient.Do(postReq)
	if err != nil {
		return fmt.Errorf("failed to exchange tokens: %w", err)
	}
	body, err = io.ReadAll(resp.Body)
	resp.Body.Close()

	// 8. Extract session cookies from transient jar into struct state
	if err := ea.extractCookiesFromJar(jar); err != nil {
		return err
	}

	ea.initialized = true
	return nil
}

// parseOIDCForm extracts OIDC form fields from the HTML response.
func (ea *ESTSAuthenticator) parseOIDCForm(body []byte) (url.Values, error) {
	bodyStr := string(body)

	// Check for error response (JSON embedded in HTML)
	if !regexp.MustCompile(`<input[^>]+name=["']code["']`).MatchString(bodyStr) {
		// Try to extract sErrorCode directly from the HTML/JS
		// Azure AD embeds error info in JavaScript like: "sErrorCode":"50058"
		errorCodeRegex := regexp.MustCompile(`"sErrorCode"\s*:\s*"(\d+)"`)
		if match := errorCodeRegex.FindStringSubmatch(bodyStr); len(match) >= 2 {
			errorCode := match[1]
			switch errorCode {
			case "50058":
				return nil, &ESTSError{
					ErrorCode:   "50058",
					Message:     "Session information is not sufficient for single-sign-on",
					NeedsAction: "Use an incognito/private browsing session to obtain a new ESTSAUTHPERSISTENT cookie",
				}
			default:
				return nil, &ESTSError{
					ErrorCode: errorCode,
					Message:   "Authentication flow failed",
				}
			}
		}

		return nil, &ESTSError{
			ErrorCode:   "invalid_response",
			Message:     "OIDC form not found in response - cookie may be invalid or expired",
			NeedsAction: "Obtain a new ESTSAUTHPERSISTENT cookie from your browser",
		}
	}

	// Extract form fields using regex
	formData := url.Values{}
	requiredFields := []string{"code", "id_token", "state", "session_state", "correlation_id"}

	for _, field := range requiredFields {
		// Match: <input type="hidden" name="field" value="..."/>
		pattern := regexp.MustCompile(fmt.Sprintf(`<input[^>]+name=["']%s["'][^>]+value=["']([^"']*)["']`, field))
		match := pattern.FindStringSubmatch(bodyStr)

		// Also try reversed order: value before name
		if match == nil {
			pattern = regexp.MustCompile(fmt.Sprintf(`<input[^>]+value=["']([^"']*)["'][^>]+name=["']%s["']`, field))
			match = pattern.FindStringSubmatch(bodyStr)
		}

		if len(match) < 2 {
			return nil, &ESTSError{
				ErrorCode: "missing_field",
				Message:   fmt.Sprintf("Required field '%s' not found in OIDC response", field),
			}
		}

		// Decode HTML entities (e.g., &amp; -> &) in the extracted value
		formData.Set(field, html.UnescapeString(match[1]))
	}

	return formData, nil
}

// createTransientClient creates a temporary HTTP client for login/refresh flows.
// Uses the stored client's timeout and transport settings with the provided cookie jar.
func (ea *ESTSAuthenticator) createTransientClient(jar http.CookieJar) *http.Client {
	timeout := 60 * time.Second
	var transport http.RoundTripper

	if ea.httpClient != nil {
		timeout = ea.httpClient.Timeout
		transport = ea.httpClient.Transport
	}

	return &http.Client{
		Jar:       jar,
		Timeout:   timeout,
		Transport: transport,
	}
}

// extractCookiesFromJar extracts session cookies from the provided cookie jar.
// Updates the struct's state fields. Must be called with mutex held.
func (ea *ESTSAuthenticator) extractCookiesFromJar(jar http.CookieJar) error {
	securityURL, _ := url.Parse("https://security.microsoft.com/")
	cookies := jar.Cookies(securityURL)

	var foundSccauth, foundXsrf bool

	for _, c := range cookies {
		nameLower := strings.ToLower(c.Name)
		switch nameLower {
		case "sccauth":
			ea.sccauthValue = c.Value
			foundSccauth = true
		case "xsrf-token":
			ea.xsrfCookieVal = c.Value
			// URL decode for header value
			decoded, err := url.QueryUnescape(c.Value)
			if err != nil {
				ea.xsrfToken = c.Value // Use as-is if decode fails
			} else {
				ea.xsrfToken = decoded
			}
			ea.xsrfExpiresAt = time.Now().Add(xsrfTTL)
			foundXsrf = true
		}
	}

	if !foundSccauth {
		return &ESTSError{
			ErrorCode:   "missing_cookie",
			Message:     "sccauth cookie not found after authentication",
			NeedsAction: "Obtain a new ESTSAUTHPERSISTENT cookie from your browser",
		}
	}

	if !foundXsrf {
		return &ESTSError{
			ErrorCode:   "missing_cookie",
			Message:     "xsrf-token cookie not found after authentication",
			NeedsAction: "Obtain a new ESTSAUTHPERSISTENT cookie from your browser",
		}
	}

	return nil
}

// isMCASResponse checks if the HTML response is an MCAS intermediate form.
// MCAS (Microsoft Cloud App Security) adds an extra authentication step where
// the login.microsoftonline.com response redirects through an MCAS endpoint.
func (ea *ESTSAuthenticator) isMCASResponse(body []byte) bool {
	// Check for form action URL containing .access.mcas.ms
	mcasActionRegex := regexp.MustCompile(`<form[^>]+action=["'][^"']*\.access\.mcas\.ms[^"']*["']`)
	return mcasActionRegex.Match(body)
}

// handleMCASFlow handles the MCAS authentication flow when detected.
// It extracts the MCAS form data, POSTs to the MCAS endpoint, and returns
// the final response body containing the actual OIDC tokens.
func (ea *ESTSAuthenticator) handleMCASFlow(ctx context.Context, client *http.Client, body []byte) ([]byte, error) {
	bodyStr := string(body)

	// Extract the MCAS action URL from the form
	actionRegex := regexp.MustCompile(`<form[^>]+action=["']([^"']*\.access\.mcas\.ms[^"']*)["']`)
	actionMatch := actionRegex.FindStringSubmatch(bodyStr)
	if len(actionMatch) < 2 {
		return nil, &ESTSError{
			ErrorCode: "mcas_parse_error",
			Message:   "Failed to extract MCAS action URL from response",
		}
	}
	mcasActionURL := actionMatch[1]

	// Extract id_token from the form
	// Try: <input type="hidden" name="id_token" value="..."/>
	idTokenRegex := regexp.MustCompile(`<input[^>]+name=["']id_token["'][^>]+value=["']([^"']*)["']`)
	idTokenMatch := idTokenRegex.FindStringSubmatch(bodyStr)

	// Also try reversed order: value before name
	if idTokenMatch == nil {
		idTokenRegex = regexp.MustCompile(`<input[^>]+value=["']([^"']*)["'][^>]+name=["']id_token["']`)
		idTokenMatch = idTokenRegex.FindStringSubmatch(bodyStr)
	}

	if len(idTokenMatch) < 2 {
		return nil, &ESTSError{
			ErrorCode: "mcas_parse_error",
			Message:   "Failed to extract id_token from MCAS form",
		}
	}
	idToken := idTokenMatch[1]

	// POST id_token to the MCAS endpoint
	formData := url.Values{}
	formData.Set("id_token", idToken)

	req, err := http.NewRequestWithContext(ctx, "POST", mcasActionURL, strings.NewReader(formData.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create MCAS request: %w", err)
	}
	req.Header.Set("User-Agent", ea.userAgent)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to complete MCAS flow: %w", err)
	}
	defer resp.Body.Close()

	mcasBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read MCAS response: %w", err)
	}

	return mcasBody, nil
}
