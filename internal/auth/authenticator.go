package auth

import (
	"context"
	"net/http"
)

// DefaultUserAgent mimics Edge browser for API requests
const DefaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0"

// Authenticator is the interface for authentication providers.
// It supports both Bearer token authentication (refresh token flow)
// and cookie-based authentication (ESTS cookie flow).
type Authenticator interface {
	// Authenticate adds authentication to an HTTP request.
	// For Bearer token auth, this sets the Authorization header.
	// For ESTS auth, this adds cookies and X-XSRF-TOKEN header.
	Authenticate(ctx context.Context, req *http.Request) error

	// ForceRefresh forces credential refresh.
	// Call this after receiving a 401 Unauthorized response.
	ForceRefresh(ctx context.Context) error
}
