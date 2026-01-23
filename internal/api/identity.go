package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/matthieugras/timeline-downloader/internal/logging"
	"github.com/matthieugras/timeline-downloader/internal/output"
)

const (
	identitySearchURL   = "/apiproxy/mdi/identity/userapiservice/identities"
	identityResolveURL  = "/apiproxy/mdi/identity/userapiservice/user/resolve"
	identityTimelineURL = "/apiproxy/mdi/identity/userapiservice/timeline/mtp"

	// IdentityMaxPageSize is the maximum events per page for identity timeline
	IdentityMaxPageSize = 1000

	// IdentityMaxSkip is the maximum skip value before API returns 400
	IdentityMaxSkip = 9000
)

// IdentityInput represents user-provided identity search term.
// Analogous to DeviceInput for devices.
type IdentityInput struct {
	Value string // The search term (username, UPN, etc.)
}

// NewIdentityInput creates an IdentityInput from a search string
func NewIdentityInput(value string) IdentityInput {
	return IdentityInput{Value: value}
}

// IdentityIDs holds identifiers from the search API response
type IdentityIDs struct {
	SID           string `json:"sid"`
	UPN           string `json:"upn"`
	AccountName   string `json:"accountName"`
	AccountDomain string `json:"accountDomain"`
	RadiusUserID  string `json:"radiusUserId"`
}

// ComplexID represents the complex identity ID structure
type ComplexID struct {
	ID   string `json:"id"`
	Saas int    `json:"saas"`
	Inst int    `json:"inst"`
}

// UserIdentifiers is the full set of identifiers for timeline API calls.
// Populated by the resolve endpoint.
type UserIdentifiers struct {
	ThirdPartyProviderAccountID string     `json:"thirdPartyProviderAccountId,omitempty"`
	ThirdPartyIdentityProvider  string     `json:"thirdPartyIdentityProvider,omitempty"`
	ComplexID                   *ComplexID `json:"complexId,omitempty"`
	AD                          string     `json:"ad,omitempty"`
	AAD                         string     `json:"aad,omitempty"`
	SID                         string     `json:"sid,omitempty"`
	CloudSID                    string     `json:"cloudSid,omitempty"`
	AccountName                 string     `json:"accountName,omitempty"`
	AccountDomain               string     `json:"accountDomain,omitempty"`
	SentinelUPN                 string     `json:"sentinelUpn,omitempty"`
	UPN                         string     `json:"upn,omitempty"`
	ArmID                       string     `json:"armId,omitempty"`
	ArmIDs                      []string   `json:"armIds,omitempty"`
	SentinelWorkspaceID         string     `json:"sentinelWorkspaceId,omitempty"`
	RadiusUserID                string     `json:"radiusUserId,omitempty"`
	TenantID                    string     `json:"tenantId,omitempty"`
}

// Identity represents a resolved identity with full identifiers.
// Analogous to Device struct.
type Identity struct {
	UserIdentifiers UserIdentifiers
	DisplayName     string
	AccountName     string
	AccountDomain   string
}

func (*Identity) isResolvedEntity() {}

// EntityDisplayName returns the display name for the identity
func (i *Identity) EntityDisplayName() string {
	if i.AccountDomain != "" {
		return i.AccountName + "@" + i.AccountDomain
	}
	return i.AccountName
}

// PrimaryKey returns the unique identifier for deduplication (RadiusUserID)
func (i *Identity) PrimaryKey() string {
	return i.UserIdentifiers.RadiusUserID
}

// IdentitySearchResult represents a single identity from the search API
type IdentitySearchResult struct {
	IDs         IdentityIDs `json:"ids"`
	DisplayName string      `json:"displayName"`
	Status      string      `json:"status"`
	Type        string      `json:"type"`
}

// IdentitySearchResponse wraps the search API response
type IdentitySearchResponse struct {
	Data []IdentitySearchResult `json:"data"`
}

// IdentityResolveRequest is the request body for identity resolve API
type IdentityResolveRequest struct {
	Workloads       []string    `json:"workloads"`
	UserIdentifiers IdentityIDs `json:"userIdentifiers"`
}

// IdentityResolveResponse wraps the resolve API response
type IdentityResolveResponse struct {
	Errors    map[string]any `json:"errors"`
	Workloads []string       `json:"workloads"`
	Results   ResolveResults `json:"results"`
}

// ResolveResults contains the resolved identity details
type ResolveResults struct {
	IDs         UserIdentifiers `json:"ids"`
	DisplayName string          `json:"displayName"`
	TenantID    string          `json:"TenantId"`
}

// TimeframeFilter specifies the time range as unix timestamps
type TimeframeFilter struct {
	Between [2]int64 `json:"between"`
}

// IdentityTimelineFilters contains the filter configuration
type IdentityTimelineFilters struct {
	Timeframe TimeframeFilter `json:"Timeframe"`
}

// IdentityTimelineRequest is the request body for identity timeline API
type IdentityTimelineRequest struct {
	Count           int                     `json:"count"`
	Skip            int                     `json:"skip"`
	UserIdentifiers UserIdentifiers         `json:"userIdentifiers"`
	Filters         IdentityTimelineFilters `json:"filters"`
}

// IdentityTimelineResponse is the response from identity timeline API
type IdentityTimelineResponse struct {
	Data   []json.RawMessage `json:"data"`
	Count  int               `json:"count"`
	Errors map[string]any    `json:"errors"`
}

// mdiHeaders returns the headers required for MDI identity API calls
func mdiHeaders(tenantID string) map[string]string {
	return map[string]string{
		"m-package": "identities",
		"tenant-id": tenantID,
	}
}

// SearchIdentity searches for identities matching the given search text
func (c *Client) SearchIdentity(ctx context.Context, searchText string) ([]IdentitySearchResult, error) {
	reqBody := map[string]any{
		"PageSize":   20,
		"Skip":       0,
		"Filters":    map[string]any{},
		"SearchText": searchText,
	}

	resp, err := c.doPostRequestWithRetry(ctx, identitySearchURL, reqBody, c.maxRetries)
	if err != nil {
		return nil, fmt.Errorf("identity search failed: %w", err)
	}

	var searchResp IdentitySearchResponse
	if err := parseJSONResponse(resp, &searchResp); err != nil {
		return nil, fmt.Errorf("failed to parse identity search response: %w", err)
	}

	return searchResp.Data, nil
}

// ResolveIdentity resolves identity IDs to get full userIdentifiers
func (c *Client) ResolveIdentity(ctx context.Context, ids IdentityIDs, tenantID string) (*Identity, error) {
	reqBody := IdentityResolveRequest{
		Workloads:       []string{"graph", "radius", "mcas", "mtp", "mdi", "sentinel"},
		UserIdentifiers: ids,
	}

	resp, err := c.doPostRequestWithRetryAndHeaders(ctx, identityResolveURL, reqBody, c.maxRetries, mdiHeaders(tenantID))
	if err != nil {
		return nil, fmt.Errorf("identity resolve failed: %w", err)
	}

	var resolveResp IdentityResolveResponse
	if err := parseJSONResponse(resp, &resolveResp); err != nil {
		return nil, fmt.Errorf("failed to parse identity resolve response: %w", err)
	}

	// Build UserIdentifiers from resolve response, adding TenantID
	userIdents := resolveResp.Results.IDs
	if userIdents.TenantID == "" && resolveResp.Results.TenantID != "" {
		userIdents.TenantID = resolveResp.Results.TenantID
	}

	// Validate RadiusUserID is present (required for deduplication)
	if userIdents.RadiusUserID == "" {
		return nil, fmt.Errorf("identity %s@%s has no RadiusUserID (required for deduplication)", ids.AccountName, ids.AccountDomain)
	}

	return &Identity{
		UserIdentifiers: userIdents,
		DisplayName:     resolveResp.Results.DisplayName,
		AccountName:     ids.AccountName,
		AccountDomain:   ids.AccountDomain,
	}, nil
}

// ResolveIdentityBySearch performs search then resolve in one call.
// Analogous to ResolveDevice for devices.
func (c *Client) ResolveIdentityBySearch(ctx context.Context, searchText string, tenantID string) (*Identity, error) {
	// Step 1: Search
	results, err := c.SearchIdentity(ctx, searchText)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no identity found matching: %s", searchText)
	}
	if len(results) > 1 {
		// List matches for debugging
		names := make([]string, len(results))
		for i, r := range results {
			names[i] = r.DisplayName
		}
		return nil, fmt.Errorf("identity search not unique: %q matches %d identities: %v", searchText, len(results), names)
	}

	// Step 2: Resolve
	return c.ResolveIdentity(ctx, results[0].IDs, tenantID)
}

// DownloadIdentityTimeline downloads identity timeline events with sub-chunk handling.
// When skip exceeds 9000 (API limit), it restarts from the boundary timestamp using
// file truncation to handle boundary events cleanly.
//
// Identity timeline returns events in descending order (newest first), so when we hit
// the skip limit, we continue from the oldest event's timestamp to fetch older events.
//
// The strategy uses file truncation for boundary handling:
// 1. Track the oldest timestamp (batchMinTime) and count of events at that timestamp
// 2. When restarting, truncate the last boundaryCount lines and set currentTo = batchMinTime + 1s
// 3. Boundary events are cleanly refetched (no dedup needed)
// This prevents data loss while avoiding memory overhead for boundary event storage.
func DownloadIdentityTimeline(
	ctx context.Context,
	client *Client,
	identity *Identity,
	fromDate, toDate time.Time,
	pageSize int,
	writer output.TruncatableWriter,
	progressCallback func(eventCount int, currentDate time.Time),
) (int, error) {
	eventCount := 0
	pageSize = min(pageSize, IdentityMaxPageSize)

	currentTo := toDate
	skip := 0

	var batchMinTime time.Time
	boundaryCount := 0
	var prevBatchMinTime time.Time // For progress check on restart

	for {
		select {
		case <-ctx.Done():
			return eventCount, ctx.Err()
		default:
		}

		req := IdentityTimelineRequest{
			Count:           pageSize,
			Skip:            skip,
			UserIdentifiers: identity.UserIdentifiers,
			Filters: IdentityTimelineFilters{
				Timeframe: TimeframeFilter{
					Between: [2]int64{fromDate.Unix(), currentTo.Unix()},
				},
			},
		}

		resp, err := client.doPostRequestWithRetryAndHeaders(ctx, identityTimelineURL, req, client.maxRetries, mdiHeaders(identity.UserIdentifiers.TenantID))
		if err != nil {
			return eventCount, fmt.Errorf("identity timeline request failed: %w", err)
		}

		var timeline IdentityTimelineResponse
		if err := parseJSONResponse(resp, &timeline); err != nil {
			return eventCount, fmt.Errorf("failed to parse identity timeline response: %w", err)
		}

		// Process events
		for _, item := range timeline.Data {
			// Strip unstable fields before any processing
			item = output.StripUnstableFields(item)

			eventTime, err := output.ExtractIdentityTimestamp(item)
			if err != nil {
				return eventCount, fmt.Errorf("failed to parse event timestamp: %w", err)
			}

			// Skip events with missing timestamp (writer would filter anyway)
			if eventTime.IsZero() {
				logging.Warn("Skipping identity event with missing timestamp")
				continue
			}

			// Write event
			if err := writer.Write(item); err != nil {
				return eventCount, fmt.Errorf("failed to write event: %w", err)
			}
			eventCount++

			// Update boundary tracking
			if batchMinTime.IsZero() || eventTime.Before(batchMinTime) {
				batchMinTime = eventTime
				boundaryCount = 1
			} else if eventTime.Equal(batchMinTime) {
				boundaryCount++
			}
		}

		// Progress callback
		if progressCallback != nil {
			progressDate := currentTo
			if !batchMinTime.IsZero() {
				progressDate = batchMinTime
			}
			progressCallback(eventCount, progressDate)
		}

		// Check for end of data
		if len(timeline.Data) < pageSize {
			break
		}

		// Check if next skip would exceed limit
		nextSkip := skip + len(timeline.Data)
		if nextSkip > IdentityMaxSkip {
			if batchMinTime.IsZero() {
				logging.Warn("Skip limit reached but no events found")
				break
			}

			// Progress check: if batchMinTime unchanged from last restart, we're stuck
			if !prevBatchMinTime.IsZero() && batchMinTime.Equal(prevBatchMinTime) {
				// PATHOLOGICAL CASE: >10000 events at same second (pageSize + maxSkip)
				// We cannot fetch all events at this timestamp due to API limits.
				// Skip the problematic second entirely and continue with older events.
				logging.Warn("DATA LOSS: >%d events at %s cannot be fully retrieved (API limit). Skipping to older events.",
					IdentityMaxSkip+pageSize, batchMinTime.Format(time.RFC3339))

				// Use exclusive bound to skip batchMinTime entirely
				currentTo = batchMinTime
				skip = 0
				prevBatchMinTime = time.Time{}
				batchMinTime = time.Time{}
				boundaryCount = 0
				continue
			}

			// Truncate boundary events from the file
			if err := writer.TruncateLastLines(boundaryCount); err != nil {
				return eventCount, fmt.Errorf("failed to truncate boundary events: %w", err)
			}
			eventCount -= boundaryCount

			logging.Info("Skip limit reached (%d), restarting at %s (truncated %d boundary events)",
				nextSkip, batchMinTime.Format(time.RFC3339), boundaryCount)

			// INCLUSIVE: add 1 second so batchMinTime events are included
			currentTo = batchMinTime.Add(time.Second)
			skip = 0
			prevBatchMinTime = batchMinTime
			batchMinTime = time.Time{}
			boundaryCount = 0
			continue
		}

		skip = nextSkip
	}

	return eventCount, nil
}
