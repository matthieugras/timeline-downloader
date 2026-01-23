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
	IdentityMaxPageSize = 999

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
	ThirdPartyProviderAccountID string    `json:"thirdPartyProviderAccountId,omitempty"`
	ThirdPartyIdentityProvider  string    `json:"thirdPartyIdentityProvider,omitempty"`
	ComplexID                   *ComplexID `json:"complexId,omitempty"`
	AD                          string    `json:"ad,omitempty"`
	AAD                         string    `json:"aad,omitempty"`
	SID                         string    `json:"sid,omitempty"`
	CloudSID                    string    `json:"cloudSid,omitempty"`
	AccountName                 string    `json:"accountName,omitempty"`
	AccountDomain               string    `json:"accountDomain,omitempty"`
	SentinelUPN                 string    `json:"sentinelUpn,omitempty"`
	UPN                         string    `json:"upn,omitempty"`
	ArmID                       string    `json:"armId,omitempty"`
	ArmIDs                      []string  `json:"armIds,omitempty"`
	SentinelWorkspaceID         string    `json:"sentinelWorkspaceId,omitempty"`
	RadiusUserID                string    `json:"radiusUserId,omitempty"`
	TenantID                    string    `json:"tenantId,omitempty"`
}

// Identity represents a resolved identity with full identifiers.
// Analogous to Device struct.
type Identity struct {
	UserIdentifiers UserIdentifiers
	DisplayName     string
	AccountName     string
	AccountDomain   string
}

// Implement ResolvedEntity for Identity
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
	Errors    map[string]any  `json:"errors"`
	Workloads []string        `json:"workloads"`
	Results   ResolveResults  `json:"results"`
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

// identityTimelineEvent is used to extract Timestamp from raw event JSON
type identityTimelineEvent struct {
	Timestamp string `json:"Timestamp"`
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
func (c *Client) ResolveIdentity(ctx context.Context, ids IdentityIDs) (*Identity, error) {
	reqBody := IdentityResolveRequest{
		Workloads:       []string{"graph", "radius", "mcas", "mtp", "mdi", "sentinel"},
		UserIdentifiers: ids,
	}

	resp, err := c.doPostRequestWithRetry(ctx, identityResolveURL, reqBody, c.maxRetries)
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
func (c *Client) ResolveIdentityBySearch(ctx context.Context, searchText string) (*Identity, error) {
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
	return c.ResolveIdentity(ctx, results[0].IDs)
}

// DownloadIdentityTimeline downloads identity timeline events with sub-chunk handling.
// When skip exceeds 9000 (API limit), it restarts from the boundary timestamp using
// hash-based deduplication to avoid losing or duplicating events at the boundary.
//
// Identity timeline returns events in descending order (newest first), so when we hit
// the skip limit, we continue from the oldest event's timestamp to fetch older events.
//
// The strategy uses inclusive boundary with hash-based dedup:
// 1. Track the oldest timestamp (batchMinTime) and hashes of events at that timestamp
// 2. When restarting, set currentTo = batchMinTime + 1 second (INCLUSIVE of boundary)
// 3. Use hash set to skip already-written events at the boundary
// This prevents data loss while correctly deduplicating boundary events.
func (c *Client) DownloadIdentityTimeline(
	ctx context.Context,
	identity *Identity,
	fromDate, toDate time.Time,
	writer output.EventWriter,
	progressCallback func(eventCount int, currentDate time.Time),
) (int, error) {
	eventCount := 0
	pageSize := min(c.timelineOptions.PageSize, IdentityMaxPageSize)

	currentTo := toDate
	skip := 0

	var batchMinTime time.Time
	boundaryHashes := make(map[output.Hash128]struct{})

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

		resp, err := c.doPostRequestWithRetry(ctx, identityTimelineURL, req, c.maxRetries)
		if err != nil {
			return eventCount, fmt.Errorf("identity timeline request failed: %w", err)
		}

		var timeline IdentityTimelineResponse
		if err := parseJSONResponse(resp, &timeline); err != nil {
			return eventCount, fmt.Errorf("failed to parse identity timeline response: %w", err)
		}

		// Process events
		for _, item := range timeline.Data {
			eventTime, _ := getIdentityEventTimestamp(item)

			// Skip events with missing timestamp (writer would filter anyway)
			if eventTime.IsZero() {
				continue
			}

			hash := output.HashEvent(item)

			// Check for duplicate at boundary
			if !batchMinTime.IsZero() && eventTime.Equal(batchMinTime) {
				if _, isDup := boundaryHashes[hash]; isDup {
					continue // Skip duplicate
				}
			}

			// Write event
			if err := writer.Write(item); err != nil {
				return eventCount, fmt.Errorf("failed to write event: %w", err)
			}
			eventCount++

			// Update boundary tracking
			if batchMinTime.IsZero() || eventTime.Before(batchMinTime) {
				batchMinTime = eventTime
				boundaryHashes = map[output.Hash128]struct{}{hash: {}}
			} else if eventTime.Equal(batchMinTime) {
				boundaryHashes[hash] = struct{}{}
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

			// INCLUSIVE: add 1 second so batchMinTime events are included
			newTo := batchMinTime.Add(time.Second)

			// Safety: ensure we're making progress
			if !newTo.Before(currentTo) {
				// PATHOLOGICAL CASE: >9999 events at same second (pageSize + maxSkip)
				// We cannot fetch all events at this timestamp due to API limits.
				// Skip the problematic second entirely and continue with older events.
				logging.Warn("DATA LOSS: >%d events at %s cannot be fully retrieved (API limit). Skipping to older events.",
					IdentityMaxSkip+pageSize, batchMinTime.Format(time.RFC3339))

				// Use exclusive bound to skip batchMinTime entirely
				currentTo = batchMinTime
				skip = 0
				boundaryHashes = make(map[output.Hash128]struct{}) // Clear - moving past this second
				continue
			}

			logging.Info("Skip limit reached (%d), restarting at %s (boundary has %d events)",
				nextSkip, batchMinTime.Format(time.RFC3339), len(boundaryHashes))

			currentTo = newTo
			skip = 0
			// Keep boundaryHashes for deduplication
			continue
		}

		skip = nextSkip
	}

	return eventCount, nil
}

// getIdentityEventTimestamp extracts the Timestamp from a raw identity timeline event JSON
func getIdentityEventTimestamp(data json.RawMessage) (time.Time, error) {
	var event identityTimelineEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return time.Time{}, err
	}

	if event.Timestamp == "" {
		return time.Time{}, nil
	}

	return time.Parse(time.RFC3339, event.Timestamp)
}
