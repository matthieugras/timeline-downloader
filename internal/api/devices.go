package api

import (
	"context"
	"fmt"
	"net/url"
)

// ResolveHostname resolves a hostname to a Device with full details
// This is a 2-step process:
// 1. Search for machine by hostname prefix
// 2. Get full machine details including SenseClientVersion
func (c *Client) ResolveHostname(ctx context.Context, hostname string) (*Device, error) {
	// Step 1: Search for machine by hostname prefix
	searchPath := fmt.Sprintf("/apiproxy/mtp/ndr/machines?hideLowFidelityDevices=true&lookingBackIndays=180&machineSearchPrefix=%s&pageIndex=1&pageSize=25&sortByField=riskscore&sortOrder=Descending",
		url.QueryEscape(hostname))

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
	path := fmt.Sprintf("/apiproxy/mtp/getMachine/machines?machineId=%s&idType=SenseMachineId&readFromCache=true&lookingBackIndays=180",
		url.QueryEscape(machineID))

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

	return &device, nil
}

// ResolveDevice resolves a device input to full device details
func (c *Client) ResolveDevice(ctx context.Context, input DeviceInput) (*Device, error) {
	if input.IsMachineID {
		return c.GetDevice(ctx, input.Value)
	}
	return c.ResolveHostname(ctx, input.Value)
}
