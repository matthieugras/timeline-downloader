# Timeline Downloader

A CLI tool for downloading device and identity timeline events from Microsoft Defender XDR using the unofficial apiproxy feature. Features parallel processing, automatic token refresh, intelligent rate limiting, and an interactive terminal UI.

## Features

- **Parallel Processing**: Download timelines from multiple devices and identities concurrently with configurable worker pools
- **Time Chunking**: Split large date ranges into parallel chunks for faster downloads
- **Dual Authentication**: Supports both OAuth 2.0 refresh tokens and ESTS cookie-based authentication
- **Automatic Token Refresh**: Handles token expiration and rotation transparently
- **Intelligent Rate Limiting**: Global exponential backoff with jitter prevents API throttling
- **Interactive Terminal UI**: Real-time progress display with per-worker status tracking
- **JSONL Output**: Machine-readable format for downstream SIEM/analysis tools
- **Gzip Compression**: Optional output compression with `--gzip` flag
- **Flexible Input**: Accept device hostnames, machine IDs, identity usernames/UPNs, or load from file
- **Identity Timeline Support**: Download timeline events for identities (users/accounts) in addition to devices

## Installation

### From Source

```bash
git clone https://github.com/matthieugras/timeline-downloader.git
cd timeline-downloader
go build -o timeline-dl ./cmd/timeline-dl
```

### Requirements

- Go 1.24.2 or later
- Valid Microsoft Defender XDR credentials (refresh token or ESTS cookie)

## Quick Start

### Using Refresh Token

```bash
timeline-dl \
  --tenant-id YOUR_TENANT_ID \
  --refresh-token "0.ABC..." \
  --devices workstation01,workstation02 \
  --days 7
```

### Using ESTS Cookie

```bash
timeline-dl \
  --tenant-id YOUR_TENANT_ID \
  --ests-cookie "0.ABC..." \
  --devices workstation01
```

### Using Environment Variables

```bash
export MDE_TENANT_ID="YOUR_TENANT_ID"
export MDE_REFRESH_TOKEN="0.ABC..."
timeline-dl --devices workstation01,workstation02
```

## Authentication

### OAuth 2.0 Refresh Token (Recommended)

Uses standard OAuth 2.0 refresh token flow against Azure AD. Tokens automatically refresh before expiration.

**Required:**
- `--tenant-id`: Your Azure AD tenant ID
- `--refresh-token` or `MDE_REFRESH_TOKEN`: OAuth refresh token

**Optional:**
- `--client-id`: OAuth client ID (defaults to Microsoft Teams app ID)

### ESTS Cookie Authentication

Uses browser session cookies for authentication. Useful when refresh tokens are not available.

**Required:**
- `--tenant-id`: Your Azure AD tenant ID
- `--ests-cookie` or `MDE_ESTS_COOKIE`: ESTSAUTHPERSISTENT cookie value from browser

**Obtaining ESTS Cookie:**
1. Open an incognito/private browser window
2. Navigate to https://security.microsoft.com
3. Sign in to Microsoft Defender XDR
4. Extract the `ESTSAUTHPERSISTENT` cookie from browser developer tools

## Configuration

### CLI Flags

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--tenant-id` | | Azure AD tenant ID | (required) |
| `--refresh-token` | | OAuth refresh token | |
| `--ests-cookie` | | ESTS cookie value | |
| `--client-id` | | OAuth client ID | Teams app ID |
| `--devices` | `-d` | Comma-separated device list | |
| `--file` | `-f` | File with device list | |
| `--identities` | | Comma-separated identity search terms (usernames, UPNs) | |
| `--identity-file` | | File with identity search terms | |
| `--from` | | Start date (RFC3339) | 7 days ago |
| `--to` | | End date (RFC3339) | now |
| `--days` | | Days to look back | 7 |
| `--workers` | `-w` | Parallel workers | 5 |
| `--timechunk` | | Time chunk size (e.g., 2d, 48h, 30m) | 2d |
| `--no-chunk` | | Disable time chunking entirely | false |
| `--output` | `-o` | Output directory | ./output |
| `--gzip` | `-z` | Compress output with gzip (.jsonl.gz) | false |
| `--simple` | | Disable fancy UI | false |
| `--verbose` | `-v` | Verbose output | false |
| `--log-file` | | Log file path | |
| `--max-retries` | | Maximum API request retries | 10 |
| `--backoff-initial` | | Initial backoff | 5s |
| `--backoff-max` | | Maximum backoff | 60s |
| `--timeout` | | HTTP request timeout | 5m |
| `--generate-identity-events` | | Include identity generation events | true |
| `--include-identity-events` | | Include identity events | true |
| `--support-mdi-only-events` | | Include MDI-only events | true |
| `--include-sentinel-events` | | Include Sentinel events | true |
| `--page-size` | | Events per API page (1-1000) | 1000 |

### Environment Variables

All flags can be set via environment variables with `MDE_` prefix:

- `MDE_TENANT_ID`
- `MDE_REFRESH_TOKEN`
- `MDE_ESTS_COOKIE`
- `MDE_CLIENT_ID`
- `MDE_DEVICES`

### Device File Format

```
# Comments start with #
workstation01.example.com
workstation02
a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0  # Machine IDs also work
server.domain.local
```

### Identity File Format

```
# Comments start with #
john.doe@example.com    # UPN format
jane.smith              # Username only
admin@contoso.com
```

## Output

Timeline events are written to JSONL files (one JSON object per line). With `--gzip`, files are compressed with `.jsonl.gz` extension.

### Device Timelines

```
output/
├── workstation01_a1b2c3d4..._timeline.jsonl
├── workstation02_e5f6g7h8..._timeline.jsonl
└── server_i9j0k1l2..._timeline.jsonl
```

### Identity Timelines

```
output/
├── john.doe_abc123def456..._identity_timeline.jsonl
├── jane.smith_789xyz012..._identity_timeline.jsonl
└── admin_345uvw678..._identity_timeline.jsonl
```

### Combined Output (with gzip)

```
output/
├── workstation01_a1b2c3d4..._timeline.jsonl.gz
├── john.doe_abc123def456..._identity_timeline.jsonl.gz
└── ...
```

Each event contains full forensic details:
- Action timestamp and type
- Machine information
- Process details (initiating and target)
- File, network, and registry events
- User/account information

## Examples

### Download Last 30 Days for Multiple Devices

```bash
timeline-dl \
  --tenant-id $TENANT_ID \
  --refresh-token "$REFRESH_TOKEN" \
  --devices "host1,host2,host3" \
  --days 30 \
  --workers 10
```

### Download Identity Timelines

```bash
timeline-dl \
  --tenant-id $TENANT_ID \
  --refresh-token "$REFRESH_TOKEN" \
  --identities "john.doe@example.com,jane.smith" \
  --days 14
```

### Download Both Device and Identity Timelines

```bash
timeline-dl \
  --tenant-id $TENANT_ID \
  --refresh-token "$REFRESH_TOKEN" \
  --devices workstation01,workstation02 \
  --identities "admin@contoso.com" \
  --days 7 \
  --workers 10
```

### Download with Time Chunking

Split a 30-day download into 7-day chunks for parallel processing:

```bash
timeline-dl \
  --tenant-id $TENANT_ID \
  --refresh-token "$REFRESH_TOKEN" \
  --devices workstation01 \
  --days 30 \
  --timechunk 7d \
  --workers 5
```

This creates 5 parallel jobs (7+7+7+7+2 days) that are automatically merged into the final output file.

### Download with Gzip Compression

```bash
timeline-dl \
  --tenant-id $TENANT_ID \
  --refresh-token "$REFRESH_TOKEN" \
  --devices workstation01 \
  --days 30 \
  --gzip
```

### Download Specific Date Range

```bash
timeline-dl \
  --tenant-id $TENANT_ID \
  --refresh-token "$REFRESH_TOKEN" \
  --file devices.txt \
  --from "2026-01-01T00:00:00Z" \
  --to "2026-01-15T00:00:00Z"
```

### Simple Mode for Scripts/Logging

```bash
timeline-dl \
  --tenant-id $TENANT_ID \
  --refresh-token "$REFRESH_TOKEN" \
  --devices workstation01 \
  --simple \
  --log-file timeline.log
```

## Troubleshooting

### Authentication Errors

**"invalid_grant" error:**
- Refresh token has expired or been revoked
- Obtain a new refresh token

**"Session information is not sufficient for single-sign-on" (Error 50058):**
- ESTS cookie is invalid or expired
- Use a fresh incognito browser session to obtain a new cookie

**401 Unauthorized after retry:**
- Credentials are no longer valid
- Tool will abort all workers (fatal error)

### Rate Limiting

The tool automatically handles rate limiting with exponential backoff:
- Initial wait: 5 seconds
- Maximum wait: 60 seconds
- Multiplier: 2x per retry
- Jitter: 0-50% randomization
- Max retries: 10

If you see frequent backoff messages, reduce the number of workers.

### Device Resolution

**"Device not found":**
- Verify the hostname/machine ID is correct
- Device must be onboarded to Microsoft Defender XDR
- Check that you have access to the device in the portal

**"Multiple devices found":**
- Hostname matches multiple devices
- Use the full FQDN or machine ID instead

## License

MIT License

Copyright (c) 2026 Matthieu Gras

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
