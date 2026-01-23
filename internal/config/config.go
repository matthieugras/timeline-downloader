package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/karrick/tparse/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/matthieugras/timeline-downloader/internal/auth"
)

// Config holds all configuration for the application
type Config struct {
	// Authentication
	TenantID     string `mapstructure:"tenant-id"`
	ClientID     string `mapstructure:"client-id"`
	RefreshToken string `mapstructure:"refresh-token"`
	ESTSCookie   string `mapstructure:"ests-cookie"` // ESTSAUTHPERSISTENT cookie value

	// Input - Devices
	Devices    []string `mapstructure:"devices"`
	DeviceFile string   `mapstructure:"file"`

	// Input - Identities
	Identities   []string `mapstructure:"identities"`
	IdentityFile string   `mapstructure:"identity-file"`

	// Time range
	FromDate time.Time
	ToDate   time.Time
	Days     int `mapstructure:"days"`

	// Processing
	Workers   int           `mapstructure:"workers"`
	TimeChunk time.Duration `mapstructure:"-"` // Parsed manually via tparse (not by viper)
	NoChunk   bool          `mapstructure:"no-chunk"` // Disable chunking entirely

	// Output
	OutputDir string `mapstructure:"output"`
	Gzip      bool   `mapstructure:"gzip"`

	// Backoff
	BackoffInitial time.Duration `mapstructure:"backoff-initial"`
	BackoffMax     time.Duration `mapstructure:"backoff-max"`

	// HTTP client
	HTTPTimeout time.Duration `mapstructure:"timeout"`

	// Logging
	Verbose bool   `mapstructure:"verbose"`
	LogFile string `mapstructure:"log-file"`

	// Timeline API options
	GenerateIdentityEvents bool `mapstructure:"generate-identity-events"`
	IncludeIdentityEvents  bool `mapstructure:"include-identity-events"`
	SupportMdiOnlyEvents   bool `mapstructure:"support-mdi-only-events"`
	IncludeSentinelEvents  bool `mapstructure:"include-sentinel-events"`
	PageSize               int  `mapstructure:"page-size"`

	// Retry settings
	MaxRetries int `mapstructure:"max-retries"`
}

// BackoffConfig holds exponential backoff settings
type BackoffConfig struct {
	InitialInterval     time.Duration
	MaxInterval         time.Duration
	Multiplier          float64
	RandomizationFactor float64
	MaxRetries          int
}

// DefaultBackoffConfig returns sensible default backoff settings
func DefaultBackoffConfig() BackoffConfig {
	return BackoffConfig{
		InitialInterval:     time.Second,
		MaxInterval:         60 * time.Second,
		Multiplier:          2.0,
		RandomizationFactor: 0.5,
		MaxRetries:          5,
	}
}

// SetupFlags configures CLI flags for the root command
func SetupFlags(cmd *cobra.Command) {
	// Authentication flags
	cmd.Flags().String("tenant-id", "", "Azure AD tenant ID")
	cmd.Flags().String("client-id", auth.DefaultClientID, "Azure AD application client ID")
	cmd.Flags().String("refresh-token", "", "OAuth refresh token (or set MDE_REFRESH_TOKEN env var)")
	cmd.Flags().String("ests-cookie", "", "ESTSAUTHPERSISTENT cookie value (or set MDE_ESTS_COOKIE env var)")

	// Input flags - Devices
	cmd.Flags().StringSliceP("devices", "d", nil, "Comma-separated list of hostnames or machine IDs (auto-detected)")
	cmd.Flags().StringP("file", "f", "", "File containing hostnames/machine IDs (one per line)")

	// Input flags - Identities
	cmd.Flags().StringSlice("identities", nil, "Comma-separated list of identity search terms (usernames, UPNs)")
	cmd.Flags().String("identity-file", "", "File containing identity search terms (one per line)")

	// Time range flags
	cmd.Flags().String("from", "", "Start date (RFC3339 format, e.g., 2025-12-01T00:00:00Z)")
	cmd.Flags().String("to", "", "End date (RFC3339 format)")
	cmd.Flags().Int("days", 7, "Number of days to look back (used if from/to not specified)")

	// Processing flags
	cmd.Flags().IntP("workers", "w", 5, "Number of parallel workers")
	cmd.Flags().String("timechunk", "2d", "Split downloads into time chunks for parallel processing (e.g., 2d, 48h, 30m)")
	cmd.Flags().Bool("no-chunk", false, "Disable time chunking entirely")

	// Output flags
	cmd.Flags().StringP("output", "o", "./output", "Output directory for JSONL files")
	cmd.Flags().BoolP("gzip", "z", false, "Compress final output files with gzip (.jsonl.gz)")

	// Backoff flags
	cmd.Flags().Duration("backoff-initial", 5*time.Second, "Initial backoff interval")
	cmd.Flags().Duration("backoff-max", 60*time.Second, "Maximum backoff interval")

	// HTTP client flags
	cmd.Flags().Duration("timeout", 5*time.Minute, "HTTP client timeout for API requests")

	// Other flags
	cmd.Flags().BoolP("verbose", "v", false, "Enable verbose logging")
	cmd.Flags().String("log-file", "", "Write full error messages to this file")

	// Timeline API options
	cmd.Flags().Bool("generate-identity-events", true, "Include generateIdentityEvents in API requests")
	cmd.Flags().Bool("include-identity-events", true, "Include includeIdentityEvents in API requests")
	cmd.Flags().Bool("support-mdi-only-events", true, "Include supportMdiOnlyEvents in API requests")
	cmd.Flags().Bool("include-sentinel-events", true, "Include includeSentinelEvents in API requests")
	cmd.Flags().Int("page-size", 1000, "Number of events per API page (1-1000)")

	// Retry settings
	cmd.Flags().Int("max-retries", 10, "Maximum retries for API requests")

	// Mark required flags (client-id has a default so not required)
	cmd.MarkFlagRequired("tenant-id")

	// Bind flags to viper
	viper.BindPFlags(cmd.Flags())

	// Bind environment variables
	viper.SetEnvPrefix("MDE")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
}

// Load loads configuration from flags, environment, and validates it
func Load() (*Config, error) {
	cfg := &Config{}

	// Unmarshal into config struct
	if err := viper.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Load refresh token from env if not set via flag
	if cfg.RefreshToken == "" {
		cfg.RefreshToken = os.Getenv("MDE_REFRESH_TOKEN")
	}

	// Load ESTS cookie from env if not set via flag
	if cfg.ESTSCookie == "" {
		cfg.ESTSCookie = os.Getenv("MDE_ESTS_COOKIE")
	}

	// Parse time chunk duration (unless --no-chunk is set)
	if !cfg.NoChunk {
		if err := cfg.parseTimeChunk(); err != nil {
			return nil, err
		}
	}

	// Parse time range
	if err := cfg.parseDateRange(); err != nil {
		return nil, err
	}

	// Load devices from file if specified
	if err := cfg.loadDevices(); err != nil {
		return nil, err
	}

	// Load identities from file if specified
	if err := cfg.loadIdentities(); err != nil {
		return nil, err
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// parseTimeChunk parses the --timechunk flag value using tparse for human-readable durations.
// Supports formats like "2d", "48h", "30m", etc.
func (c *Config) parseTimeChunk() error {
	timeChunkStr := viper.GetString("timechunk")
	if timeChunkStr == "" || timeChunkStr == "0" {
		c.TimeChunk = 0
		return nil
	}

	// Use tparse to parse human-readable duration (supports days, weeks, etc.)
	duration, err := tparse.AbsoluteDuration(time.Now(), timeChunkStr)
	if err != nil {
		return fmt.Errorf("invalid timechunk value %q: %w", timeChunkStr, err)
	}

	if duration < 0 {
		return fmt.Errorf("timechunk must be positive, got: %s", timeChunkStr)
	}

	c.TimeChunk = duration
	return nil
}

func (c *Config) parseDateRange() error {
	fromStr := viper.GetString("from")
	toStr := viper.GetString("to")

	if fromStr != "" && toStr != "" {
		var err error
		c.FromDate, err = time.Parse(time.RFC3339, fromStr)
		if err != nil {
			return fmt.Errorf("invalid from date: %w", err)
		}
		c.ToDate, err = time.Parse(time.RFC3339, toStr)
		if err != nil {
			return fmt.Errorf("invalid to date: %w", err)
		}
	} else {
		// Use days parameter
		c.ToDate = time.Now().UTC()
		c.FromDate = c.ToDate.AddDate(0, 0, -c.Days)
	}

	return nil
}

// loadEntities loads and deduplicates entities from flags and an optional file.
// Returns deduplicated list of entities.
func loadEntities(flagValues []string, filePath string, entityType string) ([]string, error) {
	seen := make(map[string]bool)
	var unique []string

	// Deduplicate from flags
	for _, v := range flagValues {
		v = strings.TrimSpace(v)
		if v != "" && !seen[v] {
			seen[v] = true
			unique = append(unique, v)
		}
	}

	// Add from file (also deduplicated)
	if filePath != "" {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s file: %w", entityType, err)
		}

		for line := range strings.SplitSeq(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "#") && !seen[line] {
				seen[line] = true
				unique = append(unique, line)
			}
		}
	}

	return unique, nil
}

func (c *Config) loadDevices() error {
	devices, err := loadEntities(c.Devices, c.DeviceFile, "device")
	if err != nil {
		return err
	}
	c.Devices = devices
	return nil
}

func (c *Config) loadIdentities() error {
	identities, err := loadEntities(c.Identities, c.IdentityFile, "identity")
	if err != nil {
		return err
	}
	c.Identities = identities
	return nil
}

// Validate checks that all required configuration is present and valid
func (c *Config) Validate() error {
	// TenantID is always required
	if c.TenantID == "" {
		return fmt.Errorf("tenant-id is required")
	}

	// Auto-detect auth method based on which credentials are provided
	hasESTSCookie := c.ESTSCookie != ""
	hasRefreshToken := c.RefreshToken != ""

	if hasESTSCookie && hasRefreshToken {
		return fmt.Errorf("both ests-cookie and refresh-token are set; please provide only one")
	}

	if !hasESTSCookie && !hasRefreshToken {
		return fmt.Errorf("authentication required: set either --ests-cookie (MDE_ESTS_COOKIE) or --refresh-token (MDE_REFRESH_TOKEN)")
	}

	// For refresh token auth, ensure client ID is set
	if hasRefreshToken && c.ClientID == "" {
		c.ClientID = auth.DefaultClientID
	}

	if len(c.Devices) == 0 && len(c.Identities) == 0 {
		return fmt.Errorf("at least one device or identity must be specified (via --devices, --file, --identities, or --identity-file)")
	}
	if c.FromDate.After(c.ToDate) || c.FromDate.Equal(c.ToDate) {
		return fmt.Errorf("from date must be before to date")
	}
	if c.Workers < 1 {
		return fmt.Errorf("workers must be at least 1")
	}
	if c.TimeChunk < 0 {
		return fmt.Errorf("timechunk must be positive")
	}

	return nil
}

// UseESTSAuth returns true if ESTS cookie authentication should be used
func (c *Config) UseESTSAuth() bool {
	return c.ESTSCookie != ""
}

// GetBackoffConfig returns backoff configuration from the config
func (c *Config) GetBackoffConfig() BackoffConfig {
	cfg := DefaultBackoffConfig()
	cfg.InitialInterval = c.BackoffInitial
	cfg.MaxInterval = c.BackoffMax
	return cfg
}
