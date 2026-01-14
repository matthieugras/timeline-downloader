package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/matthieugras/timeline-downloader/internal/api"
	"github.com/matthieugras/timeline-downloader/internal/auth"
	"github.com/matthieugras/timeline-downloader/internal/backoff"
	"github.com/matthieugras/timeline-downloader/internal/config"
	"github.com/matthieugras/timeline-downloader/internal/logging"
	"github.com/matthieugras/timeline-downloader/internal/output"
	"github.com/matthieugras/timeline-downloader/internal/ui"
	"github.com/matthieugras/timeline-downloader/internal/worker"
)

var (
	version = "0.1.0"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "timeline-dl",
		Short: "Download Microsoft Defender device timelines",
		Long: `A CLI tool to download device timeline events from Microsoft Defender XDR.

Supports parallel processing of multiple devices with automatic token refresh
and rate limiting.`,
		Version:       version,
		RunE:          run,
		SilenceUsage:  true, // Don't print usage on errors
		SilenceErrors: true, // We handle error output ourselves
	}

	// Setup flags
	config.SetupFlags(rootCmd)

	// Add simple mode flag
	rootCmd.Flags().Bool("simple", false, "Use simple output mode (no fancy UI)")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		logging.Close() // Ensure log file is flushed before exit
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	// Initialize logger if log file specified
	if cfg.LogFile != "" {
		if err := logging.Init(cfg.LogFile); err != nil {
			return fmt.Errorf("failed to initialize logging: %w", err)
		}
		defer logging.Close()
		logging.Info("Configuration loaded: %d devices, %d workers, from=%s to=%s",
			len(cfg.Devices), cfg.Workers, cfg.FromDate.Format("2006-01-02"), cfg.ToDate.Format("2006-01-02"))
	}

	// Setup context with cancellation for signal handling
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived interrupt signal, shutting down...")
		cancel()
	}()

	// Create shared HTTP client for connection pooling across all components
	// This enables TCP Keep-Alive reuse and centralized timeout configuration
	httpClient := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Create authenticator based on which credentials were provided
	var authenticator auth.Authenticator
	if cfg.UseESTSAuth() {
		authenticator = auth.NewESTSAuthenticator(httpClient, cfg.ESTSCookie, cfg.TenantID)
		if cfg.Verbose {
			fmt.Println("Using ESTS cookie authentication")
		}
	} else {
		authenticator = auth.NewRefreshTokenAuthenticator(httpClient, cfg.TenantID, cfg.ClientID, cfg.RefreshToken)
		if cfg.Verbose {
			fmt.Println("Using refresh token authentication")
		}
	}

	// Setup backoff
	bo := backoff.New(cfg.GetBackoffConfig())

	// Setup timeline options
	timelineOpts := api.TimelineOptions{
		GenerateIdentityEvents: cfg.GenerateIdentityEvents,
		IncludeIdentityEvents:  cfg.IncludeIdentityEvents,
		SupportMdiOnlyEvents:   cfg.SupportMdiOnlyEvents,
		IncludeSentinelEvents:  cfg.IncludeSentinelEvents,
		PageSize:               cfg.PageSize,
	}

	// Setup API client (shares HTTP client for connection pooling)
	client := api.NewClient(httpClient, authenticator, bo, timelineOpts, cfg.MaxRetries)

	// Setup file manager
	fileManager, err := output.NewFileManager(cfg.OutputDir)
	if err != nil {
		return fmt.Errorf("failed to setup output directory: %w", err)
	}

	// Convert device strings to DeviceInput (auto-detects machine IDs vs hostnames)
	devices := make([]api.DeviceInput, len(cfg.Devices))
	for i, d := range cfg.Devices {
		devices[i] = api.NewDeviceInput(d)
	}

	// Setup worker pool (using pond library)
	pool := worker.NewPool(worker.PoolConfig{
		NumWorkers:  cfg.Workers,
		Client:      client,
		Backoff:     bo,
		FileManager: fileManager,
		FromDate:    cfg.FromDate,
		ToDate:      cfg.ToDate,
	})

	// Generate jobs (with chunking if enabled)
	var jobs []*worker.Job
	jobID := 0
	for _, device := range devices {
		deviceJobs := worker.SplitIntoChunks(device, cfg.FromDate, cfg.ToDate, cfg.TimeChunkDays, jobID)
		jobs = append(jobs, deviceJobs...)
		jobID += len(deviceJobs)
	}

	// Submit all jobs to the pool
	for _, job := range jobs {
		pool.Submit(job)
	}

	// Calculate total expected results:
	// - One result per download job
	// - One merge result per chunked device (sent by the worker that completes the last chunk)
	chunkedDevices := make(map[string]bool)
	for _, job := range jobs {
		if job.ChunkInfo != nil {
			chunkedDevices[job.ChunkInfo.DeviceKey] = true
		}
	}
	totalExpectedResults := len(jobs) + len(chunkedDevices)

	// Check for simple mode
	simpleMode, _ := cmd.Flags().GetBool("simple")

	if simpleMode || !isTerminal() {
		// Simple output mode
		ui.RunSimple(totalExpectedResults, pool.Results())
		pool.StopAndWait()
	} else {
		// Fancy UI mode
		app := ui.NewApp(
			totalExpectedResults,
			cfg.Workers,
			pool.Results(),
			pool.StatusUpdates(),
			bo,
			pool.Stop, // Stop pool immediately on ctrl+c
		)

		// Run UI (blocks until done)
		if err := app.Run(); err != nil {
			return err
		}

		pool.StopAndWait()
	}

	return nil
}

// isTerminal checks if stdout is a terminal
func isTerminal() bool {
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
