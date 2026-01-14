package ui

import (
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/matthieugras/timeline-downloader/internal/backoff"
	"github.com/matthieugras/timeline-downloader/internal/worker"
)

// App wraps the Bubble Tea program
type App struct {
	program    *tea.Program
	model      Model
	onQuit     func() // Called when user requests quit (ctrl+c)
}

// NewApp creates a new UI application
func NewApp(
	totalDevices int,
	numWorkers int,
	resultsCh <-chan worker.JobResult,
	workerUpdates <-chan worker.WorkerStatus,
	bo *backoff.GlobalBackoff,
	onQuit func(),
) *App {
	model := NewModel(totalDevices, numWorkers, resultsCh, workerUpdates, onQuit)

	app := &App{
		model:  model,
		onQuit: onQuit,
	}

	// Set up backoff callbacks
	if bo != nil {
		bo.SetCallbacks(
			func(duration time.Duration) {
				if app.program != nil {
					app.program.Send(BackoffMsg{Active: true, Duration: duration})
				}
			},
			func() {
				if app.program != nil {
					app.program.Send(BackoffMsg{Active: false})
				}
			},
		)
	}

	return app
}

// Run starts the UI
func (a *App) Run() error {
	a.program = tea.NewProgram(a.model, tea.WithAltScreen())

	if _, err := a.program.Run(); err != nil {
		return fmt.Errorf("UI error: %w", err)
	}

	return nil
}

// Send sends a message to the UI
func (a *App) Send(msg tea.Msg) {
	if a.program != nil {
		a.program.Send(msg)
	}
}

// Quit quits the UI
func (a *App) Quit() {
	if a.program != nil {
		a.program.Quit()
	}
}

// SetOnQuit sets a callback to be called when user requests quit
func (a *App) SetOnQuit(fn func()) {
	a.onQuit = fn
}

// RunSimple runs without the fancy UI (for non-interactive mode)
func RunSimple(
	totalJobs int,
	resultsCh <-chan worker.JobResult,
) {
	completed := 0
	failed := 0
	totalEvents := 0

	fmt.Printf("Processing %d jobs...\n\n", totalJobs)

	for result := range resultsCh {
		// Determine hostname based on job type
		hostname := ""
		isMerge := result.Job.MergeInfo != nil
		if result.Device != nil {
			hostname = result.Device.ComputerDNSName
		} else if isMerge {
			hostname = result.Job.MergeInfo.Hostname
		} else {
			hostname = result.Job.DeviceInput.Value
		}

		if result.Error != nil {
			failed++
			if isMerge {
				fmt.Printf("✗ %s (merge): %v\n", hostname, result.Error)
			} else {
				fmt.Printf("✗ %s: %v\n", hostname, result.Error)
			}
		} else {
			completed++
			totalEvents += result.EventCount
			if isMerge {
				fmt.Printf("✓ %s (merged) (%s)\n",
					hostname,
					result.Duration.Round(time.Millisecond))
			} else if result.Job.ChunkInfo != nil {
				fmt.Printf("✓ %s [%s]: %d events (%s)\n",
					hostname,
					result.Job.ChunkInfo.ChunkLabel(),
					result.EventCount,
					result.Duration.Round(time.Millisecond))
			} else {
				fmt.Printf("✓ %s: %d events (%s)\n",
					hostname,
					result.EventCount,
					result.Duration.Round(time.Millisecond))
			}
		}
	}

	fmt.Printf("\nComplete: %d succeeded, %d failed, %d total events\n",
		completed, failed, totalEvents)
}
