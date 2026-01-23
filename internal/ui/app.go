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
	program *tea.Program
	model   Model
	onQuit  func() // Called when user requests quit (ctrl+c)
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

// RunSimple runs without the fancy UI (for non-interactive mode).
// It also selects on the done channel to allow graceful exit on context cancellation
// or fatal errors, preventing deadlock if results stop arriving.
func RunSimple(
	totalJobs int,
	resultsCh <-chan worker.JobResult,
	done <-chan struct{},
) {
	completed := 0
	failed := 0
	skipped := 0
	totalEvents := 0
	resultsReceived := 0

	fmt.Printf("Processing %d jobs...\n\n", totalJobs)

	for {
		select {
		case result, ok := <-resultsCh:
			if !ok {
				// Channel closed, exit
				fmt.Printf("\nComplete: %d succeeded, %d failed, %d skipped, %d total events\n",
					completed, failed, skipped, totalEvents)
				return
			}

			resultsReceived++

			// Check for fatal error - exit immediately (no interactive UI to keep visible)
			if result.Fatal {
				displayName := ""
				if result.Entity != nil {
					displayName = result.Entity.EntityDisplayName()
				} else if result.Job != nil {
					displayName = result.Job.EntityDisplayName()
				}
				fmt.Printf("\nFATAL ERROR: %s: %v\n", displayName, result.Error)
				return
			}

			// Determine display name based on job type
			displayName := ""
			isMerge := result.MergeJob != nil
			if result.Entity != nil {
				displayName = result.Entity.EntityDisplayName()
			} else if isMerge {
				displayName = result.MergeJob.MergeInfo.Hostname
			} else {
				displayName = result.Job.EntityDisplayName()
			}

			if result.Error != nil {
				failed++
				if isMerge {
					fmt.Printf("✗ %s (merge): %v\n", displayName, result.Error)
				} else {
					fmt.Printf("✗ %s: %v\n", displayName, result.Error)
				}
			} else if result.Skipped {
				skipped++
				fmt.Printf("⊘ %s: %s\n", displayName, result.SkippedReason)
			} else {
				completed++
				totalEvents += result.EventCount
				if isMerge {
					fmt.Printf("✓ %s (merged) (%s)\n",
						displayName,
						result.Duration.Round(time.Millisecond))
				} else if chunkInfo := result.Job.GetChunkInfo(); chunkInfo != nil {
					fmt.Printf("✓ %s [%s]: %d events (%s)\n",
						displayName,
						chunkInfo.ChunkLabel(),
						result.EventCount,
						result.Duration.Round(time.Millisecond))
				} else {
					fmt.Printf("✓ %s: %d events (%s)\n",
						displayName,
						result.EventCount,
						result.Duration.Round(time.Millisecond))
				}
			}

			// Exit when all expected results are received
			if resultsReceived >= totalJobs {
				fmt.Printf("\nComplete: %d succeeded, %d failed, %d skipped, %d total events\n",
					completed, failed, skipped, totalEvents)
				return
			}

		case <-done:
			// Pool signaled shutdown (e.g., fatal error or interrupt)
			fmt.Printf("\nShutdown requested. Processed %d/%d jobs: %d succeeded, %d failed, %d skipped, %d total events\n",
				resultsReceived, totalJobs, completed, failed, skipped, totalEvents)
			return
		}
	}
}
