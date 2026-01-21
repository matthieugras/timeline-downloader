package ui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/matthieugras/timeline-downloader/internal/worker"
)

// Model represents the UI state
type Model struct {
	// Progress tracking
	totalDevices     int
	completedDevices int
	failedDevices    int
	totalEvents      int

	// Worker tracking
	workers       []worker.WorkerStatus
	numWorkers    int
	workerUpdates <-chan worker.WorkerStatus

	// Backoff state
	isBackingOff     bool
	backoffRemaining time.Duration

	// Progress bars
	overallProgress  progress.Model
	workerProgress   []progress.Model

	// Results channel
	resultsCh <-chan worker.JobResult

	// Recent results for display
	recentResults []resultInfo
	maxRecent     int

	// Errors
	errors []string

	// Dimensions
	width  int
	height int

	// State
	quitting   bool
	done       bool
	startTime  time.Time
	finishTime time.Time

	// Quit callback
	onQuit func()
}

type resultInfo struct {
	hostname   string
	eventCount int
	success    bool
	errorMsg   string
	isMerge    bool   // true for merge results
	chunkLabel string // e.g., "1/4" for chunk downloads
}

// Message types
type ResultMsg worker.JobResult
type WorkerStatusMsg worker.WorkerStatus
type BackoffMsg struct {
	Active   bool
	Duration time.Duration
}
type TickMsg time.Time
type DoneMsg struct{}

// NewModel creates a new UI model
func NewModel(
	totalDevices int,
	numWorkers int,
	resultsCh <-chan worker.JobResult,
	workerUpdates <-chan worker.WorkerStatus,
	onQuit func(),
) Model {
	prog := progress.New(
		progress.WithGradient(ProgressGradientStart, ProgressGradientEnd),
		progress.WithWidth(40),
	)

	workers := make([]worker.WorkerStatus, numWorkers)
	workerProgs := make([]progress.Model, numWorkers)
	for i := range workers {
		workers[i] = worker.WorkerStatus{ID: i, State: worker.WorkerStateIdle}
		workerProgs[i] = progress.New(
			progress.WithGradient(ProgressGradientStart, ProgressGradientEnd),
			progress.WithWidth(15),
			progress.WithoutPercentage(),
		)
	}

	return Model{
		totalDevices:    totalDevices,
		numWorkers:      numWorkers,
		workers:         workers,
		workerUpdates:   workerUpdates,
		overallProgress: prog,
		workerProgress:  workerProgs,
		resultsCh:       resultsCh,
		recentResults:   make([]resultInfo, 0, 10),
		maxRecent:       5,
		errors:          make([]string, 0),
		startTime:       time.Now(),
		onQuit:          onQuit,
	}
}

// Init initializes the model
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		tickCmd(),
		waitForResult(m.resultsCh),
		waitForWorkerStatus(m.workerUpdates),
	)
}

// Update handles messages
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.quitting = true
			if m.onQuit != nil {
				m.onQuit()
			}
			return m, tea.Quit
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.overallProgress.Width = max(msg.Width-30, 20)
		return m, nil

	case ResultMsg:
		result := worker.JobResult(msg)
		if result.Error != nil {
			m.failedDevices++
			// Get hostname based on job type
			hostname := ""
			isMerge := result.Job.MergeInfo != nil
			chunkLabel := ""
			if result.Device != nil {
				hostname = result.Device.ComputerDNSName
			} else if isMerge {
				hostname = result.Job.MergeInfo.Hostname
			} else {
				hostname = result.Job.DeviceInput.Value
			}
			if result.Job.ChunkInfo != nil {
				chunkLabel = result.Job.ChunkInfo.ChunkLabel()
			}
			m.addRecentResult(resultInfo{
				hostname:   hostname,
				success:    false,
				errorMsg:   result.Error.Error(),
				isMerge:    isMerge,
				chunkLabel: chunkLabel,
			})
			m.errors = append(m.errors, fmt.Sprintf("%s: %v", hostname, result.Error))
		} else {
			m.completedDevices++
			m.totalEvents += result.EventCount
			// Get hostname (merge jobs have nil Device, use MergeInfo.Hostname instead)
			hostname := ""
			isMerge := result.Job.MergeInfo != nil
			chunkLabel := ""
			if result.Device != nil {
				hostname = result.Device.ComputerDNSName
			} else if isMerge {
				hostname = result.Job.MergeInfo.Hostname
			}
			if result.Job.ChunkInfo != nil {
				chunkLabel = result.Job.ChunkInfo.ChunkLabel()
			}
			m.addRecentResult(resultInfo{
				hostname:   hostname,
				eventCount: result.EventCount,
				success:    true,
				isMerge:    isMerge,
				chunkLabel: chunkLabel,
			})
		}
		// Stop timer when all devices are processed
		if m.completedDevices+m.failedDevices >= m.totalDevices && m.finishTime.IsZero() {
			m.finishTime = time.Now()
		}
		return m, waitForResult(m.resultsCh)

	case WorkerStatusMsg:
		status := worker.WorkerStatus(msg)
		if status.ID >= 0 && status.ID < len(m.workers) {
			m.workers[status.ID] = status
		}
		return m, waitForWorkerStatus(m.workerUpdates)

	case BackoffMsg:
		m.isBackingOff = msg.Active
		m.backoffRemaining = msg.Duration
		return m, nil

	case TickMsg:
		return m, tickCmd()

	case DoneMsg:
		m.done = true
		m.finishTime = time.Now()
		return m, nil // Keep TUI visible, user can press 'q' to quit

	case progress.FrameMsg:
		progressModel, cmd := m.overallProgress.Update(msg)
		m.overallProgress = progressModel.(progress.Model)
		return m, cmd
	}

	return m, nil
}

func (m *Model) addRecentResult(r resultInfo) {
	m.recentResults = append(m.recentResults, r)
	if len(m.recentResults) > m.maxRecent {
		m.recentResults = m.recentResults[1:]
	}
}

// View renders the UI
func (m Model) View() string {
	if m.quitting {
		return m.renderFinalSummary()
	}

	var b strings.Builder

	// Header
	header := TitleStyle.Render(" Microsoft Defender Timeline Downloader ")
	b.WriteString(header + "\n\n")

	// Overall progress
	completed := m.completedDevices + m.failedDevices
	pct := float64(completed) / float64(m.totalDevices)
	progressBar := m.overallProgress.ViewAs(pct)
	progressLine := fmt.Sprintf("Progress: %s %d/%d jobs",
		progressBar, completed, m.totalDevices)
	b.WriteString(progressLine + "\n\n")

	// Stats
	var elapsed time.Duration
	if m.finishTime.IsZero() {
		elapsed = time.Since(m.startTime).Round(time.Second)
	} else {
		elapsed = m.finishTime.Sub(m.startTime).Round(time.Second)
	}
	stats := fmt.Sprintf("Completed: %s  Failed: %s  Events: %s  Elapsed: %s",
		SuccessStyle.Render(fmt.Sprintf("%d", m.completedDevices)),
		ErrorStyle.Render(fmt.Sprintf("%d", m.failedDevices)),
		HighlightStyle.Render(fmt.Sprintf("%d", m.totalEvents)),
		elapsed)
	b.WriteString(stats + "\n\n")

	// Workers status
	b.WriteString(MutedStyle.Render("Workers:") + "\n")
	for i, w := range m.workers {
		var statusStr string
		switch w.State {
		case worker.WorkerStateIdle:
			statusStr = WorkerIdleStyle.Render(fmt.Sprintf("  [%2d] idle", w.ID))
		case worker.WorkerStateWorking:
			device := w.CurrentDevice
			maxDeviceLen := 35
			if len(device) > maxDeviceLen {
				device = device[:maxDeviceLen-3] + "..."
			}
			// Calculate date-based progress percentage
			pct := 0.0
			if !w.FromDate.IsZero() && !w.ToDate.IsZero() && !w.CurrentDate.IsZero() {
				totalRange := w.ToDate.Sub(w.FromDate).Seconds()
				currentProgress := w.CurrentDate.Sub(w.FromDate).Seconds()
				if totalRange > 0 {
					pct = currentProgress / totalRange
					if pct > 1 {
						pct = 1
					}
					if pct < 0 {
						pct = 0
					}
				}
			}
			progressBar := m.workerProgress[i].ViewAs(pct)
			if w.ChunkLabel != "" {
				statusStr = WorkerWorkingStyle.Render(fmt.Sprintf("  [%2d] %-35s [%5s] %s %3.0f%% (%d)", w.ID, device, w.ChunkLabel, progressBar, pct*100, w.Progress))
			} else {
				statusStr = WorkerWorkingStyle.Render(fmt.Sprintf("  [%2d] %-35s         %s %3.0f%% (%d)", w.ID, device, progressBar, pct*100, w.Progress))
			}
		case worker.WorkerStateBackingOff:
			device := w.CurrentDevice
			maxDeviceLen := 35
			if len(device) > maxDeviceLen {
				device = device[:maxDeviceLen-3] + "..."
			}
			progressBar := m.workerProgress[i].ViewAs(0) // Show empty/paused bar
			if w.ChunkLabel != "" {
				statusStr = WorkerBackoffStyle.Render(fmt.Sprintf("  [%2d] %-35s [%5s] %s backing off...", w.ID, device, w.ChunkLabel, progressBar))
			} else {
				statusStr = WorkerBackoffStyle.Render(fmt.Sprintf("  [%2d] %-35s         %s backing off...", w.ID, device, progressBar))
			}
		case worker.WorkerStateMerging:
			device := w.CurrentDevice
			if len(device) > 35 {
				device = device[:32] + "..."
			}
			// Calculate byte-based progress
			pct := 0.0
			if w.TotalBytes > 0 {
				pct = float64(w.BytesCopied) / float64(w.TotalBytes)
				if pct > 1 {
					pct = 1
				}
			}
			progressBar := m.workerProgress[i].ViewAs(pct)
			mbCopied := float64(w.BytesCopied) / (1024 * 1024)
			mbTotal := float64(w.TotalBytes) / (1024 * 1024)
			statusStr = WorkerMergingStyle.Render(
				fmt.Sprintf("  [%2d] %-35s [merge] %s %6.1f/%6.1f MB",
					w.ID, device, progressBar, mbCopied, mbTotal))
		case worker.WorkerStateDone:
			statusStr = MutedStyle.Render(fmt.Sprintf("  [%2d] done", w.ID))
		}
		b.WriteString(statusStr + "\n")
	}

	// Backoff indicator
	if m.isBackingOff {
		b.WriteString("\n")
		backoffMsg := WarningStyle.Render(
			fmt.Sprintf("⚠ Rate limited - backing off for %s", m.backoffRemaining.Round(time.Second)))
		b.WriteString(backoffMsg + "\n")
	}

	// Recent results
	if len(m.recentResults) > 0 {
		b.WriteString("\n" + MutedStyle.Render("Recent:") + "\n")
		for _, r := range m.recentResults {
			var resultLine string
			if r.success {
				if r.isMerge {
					// Merge results: no event count
					resultLine = SuccessStyle.Render(fmt.Sprintf("  ✓ %s (merged)", r.hostname))
				} else if r.chunkLabel != "" {
					// Chunk download: show chunk label and event count
					resultLine = SuccessStyle.Render(fmt.Sprintf("  ✓ %s [%s] (%d events)", r.hostname, r.chunkLabel, r.eventCount))
				} else {
					// Regular download: show event count
					resultLine = SuccessStyle.Render(fmt.Sprintf("  ✓ %s (%d events)", r.hostname, r.eventCount))
				}
			} else {
				errMsg := r.errorMsg
				if len(errMsg) > 50 {
					errMsg = errMsg[:47] + "..."
				}
				if r.isMerge {
					resultLine = ErrorStyle.Render(fmt.Sprintf("  ✗ %s (merge): %s", r.hostname, errMsg))
				} else if r.chunkLabel != "" {
					resultLine = ErrorStyle.Render(fmt.Sprintf("  ✗ %s [%s]: %s", r.hostname, r.chunkLabel, errMsg))
				} else {
					resultLine = ErrorStyle.Render(fmt.Sprintf("  ✗ %s: %s", r.hostname, errMsg))
				}
			}
			b.WriteString(resultLine + "\n")
		}
	}

	// Footer
	footer := FooterStyle.Render("Press 'q' to quit")
	b.WriteString("\n" + footer)

	return lipgloss.NewStyle().Padding(1, 2).Render(b.String())
}

func (m Model) renderFinalSummary() string {
	var b strings.Builder

	elapsed := time.Since(m.startTime).Round(time.Second)

	b.WriteString("\n")
	b.WriteString(TitleStyle.Render(" Download Complete ") + "\n\n")

	b.WriteString(fmt.Sprintf("Total jobs:     %d\n", m.totalDevices))
	b.WriteString(fmt.Sprintf("Completed:      %s\n", SuccessStyle.Render(fmt.Sprintf("%d", m.completedDevices))))
	b.WriteString(fmt.Sprintf("Failed:         %s\n", ErrorStyle.Render(fmt.Sprintf("%d", m.failedDevices))))
	b.WriteString(fmt.Sprintf("Total events:   %s\n", HighlightStyle.Render(fmt.Sprintf("%d", m.totalEvents))))
	b.WriteString(fmt.Sprintf("Duration:       %s\n", elapsed))

	if len(m.errors) > 0 && len(m.errors) <= 10 {
		b.WriteString("\n" + ErrorStyle.Render("Errors:") + "\n")
		for _, err := range m.errors {
			b.WriteString(fmt.Sprintf("  • %s\n", err))
		}
	} else if len(m.errors) > 10 {
		b.WriteString("\n" + ErrorStyle.Render(fmt.Sprintf("Errors: %d (showing first 10)", len(m.errors))) + "\n")
		for _, err := range m.errors[:10] {
			b.WriteString(fmt.Sprintf("  • %s\n", err))
		}
	}

	b.WriteString("\n")
	return b.String()
}

// Helper commands
func tickCmd() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(t time.Time) tea.Msg {
		return TickMsg(t)
	})
}

func waitForResult(ch <-chan worker.JobResult) tea.Cmd {
	return func() tea.Msg {
		result, ok := <-ch
		if !ok {
			return DoneMsg{}
		}
		return ResultMsg(result)
	}
}

func waitForWorkerStatus(ch <-chan worker.WorkerStatus) tea.Cmd {
	return func() tea.Msg {
		status, ok := <-ch
		if !ok {
			return nil
		}
		return WorkerStatusMsg(status)
	}
}
