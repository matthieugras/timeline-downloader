package backoff

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/matthieugras/timeline-downloader/internal/config"
)

// GlobalBackoff coordinates exponential backoff across multiple workers
type GlobalBackoff struct {
	mu              sync.RWMutex
	backoffUntil    time.Time
	currentInterval time.Duration
	successStreak   int
	config          config.BackoffConfig

	// Callbacks for UI updates
	onBackoffStart func(duration time.Duration)
	onBackoffEnd   func()
}

// New creates a new global backoff coordinator
func New(cfg config.BackoffConfig) *GlobalBackoff {
	return &GlobalBackoff{
		currentInterval: cfg.InitialInterval,
		config:          cfg,
	}
}

// SetCallbacks sets optional callbacks for backoff state changes
func (g *GlobalBackoff) SetCallbacks(onStart func(time.Duration), onEnd func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.onBackoffStart = onStart
	g.onBackoffEnd = onEnd
}

// WaitIfNeeded blocks if global backoff is active
func (g *GlobalBackoff) WaitIfNeeded(ctx context.Context) error {
	g.mu.RLock()
	until := g.backoffUntil
	g.mu.RUnlock()

	if time.Now().Before(until) {
		waitDuration := time.Until(until)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitDuration):
			return nil
		}
	}
	return nil
}

// ReportError triggers the global backoff state.
// The caller is responsible for determining IF this error warrants a backoff.
// This keeps API-specific logic (like detecting "stealth" rate limits) in the client.
func (g *GlobalBackoff) ReportError() {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Reset success streak
	g.successStreak = 0

	// Calculate next interval with jitter
	jitter := g.config.RandomizationFactor * float64(g.currentInterval)
	randomJitter := time.Duration(rand.Float64() * jitter)
	backoffDuration := g.currentInterval + randomJitter

	g.backoffUntil = time.Now().Add(backoffDuration)

	// Notify callback
	if g.onBackoffStart != nil {
		g.onBackoffStart(backoffDuration)
	}

	// Increase interval for next time (exponential)
	g.currentInterval = min(time.Duration(float64(g.currentInterval)*g.config.Multiplier), g.config.MaxInterval)

	// Schedule callback for backoff end
	if g.onBackoffEnd != nil {
		go func(duration time.Duration, callback func()) {
			time.Sleep(duration)
			callback()
		}(backoffDuration, g.onBackoffEnd)
	}
}

// ReportSuccess reports a successful request
func (g *GlobalBackoff) ReportSuccess() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.successStreak++

	// Reset interval after a streak of successes
	if g.successStreak >= 3 {
		g.currentInterval = g.config.InitialInterval
	}
}

// IsBackingOff returns true if backoff is currently active
func (g *GlobalBackoff) IsBackingOff() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return time.Now().Before(g.backoffUntil)
}

// GetBackoffRemaining returns the remaining backoff duration
func (g *GlobalBackoff) GetBackoffRemaining() time.Duration {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if time.Now().Before(g.backoffUntil) {
		return time.Until(g.backoffUntil)
	}
	return 0
}

// GetCurrentInterval returns the current backoff interval
func (g *GlobalBackoff) GetCurrentInterval() time.Duration {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.currentInterval
}
