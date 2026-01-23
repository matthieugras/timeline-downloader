package ui

import "github.com/charmbracelet/lipgloss"

var (
	// Colors
	primaryColor   = lipgloss.Color("205")
	successColor   = lipgloss.Color("42")
	errorColor     = lipgloss.Color("196")
	warningColor   = lipgloss.Color("214")
	mutedColor     = lipgloss.Color("240")
	highlightColor = lipgloss.Color("86")

	// Header style
	HeaderStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(primaryColor).
			Padding(0, 1).
			MarginBottom(1)

	// Title style
	TitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("255")).
			Background(primaryColor).
			Padding(0, 2)

	// Success style
	SuccessStyle = lipgloss.NewStyle().
			Foreground(successColor)

	// Error style
	ErrorStyle = lipgloss.NewStyle().
			Foreground(errorColor)

	// Warning style
	WarningStyle = lipgloss.NewStyle().
			Foreground(warningColor)

	// Muted style
	MutedStyle = lipgloss.NewStyle().
			Foreground(mutedColor)

	// Highlight style
	HighlightStyle = lipgloss.NewStyle().
			Foreground(highlightColor).
			Bold(true)

	// Box style for containers
	BoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(mutedColor).
			Padding(0, 1)

	// Status bar style
	StatusBarStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("255")).
			Background(lipgloss.Color("236")).
			Padding(0, 1)

	// Footer style
	FooterStyle = lipgloss.NewStyle().
			Foreground(mutedColor).
			MarginTop(1)

	// Worker idle style
	WorkerIdleStyle = lipgloss.NewStyle().
			Foreground(mutedColor)

	// Worker working style
	WorkerWorkingStyle = lipgloss.NewStyle().
				Foreground(highlightColor)

	// Worker backing off style
	WorkerBackoffStyle = lipgloss.NewStyle().
				Foreground(warningColor)

	// Worker merging style (orange)
	WorkerMergingStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("214"))

	// Skipped style (yellow/dimmed)
	SkippedStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("178"))

	// Progress bar colors
	ProgressGradientStart = "#5A56E0"
	ProgressGradientEnd   = "#EE6FF8"
)
