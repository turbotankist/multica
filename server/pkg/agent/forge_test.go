package agent

import (
	"log/slog"
	"strings"
	"testing"
)

func TestNewReturnsForgeBackend(t *testing.T) {
	t.Parallel()
	b, err := New("forge", Config{ExecutablePath: "/nonexistent/forge"})
	if err != nil {
		t.Fatalf("New(forge) error: %v", err)
	}
	if _, ok := b.(*forgeBackend); !ok {
		t.Fatalf("expected *forgeBackend, got %T", b)
	}
}

// ── processOutput tests ──

func TestForgeProcessOutputTextLines(t *testing.T) {
	t.Parallel()

	b := &forgeBackend{cfg: Config{Logger: slog.Default()}}
	ch := make(chan Message, 32)

	input := "Hello from forge\nSecond line"
	result := b.processOutput(strings.NewReader(input), ch)
	close(ch)

	if result.status != "completed" {
		t.Errorf("status: got %q, want completed", result.status)
	}
	if result.output != "Hello from forge\nSecond line" {
		t.Errorf("output: got %q, want %q", result.output, "Hello from forge\nSecond line")
	}

	var msgs []Message
	for m := range ch {
		msgs = append(msgs, m)
	}
	textMsgs := 0
	for _, m := range msgs {
		if m.Type == MessageText {
			textMsgs++
		}
	}
	if textMsgs != 2 {
		t.Errorf("expected 2 MessageText messages, got %d", textMsgs)
	}
}

func TestForgeProcessOutputEventLineInitialize(t *testing.T) {
	t.Parallel()

	b := &forgeBackend{cfg: Config{Logger: slog.Default()}}
	ch := make(chan Message, 32)

	input := "● [15:09:21] Initialize abc-def-123\nModel response here"
	result := b.processOutput(strings.NewReader(input), ch)
	close(ch)

	if result.sessionID != "abc-def-123" {
		t.Errorf("sessionID: got %q, want %q", result.sessionID, "abc-def-123")
	}
	if result.status != "completed" {
		t.Errorf("status: got %q, want completed", result.status)
	}
	if result.output != "Model response here" {
		t.Errorf("output: got %q, want %q", result.output, "Model response here")
	}
}

func TestForgeProcessOutputOnlyEventLines(t *testing.T) {
	t.Parallel()

	b := &forgeBackend{cfg: Config{Logger: slog.Default()}}
	ch := make(chan Message, 32)

	input := "● [15:09:21] Initialize session-id-456\n● [15:09:22] Summary done"
	result := b.processOutput(strings.NewReader(input), ch)
	close(ch)

	if result.sessionID != "session-id-456" {
		t.Errorf("sessionID: got %q, want %q", result.sessionID, "session-id-456")
	}
	if result.output != "" {
		t.Errorf("expected empty output, got %q", result.output)
	}
	if result.status != "completed" {
		t.Errorf("status: got %q, want completed", result.status)
	}
}

// ── handleEventLine tests ──

func TestForgeHandleEventLineInitialize(t *testing.T) {
	t.Parallel()

	b := &forgeBackend{cfg: Config{Logger: slog.Default()}}
	ch := make(chan Message, 10)

	var sessionID, finalStatus, finalError string
	finalStatus = "completed"
	b.handleEventLine("● [15:09:21] Initialize my-session-uuid", ch, &sessionID, &finalStatus, &finalError)

	if sessionID != "my-session-uuid" {
		t.Errorf("sessionID: got %q, want %q", sessionID, "my-session-uuid")
	}
	if finalStatus != "completed" {
		t.Errorf("status should remain completed, got %q", finalStatus)
	}

	msg := <-ch
	if msg.Type != MessageStatus {
		t.Errorf("msg type: got %v, want MessageStatus", msg.Type)
	}
	if msg.SessionID != "my-session-uuid" {
		t.Errorf("msg.SessionID: got %q, want %q", msg.SessionID, "my-session-uuid")
	}
}

func TestForgeHandleEventLineUnknown(t *testing.T) {
	t.Parallel()

	b := &forgeBackend{cfg: Config{Logger: slog.Default()}}
	ch := make(chan Message, 10)

	var sessionID, finalStatus, finalError string
	finalStatus = "completed"
	b.handleEventLine("● [15:09:22] Summary all done", ch, &sessionID, &finalStatus, &finalError)

	if sessionID != "" {
		t.Errorf("sessionID should be empty, got %q", sessionID)
	}
	if finalStatus != "completed" {
		t.Errorf("status should remain completed, got %q", finalStatus)
	}

	msg := <-ch
	if msg.Type != MessageStatus {
		t.Errorf("msg type: got %v, want MessageStatus", msg.Type)
	}
}

func TestForgeHandleEventLineMalformed(t *testing.T) {
	t.Parallel()

	b := &forgeBackend{cfg: Config{Logger: slog.Default()}}
	ch := make(chan Message, 10)

	var sessionID, finalStatus, finalError string
	finalStatus = "completed"
	// No closing bracket — should not panic and should still send a status ping.
	b.handleEventLine("● [no closing bracket here", ch, &sessionID, &finalStatus, &finalError)

	msg := <-ch
	if msg.Type != MessageStatus {
		t.Errorf("msg type: got %v, want MessageStatus", msg.Type)
	}
}

// ── parseForgeModels tests ──

func TestParseForgeModels(t *testing.T) {
	t.Parallel()

	input := `ID                          MODEL              PROVIDER    PROVIDER ID  CONTEXT WINDOW  TOOL SUPPORTED  IMAGE
claude-3-5-haiku-20241022   Claude Haiku 3.5   ClaudeCode  claude_code  200k            [yes]           [yes]
claude-fable-5              Claude Fable 5     ClaudeCode  claude_code  1M              [yes]           [yes]
gpt-4o                      GPT-4o             OpenAI      openai       128k            [yes]           [yes]
`
	models := parseForgeModels(input)
	if len(models) != 3 {
		t.Fatalf("expected 3 models, got %d", len(models))
	}

	tests := []struct {
		id       string
		label    string
		provider string
	}{
		{"claude-3-5-haiku-20241022", "Claude Haiku 3.5", "claude_code"},
		{"claude-fable-5", "Claude Fable 5", "claude_code"},
		{"gpt-4o", "GPT-4o", "openai"},
	}
	for i, tc := range tests {
		m := models[i]
		if m.ID != tc.id {
			t.Errorf("[%d] ID: got %q, want %q", i, m.ID, tc.id)
		}
		if m.Label != tc.label {
			t.Errorf("[%d] Label: got %q, want %q", i, m.Label, tc.label)
		}
		if m.Provider != tc.provider {
			t.Errorf("[%d] Provider: got %q, want %q", i, m.Provider, tc.provider)
		}
	}
}

func TestParseForgeModelsEmpty(t *testing.T) {
	t.Parallel()
	models := parseForgeModels("")
	if len(models) != 0 {
		t.Errorf("expected empty slice, got %d models", len(models))
	}
}

func TestParseForgeModelsHeaderOnly(t *testing.T) {
	t.Parallel()
	input := "ID                          MODEL              PROVIDER    PROVIDER ID  CONTEXT WINDOW  TOOL SUPPORTED  IMAGE\n"
	models := parseForgeModels(input)
	if len(models) != 0 {
		t.Errorf("expected empty slice for header-only input, got %d models", len(models))
	}
}
