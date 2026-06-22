package agent

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// forgeBlockedArgs are flags hardcoded by the daemon that must not be
// overridden by user-configured custom_args.
var forgeBlockedArgs = map[string]blockedArgMode{
	"-p":                blockedWithValue,  // owned by the prompt arg
	"--prompt":          blockedWithValue,
	"-C":                blockedWithValue,  // task workdir anchor
	"--directory":       blockedWithValue,
	"--conversation-id": blockedWithValue,  // managed via ExecOptions.ResumeSessionID
	"--sandbox":         blockedWithValue,  // daemon manages the workdir; sandbox would create a nested worktree
}

// forgeBackend implements Backend by spawning `forge --prompt <prompt>` in
// one-shot mode. Forge outputs a mix of bullet-format event lines on stdout
// ("● [HH:MM:SS] EventType ...") and plain markdown text. There is no
// structured event-stream mode. The backend streams text output line-by-line
// as MessageText events and extracts the session ID from the Initialize event.
//
// Model selection uses the FORGE_MODEL_ID environment variable because forge
// has no --model CLI flag.
type forgeBackend struct {
	cfg Config
}

// forgeTitleLinePrefix is the prefix forge uses for event/status lines on stdout.
const forgeTitleLinePrefix = "● ["

func (b *forgeBackend) Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
	execPath := b.cfg.ExecutablePath
	if execPath == "" {
		execPath = "forge"
	}
	if _, err := exec.LookPath(execPath); err != nil {
		return nil, fmt.Errorf("forge executable not found at %q: %w", execPath, err)
	}

	timeout := opts.Timeout
	runCtx, cancel := runContext(ctx, timeout)

	args := []string{"--prompt", prompt}
	if opts.Cwd != "" {
		args = append(args, "-C", opts.Cwd)
	}
	if opts.ResumeSessionID != "" {
		args = append(args, "--conversation-id", opts.ResumeSessionID)
	}
	if opts.MaxTurns > 0 {
		b.cfg.Logger.Warn("forge does not support --max-turns; ignoring", "maxTurns", opts.MaxTurns)
	}
	if opts.ThinkingLevel != "" {
		b.cfg.Logger.Warn("forge does not support thinking levels; ignoring", "level", opts.ThinkingLevel)
	}
	args = append(args, filterCustomArgs(opts.CustomArgs, forgeBlockedArgs, b.cfg.Logger)...)

	cmd := exec.CommandContext(runCtx, execPath, args...)
	hideAgentWindow(cmd)
	b.cfg.Logger.Info("agent command", "exec", execPath, "args", args)
	cmd.WaitDelay = 10 * time.Second
	if opts.Cwd != "" {
		cmd.Dir = opts.Cwd
	}

	env := buildEnv(b.cfg.Env)
	// Inject FORGE_MODEL_ID when a model override is configured — forge has no
	// --model CLI flag; the environment variable is the only runtime selection point.
	if opts.Model != "" {
		env = append(env, "FORGE_MODEL_ID="+opts.Model)
	}
	cmd.Env = env

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("forge stdout pipe: %w", err)
	}
	cmd.Stderr = newLogWriter(b.cfg.Logger, "[forge:stderr] ")

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start forge: %w", err)
	}

	b.cfg.Logger.Info("forge started", "pid", cmd.Process.Pid, "cwd", opts.Cwd, "model", opts.Model)

	msgCh := make(chan Message, 256)
	resCh := make(chan Result, 1)

	go func() {
		<-runCtx.Done()
		_ = stdout.Close()
	}()

	go func() {
		defer cancel()
		defer close(msgCh)
		defer close(resCh)

		startTime := time.Now()
		scanResult := b.processOutput(stdout, msgCh)

		exitErr := cmd.Wait()
		duration := time.Since(startTime)

		if runCtx.Err() == context.DeadlineExceeded {
			scanResult.status = "timeout"
			scanResult.errMsg = fmt.Sprintf("forge timed out after %s", timeout)
		} else if runCtx.Err() == context.Canceled {
			scanResult.status = "aborted"
			scanResult.errMsg = "execution cancelled"
		} else if exitErr != nil && scanResult.status == "completed" {
			scanResult.status = "failed"
			scanResult.errMsg = fmt.Sprintf("forge exited with error: %v", exitErr)
		}

		b.cfg.Logger.Info("forge finished", "pid", cmd.Process.Pid, "status", scanResult.status, "duration", duration.Round(time.Millisecond).String())

		resCh <- Result{
			Status:     scanResult.status,
			Output:     scanResult.output,
			Error:      scanResult.errMsg,
			DurationMs: duration.Milliseconds(),
			SessionID:  scanResult.sessionID,
			// The Forge CLI doesn't surface per-turn token usage; leave Usage
			// empty rather than report misleading zeros under a guessed model name.
			Usage: map[string]TokenUsage{},
		}
	}()

	return &Session{Messages: msgCh, Result: resCh}, nil
}

// forgeScanResult holds the accumulated state from scanning forge's stdout.
type forgeScanResult struct {
	status    string
	errMsg    string
	output    string
	sessionID string
}

// processOutput reads forge's stdout line by line. Lines starting with "● ["
// are event lines: Initialize carries the session ID, other event lines emit
// a status ping. All other non-empty lines are plain text output from the model.
func (b *forgeBackend) processOutput(r io.Reader, ch chan<- Message) forgeScanResult {
	var output strings.Builder
	var sessionID string
	finalStatus := "completed"
	var finalError string

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)

	trySend(ch, Message{Type: MessageStatus, Status: "running"})

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, forgeTitleLinePrefix) {
			b.handleEventLine(line, ch, &sessionID, &finalStatus, &finalError)
			continue
		}
		// Plain text output from the model.
		if strings.TrimSpace(line) != "" {
			if output.Len() > 0 {
				output.WriteByte('\n')
			}
			output.WriteString(line)
			trySend(ch, Message{Type: MessageText, Content: line})
		} else if output.Len() > 0 {
			// Preserve blank lines within output but not leading blank lines.
			output.WriteByte('\n')
		}
	}

	if scanErr := scanner.Err(); scanErr != nil {
		b.cfg.Logger.Warn("forge stdout scanner error", "error", scanErr)
		if finalStatus == "completed" {
			finalStatus = "failed"
			finalError = fmt.Sprintf("stdout read error: %v", scanErr)
		}
	}

	return forgeScanResult{
		status:    finalStatus,
		errMsg:    finalError,
		output:    strings.TrimRight(output.String(), "\n"),
		sessionID: sessionID,
	}
}

// handleEventLine processes a single forge event line of the form
// "● [HH:MM:SS] EventType [data...]". It extracts the session ID from
// Initialize events and emits a running status ping for all other events.
func (b *forgeBackend) handleEventLine(line string, ch chan<- Message, sessionID *string, finalStatus, finalError *string) {
	// Strip the "● [" prefix and find the closing bracket of the timestamp.
	rest := strings.TrimPrefix(line, "● [")
	closeBracket := strings.Index(rest, "]")
	if closeBracket < 0 {
		trySend(ch, Message{Type: MessageStatus, Status: "running"})
		return
	}
	// afterTS is "EventType rest..." after stripping the "[HH:MM:SS] " prefix.
	afterTS := strings.TrimSpace(rest[closeBracket+1:])
	spaceIdx := strings.Index(afterTS, " ")
	var eventType, data string
	if spaceIdx < 0 {
		eventType = afterTS
	} else {
		eventType = afterTS[:spaceIdx]
		data = strings.TrimSpace(afterTS[spaceIdx+1:])
	}

	switch eventType {
	case "Initialize":
		// "● [HH:MM:SS] Initialize <conversation-uuid>"
		if data != "" && *sessionID == "" {
			*sessionID = data
			trySend(ch, Message{Type: MessageStatus, Status: "running", SessionID: data})
		}
	default:
		_ = finalStatus
		_ = finalError
		_ = data
		trySend(ch, Message{Type: MessageStatus, Status: "running"})
	}
}
