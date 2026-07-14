// Package progress renders invocation-local, observational workflow progress.
// It never returns rendering errors to callers, and its update methods only
// enqueue bounded state so Kubernetes polling is not blocked by output.
package progress

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

// Phase is a display-only workflow phase. These values are intentionally
// separate from internal/operation recovery checkpoints.
type Phase string

const (
	LockAcquiring             Phase = "lock/acquiring"
	LockWaiting               Phase = "lock/waiting"
	QuiesceScalingConsumers   Phase = "quiesce/scaling-consumers"
	QuiesceWaitingForUnmount  Phase = "quiesce/waiting-for-unmount"
	InspectScheduling         Phase = "inspect/scheduling"
	InspectMounting           Phase = "inspect/mounting"
	InspectScanning           Phase = "inspect/scanning"
	PrepareTempCreating       Phase = "prepare-temp/creating"
	PrepareTempProvisioning   Phase = "prepare-temp/provisioning"
	PrepareTempMounting       Phase = "prepare-temp/mounting"
	CopyToTempScheduling      Phase = "copy-to-temp/scheduling"
	CopyToTempMounting        Phase = "copy-to-temp/mounting"
	CopyToTempTransferring    Phase = "copy-to-temp/transferring"
	VerifyTempScheduling      Phase = "verify-temp/scheduling"
	VerifyTempChecksumming    Phase = "verify-temp/checksumming"
	VerifyTempCompleted       Phase = "verify-temp/completed"
	ReplaceSourceDeleting     Phase = "replace-source/deleting"
	ReplaceSourceCreating     Phase = "replace-source/creating"
	ReplaceSourceProvisioning Phase = "replace-source/provisioning"
	ReplaceSourceMounting     Phase = "replace-source/mounting"
	CopyBackScheduling        Phase = "copy-back/scheduling"
	CopyBackMounting          Phase = "copy-back/mounting"
	CopyBackTransferring      Phase = "copy-back/transferring"
	VerifySourceScheduling    Phase = "verify-source/scheduling"
	VerifySourceChecksumming  Phase = "verify-source/checksumming"
	VerifySourceCompleted     Phase = "verify-source/completed"
	RestoreControllers        Phase = "restore/controllers"
	RestoreObservingConsumers Phase = "restore/observing-consumers"
	CleanupJobsPods           Phase = "cleanup/jobs-pods"
	CleanupTempPVC            Phase = "cleanup/temp-pvc"
	CleanupCheckpoint         Phase = "cleanup/checkpoint"
	CleanupLease              Phase = "cleanup/lease"
)

const (
	defaultHeartbeatInterval = 2 * time.Second
	defaultActivityInterval  = 500 * time.Millisecond
	defaultTTYWidth          = 120
	maxNonTTYLineRunes       = 512
	maxActivityRunes         = 320
	maxQueuedSnapshots       = 128
)

type snapshot struct {
	phase        Phase
	totalElapsed time.Duration
	phaseElapsed time.Duration
	activity     string
}

type renderCommand struct {
	writer           io.Writer
	text             string
	onProgressWriter bool
	close            bool
	flush            bool
	done             chan struct{}
}

// Reporter owns progress rendering and serializes it with durable output.
// Phase and activity methods are non-blocking with respect to the output
// writer; only Output and Close wait for the renderer so durable text cannot
// be corrupted by a redraw.
type Reporter struct {
	out               io.Writer
	quiet             bool
	tty               bool
	width             int
	now               func() time.Time
	heartbeatInterval time.Duration
	activityInterval  time.Duration

	mu             sync.Mutex
	commandMu      sync.Mutex
	startedAt      time.Time
	phaseStartedAt time.Time
	phase          Phase
	activity       string
	lastHighQueued time.Time
	pendingHigh    bool
	queue          []snapshot
	closed         bool

	signal   chan struct{}
	commands chan renderCommand
	done     chan struct{}
}

type options struct {
	out               io.Writer
	quiet             bool
	tty               bool
	width             int
	now               func() time.Time
	heartbeatInterval time.Duration
	activityInterval  time.Duration
}

// New creates a reporter with a fresh invocation timer.
func New(out io.Writer, quiet bool) *Reporter {
	tty, width := terminalInfo(out)
	return newReporter(options{out: out, quiet: quiet, tty: tty, width: width})
}

func newReporter(opts options) *Reporter {
	if opts.out == nil {
		opts.out = io.Discard
	}
	if opts.width <= 0 {
		opts.width = defaultTTYWidth
	}
	if opts.now == nil {
		opts.now = time.Now
	}
	if opts.heartbeatInterval <= 0 {
		opts.heartbeatInterval = defaultHeartbeatInterval
	}
	if opts.activityInterval <= 0 {
		opts.activityInterval = defaultActivityInterval
	}
	now := opts.now()
	r := &Reporter{
		out: opts.out, quiet: opts.quiet, tty: opts.tty, width: opts.width,
		now: opts.now, heartbeatInterval: opts.heartbeatInterval, activityInterval: opts.activityInterval,
		startedAt: now, phaseStartedAt: now,
		signal: make(chan struct{}, 1), commands: make(chan renderCommand), done: make(chan struct{}),
	}
	go r.renderLoop()
	return r
}

// SetPhase publishes a phase change immediately. Repeating a phase does not
// reset phase elapsed time; changed activity is still published promptly.
func (r *Reporter) SetPhase(phase Phase, activity string) {
	now := r.now()
	activity = sanitizeActivity(activity)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	changedPhase := phase != r.phase
	changedActivity := activity != r.activity
	if !changedPhase && !changedActivity {
		return
	}
	if r.pendingHigh {
		r.enqueueLocked(r.snapshotLocked(now))
		r.pendingHigh = false
	}
	if changedPhase {
		r.phaseStartedAt = now
		r.lastHighQueued = time.Time{}
		r.pendingHigh = false
	}
	r.phase = phase
	r.activity = activity
	r.enqueueLocked(r.snapshotLocked(now))
	r.notifyLocked()
}

// Activity publishes a changed lifecycle observation immediately. Repeated
// observations are emitted by the low-rate heartbeat instead.
func (r *Reporter) Activity(activity string) {
	now := r.now()
	activity = sanitizeActivity(activity)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.phase == "" || activity == r.activity {
		return
	}
	if r.pendingHigh {
		r.enqueueLocked(r.snapshotLocked(now))
		r.pendingHigh = false
	}
	r.activity = activity
	r.enqueueLocked(r.snapshotLocked(now))
	r.notifyLocked()
}

// HighFrequencyActivity coalesces bursty activity such as rsync log records.
func (r *Reporter) HighFrequencyActivity(activity string) {
	now := r.now()
	activity = sanitizeActivity(activity)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.phase == "" {
		return
	}
	if activity == r.activity && r.lastHighQueued.IsZero() {
		r.lastHighQueued = now
		return
	}
	r.activity = activity
	if r.lastHighQueued.IsZero() || now.Sub(r.lastHighQueued) >= r.activityInterval {
		r.enqueueLocked(r.snapshotLocked(now))
		r.lastHighQueued = now
		r.pendingHigh = false
		r.notifyLocked()
		return
	}
	r.pendingHigh = true
}

// FinalActivity preserves the final complete record in a burst before the
// workflow advances to another phase.
func (r *Reporter) FinalActivity(activity string) {
	now := r.now()
	activity = sanitizeActivity(activity)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.phase == "" {
		return
	}
	r.activity = activity
	r.enqueueLocked(r.snapshotLocked(now))
	r.lastHighQueued = now
	r.pendingHigh = false
	r.notifyLocked()
}

// Output writes durable text after ending any active TTY redraw line. The
// supplied text is not reformatted, so existing result and warning contracts
// remain unchanged.
func (r *Reporter) Output(writer io.Writer, text string) {
	if writer == nil || text == "" {
		return
	}
	r.commandMu.Lock()
	defer r.commandMu.Unlock()
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		_, _ = io.WriteString(writer, text)
		return
	}
	done := make(chan struct{})
	r.commands <- renderCommand{writer: writer, text: text, onProgressWriter: sameWriter(writer, r.out), done: done}
	<-done
}

// Flush renders the newest coalesced activity. It is intended for deterministic
// tests and durable-output boundaries, not Kubernetes polling callbacks.
func (r *Reporter) Flush() {
	r.commandMu.Lock()
	defer r.commandMu.Unlock()
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return
	}
	done := make(chan struct{})
	r.commands <- renderCommand{flush: true, done: done}
	<-done
}

// Close drains bounded progress output and terminates an active TTY line.
func (r *Reporter) Close() {
	r.commandMu.Lock()
	defer r.commandMu.Unlock()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		<-r.done
		return
	}
	r.closed = true
	r.mu.Unlock()
	done := make(chan struct{})
	r.commands <- renderCommand{close: true, flush: true, done: done}
	<-done
	<-r.done
}

func (r *Reporter) renderLoop() {
	heartbeat := time.NewTicker(r.heartbeatInterval)
	defer heartbeat.Stop()
	highTick := time.NewTicker(r.activityInterval)
	defer highTick.Stop()

	activeTTYLine := false
	activeNonTTYLine := false
	renderDisabled := false

	breakTTYLine := func() {
		if !r.tty || !activeTTYLine || renderDisabled {
			return
		}
		if _, err := io.WriteString(r.out, "\n"); err != nil {
			renderDisabled = true
		}
		activeTTYLine = false
	}
	breakNonTTYLine := func() bool {
		if r.tty || !activeNonTTYLine {
			return true
		}
		activeNonTTYLine = false
		_, err := io.WriteString(r.out, "\n")
		return err == nil
	}
	render := func(s snapshot) {
		if r.quiet || renderDisabled || s.phase == "" {
			return
		}
		if !breakNonTTYLine() {
			renderDisabled = true
			return
		}
		limit := maxNonTTYLineRunes
		if r.tty {
			width := r.width
			if detectedTTY, currentWidth := terminalInfo(r.out); detectedTTY && currentWidth > 0 {
				width = currentWidth
			}
			limit = width - 1
			if limit < 1 {
				limit = 1
			}
		}
		line := truncateRunes(formatSnapshot(s), limit)
		if !r.tty {
			if _, err := io.WriteString(r.out, line+"\n"); err != nil {
				renderDisabled = true
			}
			return
		}
		line = formatTTYSnapshot(s, limit)
		if _, err := io.WriteString(r.out, "\r\x1b[2K"+line); err != nil {
			renderDisabled = true
			activeTTYLine = false
			return
		}
		activeTTYLine = true
	}
	drain := func() {
		for {
			r.mu.Lock()
			if len(r.queue) == 0 {
				r.mu.Unlock()
				return
			}
			queued := append([]snapshot(nil), r.queue...)
			r.queue = r.queue[:0]
			r.mu.Unlock()
			for _, s := range queued {
				render(s)
			}
		}
	}
	materializeHigh := func(force bool) {
		now := r.now()
		r.mu.Lock()
		if r.pendingHigh && (force || r.lastHighQueued.IsZero() || now.Sub(r.lastHighQueued) >= r.activityInterval) {
			r.enqueueLocked(r.snapshotLocked(now))
			r.lastHighQueued = now
			r.pendingHigh = false
		}
		r.mu.Unlock()
	}
	queueHeartbeat := func() {
		now := r.now()
		r.mu.Lock()
		if !r.closed && !r.quiet && r.phase != "" {
			r.enqueueLocked(r.snapshotLocked(now))
		}
		r.mu.Unlock()
	}

	for {
		select {
		case <-r.signal:
			drain()
		case <-highTick.C:
			materializeHigh(false)
			drain()
		case <-heartbeat.C:
			materializeHigh(false)
			queueHeartbeat()
			drain()
		case cmd := <-r.commands:
			if cmd.flush {
				materializeHigh(true)
			}
			drain()
			if cmd.writer != nil && cmd.text != "" {
				breakTTYLine()
				if cmd.onProgressWriter && !r.tty && activeNonTTYLine && !strings.HasPrefix(cmd.text, "\n") {
					_ = breakNonTTYLine()
				}
				_, _ = io.WriteString(cmd.writer, cmd.text)
				if cmd.onProgressWriter && !r.tty {
					activeNonTTYLine = !strings.HasSuffix(cmd.text, "\n")
				}
			}
			if cmd.close {
				breakTTYLine()
				close(cmd.done)
				close(r.done)
				return
			}
			close(cmd.done)
		}
	}
}

func (r *Reporter) snapshotLocked(now time.Time) snapshot {
	total := now.Sub(r.startedAt)
	phaseElapsed := now.Sub(r.phaseStartedAt)
	if total < 0 {
		total = 0
	}
	if phaseElapsed < 0 {
		phaseElapsed = 0
	}
	return snapshot{phase: r.phase, totalElapsed: total, phaseElapsed: phaseElapsed, activity: r.activity}
}

func (r *Reporter) enqueueLocked(s snapshot) {
	if r.quiet || s.phase == "" {
		return
	}
	if len(r.queue) == maxQueuedSnapshots {
		copy(r.queue, r.queue[1:])
		r.queue = r.queue[:maxQueuedSnapshots-1]
	}
	r.queue = append(r.queue, s)
}

func (r *Reporter) notifyLocked() {
	select {
	case r.signal <- struct{}{}:
	default:
	}
}

func formatSnapshot(s snapshot) string {
	return fmt.Sprintf("[progress] phase=%s total=%s phase-elapsed=%s activity=%s", s.phase, formatElapsed(s.totalElapsed), formatElapsed(s.phaseElapsed), sanitizeActivity(s.activity))
}

func formatTTYSnapshot(s snapshot, limit int) string {
	activity := sanitizeActivity(s.activity)
	prefix := fmt.Sprintf("[progress] phase=%s total=%s phase-elapsed=%s activity=", s.phase, formatElapsed(s.totalElapsed), formatElapsed(s.phaseElapsed))
	if utf8.RuneCountInString(prefix)+1 > limit {
		prefix = fmt.Sprintf("%s total=%s phase=%s activity=", s.phase, formatElapsed(s.totalElapsed), formatElapsed(s.phaseElapsed))
	}
	prefixWidth := utf8.RuneCountInString(prefix)
	if prefixWidth >= limit {
		return truncateRunes(prefix+"…", limit)
	}
	return prefix + truncateRunes(activity, limit-prefixWidth)
}

func formatElapsed(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	return d.Truncate(time.Second).String()
}

func sanitizeActivity(value string) string {
	var sanitized strings.Builder
	for _, r := range value {
		switch {
		case r == '\t':
			sanitized.WriteByte(' ')
		case r == utf8.RuneError:
			sanitized.WriteByte('?')
		case unicode.IsControl(r):
			continue
		case r >= ' ' && r <= '~':
			sanitized.WriteRune(r)
		case r <= 0xffff:
			fmt.Fprintf(&sanitized, "\\u%04X", r)
		default:
			fmt.Fprintf(&sanitized, "\\U%08X", r)
		}
	}
	value = strings.Join(strings.Fields(sanitized.String()), " ")
	if value == "" {
		value = "waiting for Kubernetes activity"
	}
	return truncateRunes(value, maxActivityRunes)
}

func truncateRunes(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if utf8.RuneCountInString(value) <= limit {
		return value
	}
	if limit == 1 {
		return "…"
	}
	runes := []rune(value)
	return string(runes[:limit-1]) + "…"
}

func sameWriter(left, right io.Writer) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)
	if leftValue.Type() != rightValue.Type() || !leftValue.Comparable() {
		return false
	}
	return leftValue.Interface() == rightValue.Interface()
}

func terminalInfo(writer io.Writer) (bool, int) {
	fdWriter, ok := writer.(interface{ Fd() uintptr })
	if !ok {
		return false, 0
	}
	width, terminal := terminalWidth(fdWriter.Fd())
	if !terminal {
		return false, 0
	}
	if width <= 0 {
		width = defaultTTYWidth
	}
	return true, width
}
