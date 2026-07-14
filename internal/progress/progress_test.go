package progress

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

func TestTerminalInfoRejectsNonTerminalWriters(t *testing.T) {
	if terminal, width := terminalInfo(&bytes.Buffer{}); terminal || width != 0 {
		t.Fatalf("buffer terminal info = (%t, %d), want (false, 0)", terminal, width)
	}

	file, err := os.CreateTemp(t.TempDir(), "progress-output")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if terminal, width := terminalInfo(file); terminal || width != 0 {
		t.Fatalf("regular file terminal info = (%t, %d), want (false, 0)", terminal, width)
	}
}

func TestNonTTYRenderingIncludesMonotonicElapsedAndResetsPhaseTimer(t *testing.T) {
	var out bytes.Buffer
	base := time.Now()
	now := base
	reporter := newReporter(options{
		out: &out, now: func() time.Time { return now },
		heartbeatInterval: time.Hour, activityInterval: time.Second,
	})

	reporter.SetPhase(LockAcquiring, "creating Lease")
	reporter.Flush()
	now = base.Add(5 * time.Second)
	reporter.Activity("observed Lease")
	reporter.Flush()
	now = base.Add(8 * time.Second)
	reporter.SetPhase(LockWaiting, "held by another invocation")
	reporter.Close()

	got := out.String()
	for _, want := range []string{
		"[progress] phase=lock/acquiring total=0s phase-elapsed=0s activity=creating Lease",
		"[progress] phase=lock/acquiring total=5s phase-elapsed=5s activity=observed Lease",
		"[progress] phase=lock/waiting total=8s phase-elapsed=0s activity=held by another invocation",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q:\n%s", want, got)
		}
	}
	if strings.ContainsAny(got, "\r\x1b") {
		t.Fatalf("non-TTY output contains terminal redraw controls: %q", got)
	}
	for _, line := range strings.Split(strings.TrimSuffix(got, "\n"), "\n") {
		if utf8.RuneCountInString(line) > maxNonTTYLineRunes {
			t.Fatalf("non-TTY snapshot has %d runes, limit %d", utf8.RuneCountInString(line), maxNonTTYLineRunes)
		}
	}
}

func TestNonTTYProgressDoesNotJoinUnterminatedDurableOutput(t *testing.T) {
	var out bytes.Buffer
	reporter := newReporter(options{
		out: &out, heartbeatInterval: time.Hour, activityInterval: time.Hour,
	})
	reporter.Output(&out, "Continue? Type 'yes' to proceed: ")
	reporter.SetPhase(LockAcquiring, "creating Lease")
	reporter.Close()

	got := out.String()
	if !strings.Contains(got, "Continue? Type 'yes' to proceed: \n[progress] phase=lock/acquiring") {
		t.Fatalf("progress was not separated from unterminated prompt: %q", got)
	}
	if strings.ContainsAny(got, "\r\x1b") {
		t.Fatalf("non-TTY output contains terminal redraw controls: %q", got)
	}
}

func TestNonTTYDurableOutputDoesNotJoinUnterminatedPromptWhenQuiet(t *testing.T) {
	var out bytes.Buffer
	reporter := newReporter(options{out: &out, quiet: true})
	reporter.Output(&out, "Continue? Type 'yes' to proceed: ")
	reporter.Output(&out, "Source usage: 100 bytes.\n")
	reporter.Close()

	want := "Continue? Type 'yes' to proceed: \nSource usage: 100 bytes.\n"
	if got := out.String(); got != want {
		t.Fatalf("quiet durable output = %q, want %q", got, want)
	}
}

func TestTTYRenderingRedrawsOneLineAndProtectsDurableOutput(t *testing.T) {
	var out bytes.Buffer
	reporter := newReporter(options{
		out: &out, tty: true, width: 160,
		heartbeatInterval: time.Hour, activityInterval: time.Hour,
	})
	reporter.SetPhase(InspectScheduling, "inspection Pod pending")
	reporter.Flush()
	reporter.SetPhase(InspectScanning, "inspection Pod running")
	reporter.Flush()
	reporter.Output(&out, "DURABLE RESULT\n")
	reporter.SetPhase(CleanupJobsPods, "inspection Pod removed")
	reporter.Close()

	got := out.String()
	if !strings.Contains(got, "\r") {
		t.Fatalf("TTY output did not redraw with carriage returns: %q", got)
	}
	if !strings.Contains(got, "\x1b[2K") {
		t.Fatalf("TTY renderer did not safely clear the redraw line: %q", got)
	}
	if !strings.Contains(got, "\nDURABLE RESULT\n") {
		t.Fatalf("durable result was not isolated from redraw line: %q", got)
	}
	if !strings.HasSuffix(got, "\n") {
		t.Fatalf("TTY progress line was not terminated on close: %q", got)
	}
	for _, r := range got {
		if r < ' ' && r != '\r' && r != '\n' && r != '\x1b' {
			t.Fatalf("unexpected control character %q in TTY output", r)
		}
	}
}

func TestHeartbeatRepeatsUnchangedSnapshotAtLowRate(t *testing.T) {
	var out bytes.Buffer
	reporter := newReporter(options{
		out: &out, heartbeatInterval: 10 * time.Millisecond,
		activityInterval: time.Hour,
	})
	reporter.SetPhase(QuiesceWaitingForUnmount, "one Pod remains")
	time.Sleep(60 * time.Millisecond)
	reporter.Close()

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected initial snapshot and heartbeats, got %d lines: %q", len(lines), out.String())
	}
	for _, line := range lines {
		if !strings.Contains(line, "quiesce/waiting-for-unmount") || !strings.Contains(line, "one Pod remains") {
			t.Fatalf("heartbeat lost current phase/activity: %q", line)
		}
	}
}

func TestHighFrequencyActivityCoalescesAndPreservesLatestRecord(t *testing.T) {
	var out bytes.Buffer
	base := time.Now()
	now := base
	reporter := newReporter(options{
		out: &out, now: func() time.Time { return now },
		heartbeatInterval: time.Hour, activityInterval: time.Second,
	})
	reporter.SetPhase(CopyToTempTransferring, "copy started")
	reporter.Flush()
	now = base.Add(100 * time.Millisecond)
	reporter.HighFrequencyActivity("per-copy progress=10%")
	reporter.Flush()
	now = base.Add(200 * time.Millisecond)
	reporter.HighFrequencyActivity("per-copy progress=20%")
	now = base.Add(300 * time.Millisecond)
	reporter.HighFrequencyActivity("per-copy progress=30%")
	reporter.SetPhase(VerifyTempScheduling, "verification queued")
	reporter.Close()

	got := out.String()
	if strings.Contains(got, "progress=20%") {
		t.Fatalf("intermediate burst record was not coalesced: %s", got)
	}
	if !strings.Contains(got, "progress=10%") || !strings.Contains(got, "progress=30%") {
		t.Fatalf("first or latest burst record missing: %s", got)
	}
	if strings.Index(got, "progress=30%") > strings.Index(got, "verify-temp/scheduling") {
		t.Fatalf("latest copy record rendered after the next phase: %s", got)
	}
}

func TestRenderingSanitizesAndBoundsHostileActivity(t *testing.T) {
	var out bytes.Buffer
	reporter := newReporter(options{out: &out, heartbeatInterval: time.Hour, activityInterval: time.Hour})
	reporter.SetPhase(CopyBackTransferring, "name\x1b[2J\r\n"+strings.Repeat("x", 2000))
	reporter.Close()

	got := out.String()
	if strings.ContainsAny(got, "\r\x1b") {
		t.Fatalf("rendered activity retained terminal controls: %q", got)
	}
	line := strings.TrimSuffix(got, "\n")
	if utf8.RuneCountInString(line) > maxNonTTYLineRunes {
		t.Fatalf("rendered snapshot exceeds bound: %d", utf8.RuneCountInString(line))
	}
}

func TestQuietSuppressesOnlyProgress(t *testing.T) {
	var out bytes.Buffer
	reporter := newReporter(options{out: &out, quiet: true})
	reporter.SetPhase(LockAcquiring, "acquiring Lease")
	reporter.Activity("Lease acquired")
	reporter.HighFrequencyActivity("per-copy progress=50%")
	reporter.Output(&out, "PVC shrink plan\n")
	reporter.Output(&out, "PVC shrink workflow completed successfully.\n")
	reporter.Close()

	want := "PVC shrink plan\nPVC shrink workflow completed successfully.\n"
	if got := out.String(); got != want {
		t.Fatalf("quiet output = %q, want %q", got, want)
	}
}

func TestTTYEscapesWideAndCombiningActivityWithinDisplayWidth(t *testing.T) {
	var out bytes.Buffer
	reporter := newReporter(options{out: &out, tty: true, width: 100, heartbeatInterval: time.Hour})
	reporter.SetPhase(CopyBackTransferring, "文件-e\u0301-🙂")
	reporter.Close()

	got := out.String()
	if strings.Contains(got, "文件") || strings.Contains(got, "🙂") || !strings.Contains(got, "\\u") {
		t.Fatalf("TTY activity was not escaped to single-column ASCII: %q", got)
	}
	for _, segment := range strings.Split(got, "\r") {
		segment = strings.TrimPrefix(segment, "\x1b[2K")
		segment = strings.TrimSuffix(segment, "\n")
		if utf8.RuneCountInString(segment) > 99 {
			t.Fatalf("TTY segment exceeds configured width: %d runes", utf8.RuneCountInString(segment))
		}
	}
}

func TestTTYStandardWidthRetainsAllSnapshotFields(t *testing.T) {
	var out bytes.Buffer
	reporter := newReporter(options{out: &out, tty: true, width: 80, heartbeatInterval: time.Hour})
	reporter.SetPhase(QuiesceWaitingForUnmount, "one Pod remains")
	reporter.Close()

	got := out.String()
	for _, required := range []string{"quiesce/waiting-for-unmount", "total=", "phase=", "activity="} {
		if !strings.Contains(got, required) {
			t.Fatalf("80-column TTY snapshot omitted %q: %q", required, got)
		}
	}
}

func TestTTYExtremeNarrowWidthRemainsBounded(t *testing.T) {
	var out bytes.Buffer
	reporter := newReporter(options{out: &out, tty: true, width: 1, heartbeatInterval: time.Hour})
	reporter.SetPhase(QuiesceWaitingForUnmount, "one Pod remains")
	reporter.Close()

	for _, segment := range strings.Split(out.String(), "\r") {
		segment = strings.TrimPrefix(segment, "\x1b[2K")
		segment = strings.TrimSuffix(segment, "\n")
		if utf8.RuneCountInString(segment) > 1 {
			t.Fatalf("one-column TTY segment has %d columns: %q", utf8.RuneCountInString(segment), segment)
		}
	}
}

func TestConcurrentOutputFlushAndCloseDoNotDeadlock(t *testing.T) {
	for iteration := 0; iteration < 25; iteration++ {
		var out bytes.Buffer
		reporter := newReporter(options{out: &out, heartbeatInterval: time.Hour})
		reporter.SetPhase(LockAcquiring, "acquiring Lease")
		start := make(chan struct{})
		var calls sync.WaitGroup
		calls.Add(3)
		go func() {
			defer calls.Done()
			<-start
			reporter.Output(&out, "durable\n")
		}()
		go func() {
			defer calls.Done()
			<-start
			reporter.Flush()
		}()
		go func() {
			defer calls.Done()
			<-start
			reporter.Close()
		}()
		close(start)
		done := make(chan struct{})
		go func() {
			calls.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d deadlocked reporter lifecycle", iteration)
		}
		reporter.Close()
	}
}

func TestRenderingFailuresAndConcurrentActivityCannotFailCallers(t *testing.T) {
	reporter := newReporter(options{out: errorWriter{}, heartbeatInterval: time.Hour, activityInterval: time.Millisecond})
	reporter.SetPhase(CopyToTempTransferring, "copy started")
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for update := 0; update < 50; update++ {
				reporter.HighFrequencyActivity("copy update")
			}
		}()
	}
	workers.Wait()
	done := make(chan struct{})
	go func() {
		reporter.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("rendering error prevented reporter shutdown")
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("output unavailable") }

func TestEveryCanonicalPhaseConstantIsStable(t *testing.T) {
	phases := []Phase{
		LockAcquiring, LockWaiting, QuiesceScalingConsumers, QuiesceWaitingForUnmount,
		InspectScheduling, InspectMounting, InspectScanning,
		PrepareTempCreating, PrepareTempProvisioning, PrepareTempMounting,
		CopyToTempScheduling, CopyToTempMounting, CopyToTempTransferring,
		VerifyTempScheduling, VerifyTempChecksumming, VerifyTempCompleted,
		ReplaceSourceDeleting, ReplaceSourceCreating, ReplaceSourceProvisioning, ReplaceSourceMounting,
		CopyBackScheduling, CopyBackMounting, CopyBackTransferring,
		VerifySourceScheduling, VerifySourceChecksumming, VerifySourceCompleted,
		RestoreControllers, RestoreObservingConsumers,
		CleanupJobsPods, CleanupTempPVC, CleanupCheckpoint, CleanupLease,
	}
	want := []string{
		"lock/acquiring", "lock/waiting", "quiesce/scaling-consumers", "quiesce/waiting-for-unmount",
		"inspect/scheduling", "inspect/mounting", "inspect/scanning",
		"prepare-temp/creating", "prepare-temp/provisioning", "prepare-temp/mounting",
		"copy-to-temp/scheduling", "copy-to-temp/mounting", "copy-to-temp/transferring",
		"verify-temp/scheduling", "verify-temp/checksumming", "verify-temp/completed",
		"replace-source/deleting", "replace-source/creating", "replace-source/provisioning", "replace-source/mounting",
		"copy-back/scheduling", "copy-back/mounting", "copy-back/transferring",
		"verify-source/scheduling", "verify-source/checksumming", "verify-source/completed",
		"restore/controllers", "restore/observing-consumers",
		"cleanup/jobs-pods", "cleanup/temp-pvc", "cleanup/checkpoint", "cleanup/lease",
	}
	if len(phases) != len(want) {
		t.Fatalf("phase count = %d, want %d", len(phases), len(want))
	}
	for i := range phases {
		if string(phases[i]) != want[i] {
			t.Fatalf("phase[%d] = %q, want %q", i, phases[i], want[i])
		}
	}
}
