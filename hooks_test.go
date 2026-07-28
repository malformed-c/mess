package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// End-to-end tests for the shell hooks in hooks/. They drive the real scripts
// against a throwaway daemon, because the failures they cover live in the
// script's control flow (which empty return means "stand down") rather than in
// any Go function — the daemon and CLI can both be perfectly correct while the
// agent still ends up unwakeable.

// hookEnv builds a throwaway mess world: a freshly built binary, a private
// MESS_DIR/TMPDIR, and a running daemon that is stopped on cleanup.
type hookEnv struct {
	t    *testing.T
	bin  string
	dir  string
	tmp  string
	hook string // repo hooks/ directory
}

func newHookEnv(t *testing.T) *hookEnv {
	t.Helper()
	if testing.Short() {
		t.Skip("builds a binary and runs a daemon")
	}
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("hooks require jq")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "mess")
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building mess: %v\n%s", err, out)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	e := &hookEnv{t: t, bin: bin, dir: filepath.Join(dir, "state"), tmp: filepath.Join(dir, "tmp"), hook: filepath.Join(wd, "hooks")}
	for _, d := range []string{e.dir, e.tmp} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = e.command("", "stop").Run() })
	return e
}

// env returns the environment for a mess/hook invocation acting as `as`.
func (e *hookEnv) env(as string, extra ...string) []string {
	env := append(os.Environ(),
		"MESS_DIR="+e.dir,
		"TMPDIR="+e.tmp,
		"MESS_BIN="+e.bin,
		"MESS_NO_NOTIFY=1",
		"MESS_NO_BRIDGE=1",
	)
	if as != "" {
		env = append(env, "MESS_AGENT="+as, "MESS_SESSION_ID=sess-"+as)
	}
	return append(env, extra...)
}

func (e *hookEnv) command(as string, args ...string) *exec.Cmd {
	cmd := exec.Command(e.bin, args...)
	cmd.Env = e.env(as)
	return cmd
}

// mess runs a mess command and fails the test if it errors.
func (e *hookEnv) mess(as string, args ...string) string {
	e.t.Helper()
	out, err := e.command(as, args...).CombinedOutput()
	if err != nil {
		e.t.Fatalf("mess %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// listening reports whether the agent currently has a parked waiter — the thing
// that makes it wakeable at all.
func (e *hookEnv) listening(as string) bool {
	return e.command(as, "islistening", "--as", as).Run() == nil
}

func (e *hookEnv) waitListening(as string, want bool, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if e.listening(as) == want {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// runningHook is a backgrounded mess-wake.sh, with its stderr (where an
// asyncRewake delivery lands) captured for inspection once it exits.
type runningHook struct {
	cmd    *exec.Cmd
	stderr *bytes.Buffer
}

// wait blocks for the hook to exit and returns its exit code and stderr. The
// exit code is the whole contract with asyncRewake: 2 wakes the agent, 0 does
// not (and does not re-arm).
func (h *runningHook) wait(t *testing.T, within time.Duration) (int, string) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- h.cmd.Wait() }()
	select {
	case err := <-done:
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatalf("waiting for wake hook: %v", err)
		}
		return code, h.stderr.String()
	case <-time.After(within):
		t.Fatalf("wake hook did not exit within %s (it kept parking)", within)
		return 0, ""
	}
}

// wakeHook starts hooks/mess-wake.sh in the background as `as`.
func (e *hookEnv) wakeHook(as string, extra ...string) *runningHook {
	cmd := exec.Command("sh", filepath.Join(e.hook, "mess-wake.sh"))
	cmd.Env = e.env(as, extra...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		e.t.Fatalf("starting wake hook: %v", err)
	}
	e.t.Cleanup(func() { _ = cmd.Process.Kill() })
	return &runningHook{cmd: cmd, stderr: &stderr}
}

// The core wake bug: a park that wakes on real mail but finds the inbox already
// drained (a second hook instance, or the agent's own mid-turn `mess recv`)
// used to exit 0. Under asyncRewake exit 0 neither wakes nor re-arms, so the
// agent went deaf until its next Stop — which for an idle agent never comes.
// The hook must go back to parking instead.
func TestWakeHookRepArksAfterLosingTheDrainRace(t *testing.T) {
	e := newHookEnv(t)
	e.mess("victim", "register", "victim")
	e.mess("sender", "register", "sender")

	h := e.wakeHook("victim")
	if !e.waitListening("victim", true, 5*time.Second) {
		t.Fatal("wake hook never parked")
	}

	// Mail arrives; a racing consumer drains it inside the hook's --batch
	// window, so the hook's park returns empty.
	e.mess("sender", "send", "victim", "hello")
	time.Sleep(150 * time.Millisecond)
	e.mess("victim", "recv", "--if-idle", "--no-broadcast", "--json")

	// The doomed park still counts as listening until its batch window expires,
	// so wait that out before asking whether the hook stood down or re-parked.
	time.Sleep(1500 * time.Millisecond)
	if !e.listening("victim") {
		t.Fatal("hook stood down after losing the drain race — the agent is now unwakeable until its next Stop")
	}

	// And it is a live park, not a stale listener: the next message must
	// actually be delivered, as an asyncRewake (exit 2, body on stderr).
	e.mess("sender", "send", "victim", "second")
	code, stderr := h.wait(t, 10*time.Second)
	if code != 2 || !strings.Contains(stderr, "second") {
		t.Fatalf("re-parked hook did not deliver the next message: exit=%d stderr=%q", code, stderr)
	}
}

// A wake can end up with nothing but quiet mail to hand over: the message that
// triggered it is taken by a racing consumer, leaving only a copy that was
// delivered-without-notifying. There is nothing to wake the agent for, but that
// is not a reason to give up the park either.
func TestWakeHookRepArksWhenItsWakeLeavesOnlyQuietMail(t *testing.T) {
	e := newHookEnv(t)
	e.mess("victim", "register", "victim")
	e.mess("sender", "register", "sender")
	e.mess("victim", "sub", "work")
	e.mess("sender", "sub", "work")

	e.wakeHook("victim")
	if !e.waitListening("victim", true, 5*time.Second) {
		t.Fatal("wake hook never parked")
	}

	// The @mention names someone else, so victim's copy is quiet: queued, but
	// not a wake trigger on its own.
	e.mess("sender", "pub", "work", "@sender only, not victim")
	time.Sleep(300 * time.Millisecond)
	if !e.listening("victim") {
		t.Fatal("a quiet message must not wake the park")
	}

	// A direct message does trigger the wake — and a racer takes exactly that
	// one, so the park drains nothing but the quiet copy.
	e.mess("sender", "send", "victim", "hello")
	time.Sleep(150 * time.Millisecond)
	e.mess("victim", "recv", "--if-idle", "--kind", "direct", "--json")

	time.Sleep(1500 * time.Millisecond)
	if !e.listening("victim") {
		t.Fatal("hook stood down when its wake turned up only quiet mail")
	}
}

// Orphan guard: when the session that launched it dies without a clean Stop,
// the hook must stop parking. Otherwise it holds the per-agent lock (so the
// next session can never park) and keeps a phantom `listening` that makes
// senders believe their wake landed.
func TestWakeHookStandsDownWhenItsSessionDies(t *testing.T) {
	e := newHookEnv(t)
	e.mess("victim", "register", "victim")

	session := exec.Command("sleep", "60")
	if err := session.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Process.Kill(); _ = session.Wait() }()

	hook := e.wakeHook("victim",
		"MESS_WAKE_SESSION_PID="+strconv.Itoa(session.Process.Pid),
		"MESS_WAKE_PARK_TIMEOUT=300ms",
	)
	if !e.waitListening("victim", true, 5*time.Second) {
		t.Fatal("wake hook never parked")
	}
	// Still parking while the session lives, despite the short park timeout.
	time.Sleep(time.Second)
	if !e.listening("victim") {
		t.Fatal("hook stood down while its session was still alive")
	}

	_ = session.Process.Kill()
	_ = session.Wait()

	// Standing down is silent (exit 0): there is no session left to wake, and a
	// rewake here would just be re-run into the same state.
	if code, _ := hook.wait(t, 5*time.Second); code != 0 {
		t.Fatalf("orphaned hook should stand down silently, got exit %d", code)
	}
	if !e.waitListening("victim", false, 2*time.Second) {
		t.Fatal("orphaned hook left a phantom listener behind")
	}
}

// steer runs hooks/mess-steer.sh once and returns its stdout (the notice, if any).
func (e *hookEnv) steer(as string, extra ...string) string {
	e.t.Helper()
	cmd := exec.Command("sh", filepath.Join(e.hook, "mess-steer.sh"), "PreToolUse")
	cmd.Env = e.env(as, extra...)
	out, _ := cmd.Output()
	return string(out)
}

// The mid-turn notice used to watermark on print: it advanced its "already
// announced" marker the moment it emitted, with no confirmation the agent saw
// it. One dropped injection meant that message was never mentioned again. It
// must re-announce while the mail stays unread.
func TestSteerHookReAnnouncesStillUnreadMail(t *testing.T) {
	e := newHookEnv(t)
	e.mess("victim", "register", "victim")
	e.mess("sender", "register", "sender")
	e.mess("sender", "send", "victim", "hello")

	first := e.steer("victim", "MESS_STEER_RENOTIFY=60")
	if !strings.Contains(first, "1 unread peer message") {
		t.Fatalf("no notice for new mail: %q", first)
	}
	// Immediately after, the same mail must not re-fire (that would spam every
	// tool call).
	if again := e.steer("victim", "MESS_STEER_RENOTIFY=60"); again != "" {
		t.Fatalf("re-announced within the window: %q", again)
	}
	// Once the window passes and the mail is STILL unread, say so again.
	time.Sleep(2100 * time.Millisecond)
	retry := e.steer("victim", "MESS_STEER_RENOTIFY=1")
	if !strings.Contains(retry, "still unread") {
		t.Fatalf("dropped notice was never retried: %q", retry)
	}

	// Once read, it goes quiet again.
	e.mess("victim", "recv")
	time.Sleep(2100 * time.Millisecond)
	if after := e.steer("victim", "MESS_STEER_RENOTIFY=1"); after != "" {
		t.Fatalf("kept announcing after the inbox was drained: %q", after)
	}
}

// A daemon that refuses the peek must not look like an empty inbox. Both hooks
// used to send every error to /dev/null, so an agent that had silently stopped
// receiving had no signal at all — `mess whoami` keeps working off a local file
// even when every daemon operation is rejected.
func TestSteerHookReportsAnOutageInsteadOfLookingEmpty(t *testing.T) {
	e := newHookEnv(t)
	e.mess("victim", "register", "victim")
	e.mess("sender", "register", "sender")
	e.mess("sender", "send", "victim", "hello")

	// A different live session claiming the same name is refused by the
	// daemon's ownership guard — the shape of a real identity collision.
	cmd := exec.Command("sh", filepath.Join(e.hook, "mess-steer.sh"), "PreToolUse")
	cmd.Env = e.env("victim", "MESS_SESSION_ID=some-other-session")
	out, _ := cmd.Output()

	if !strings.Contains(string(out), "can't read your inbox") {
		t.Fatalf("a refused peek was reported as silence, not as an outage: %q", out)
	}
}
