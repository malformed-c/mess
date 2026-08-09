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

// The SessionEnd hook is the clean counterpart to the orphan guard: it retires
// presence the moment a session ends, instead of leaving `working` set for
// busy's one-hour backstop and the wake hook parked until it notices. Crucially
// it must free the per-agent lock, or a relaunch under the same name can't park.
func TestSessionEndHookFreesTheWaiterForARelaunch(t *testing.T) {
	e := newHookEnv(t)
	e.mess("victim", "register", "victim")
	e.mess("sender", "register", "sender")

	hook := e.wakeHook("victim")
	if !e.waitListening("victim", true, 5*time.Second) {
		t.Fatal("wake hook never parked")
	}
	e.mess("victim", "busy", "--ttl", "1h") // mid-turn when the session ends

	cmd := exec.Command("sh", filepath.Join(e.hook, "mess-session-end.sh"))
	cmd.Env = e.env("victim")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("session-end hook: %v\n%s", err, out)
	}

	// The parked waiter is evicted, so it exits and releases both its listener
	// and the lock.
	if code, _ := hook.wait(t, 5*time.Second); code != 0 {
		t.Fatalf("evicted wake hook should exit cleanly, got %d", code)
	}
	if !e.waitListening("victim", false, 3*time.Second) {
		t.Fatal("session-end left a phantom listener behind")
	}
	for _, line := range strings.Split(e.mess("victim", "ps"), "\n") {
		if strings.Contains(line, "victim") && strings.Contains(line, "working") {
			t.Fatalf("session-end must clear `working`, not leave busy's 1h backstop running: %q", line)
		}
	}

	// A relaunch under the same name can park straight away, and identity and
	// queued mail survive — ending a session is not leaving the network.
	relaunch := e.wakeHook("victim")
	if !e.waitListening("victim", true, 5*time.Second) {
		t.Fatal("a relaunch under the same name could not park — the lock was never released")
	}
	e.mess("sender", "send", "victim", "mail after the session ended")
	code, stderr := relaunch.wait(t, 10*time.Second)
	if code != 2 || !strings.Contains(stderr, "mail after the session ended") {
		t.Fatalf("relaunched hook did not deliver mail queued while away: exit=%d stderr=%q", code, stderr)
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

// `mess shout` supersedes `broadcast --loud`, which went host-wide: it woke
// every agent in every room, so the one command reached for when something
// must not be missed was also the only routine way to breach a room boundary.
// A room is an exclusive namespace, so leaving it is now an explicit choice.
func TestShoutStaysInItsRoomUnlessAskedToCross(t *testing.T) {
	e := newHookEnv(t)
	e.mess("alice", "register", "alice")
	e.mess("bob", "register", "bob")
	e.mess("outsider", "register", "outsider")
	e.mess("outsider", "room", "join", "coord")

	out := e.mess("alice", "shout", "everyone in my room")
	if !strings.Contains(out, "1 agent(s) in your room") {
		t.Fatalf("a plain shout should reach only the sender's room, got %q", out)
	}
	if got := e.mess("outsider", "recv"); strings.Contains(got, "everyone in my room") {
		t.Fatalf("shout leaked across a room boundary: %q", got)
	}

	out = e.mess("alice", "shout", "--host-wide", "everyone everywhere")
	if !strings.Contains(out, "2 agent(s) in every room") {
		t.Fatalf("--host-wide should cross rooms, got %q", out)
	}
	if got := e.mess("outsider", "recv"); !strings.Contains(got, "everyone everywhere") {
		t.Fatalf("--host-wide shout did not reach the other room: %q", got)
	}
	_ = e.mess("bob", "recv")
}

// A shout has to actually wake a waiter parked with --no-broadcast — that is
// the whole point of it, and the difference from a plain broadcast.
func TestShoutWakesAParkedWaiterAndAPlainBroadcastDoesNot(t *testing.T) {
	e := newHookEnv(t)
	e.mess("alice", "register", "alice")
	e.mess("bob", "register", "bob")

	h := e.wakeHook("bob")
	if !e.waitListening("bob", true, 5*time.Second) {
		t.Fatal("wake hook never parked")
	}

	// A plain, unmentioning broadcast must leave it parked.
	e.mess("alice", "broadcast", "routine status, nobody in particular")
	time.Sleep(1500 * time.Millisecond)
	if !e.listening("bob") {
		t.Fatal("a plain broadcast woke a --no-broadcast waiter — that is the wake storm this filter exists to prevent")
	}

	e.mess("alice", "shout", "everybody up")
	code, stderr := h.wait(t, 10*time.Second)
	if code != 2 || !strings.Contains(stderr, "everybody up") {
		t.Fatalf("shout did not wake the parked hook: exit=%d stderr=%q", code, stderr)
	}
}

// The same, via an @mention on an ordinary broadcast: it must wake the agent
// named without shouting at the rest of the room.
func TestBroadcastMentionWakesTheNamedAgentEndToEnd(t *testing.T) {
	e := newHookEnv(t)
	e.mess("alice", "register", "alice")
	e.mess("bob", "register", "bob")

	h := e.wakeHook("bob")
	if !e.waitListening("bob", true, 5*time.Second) {
		t.Fatal("wake hook never parked")
	}

	e.mess("alice", "broadcast", "@bob can you check the deploy?")
	code, stderr := h.wait(t, 10*time.Second)
	if code != 2 || !strings.Contains(stderr, "check the deploy") {
		t.Fatalf("an @mentioning broadcast did not wake the named agent: exit=%d stderr=%q", code, stderr)
	}
}

// agents returns the composite "room/name" keys `mess ps --all` reports.
func (e *hookEnv) agents(as string) string {
	return e.mess(as, "ps", "--all")
}

// Addressing another room used to leak your identity into it. Request.Room
// served two masters — "the room I'm acting in" and "the room my target is in"
// — and the daemon keyed the CALLER off it, so `send --room coord` claimed and
// materialized a ghost "coord/alice". That ghost then absorbed the room's
// broadcasts and, worst of all, swallowed direct replies: a peer inside coord
// answering plain "alice" resolved to the ghost, and the real alice never saw
// it — no error, no trace, message gone.
//
// This runs through the real CLI because the key is materialized by the
// ClaimIdentity gate in handle, above the dispatch layer.
func TestAddressingAnotherRoomDoesNotLeakYourIdentityIntoIt(t *testing.T) {
	e := newHookEnv(t)
	e.mess("alice", "register", "alice") // global room
	e.mess("carol", "register", "carol")
	e.mess("carol", "room", "join", "coord")

	e.mess("alice", "send", "carol", "cross-room hello", "--room", "coord")

	if got := e.agents("alice"); strings.Contains(got, "coord/alice") {
		t.Fatalf("addressing a room materialized a ghost copy of the sender inside it:\n%s", got)
	}
	if got := e.mess("carol", "recv"); !strings.Contains(got, "cross-room hello") {
		t.Fatalf("the real recipient never got the message: %q", got)
	}

	// The consequence that actually cost a message: a reply from inside coord
	// to the bare name "alice" must not find a ghost to fall into.
	out, err := e.command("carol", "send", "alice", "answer").CombinedOutput()
	if err == nil {
		t.Fatalf("a reply to a name living in another room should be refused, not swallowed: %q", out)
	}
	if !strings.Contains(string(out), "--global") {
		t.Fatalf("the refusal should name the flag that reaches the global room, got %q", out)
	}
	// ...and once aimed correctly it lands on the real alice.
	e.mess("carol", "send", "alice", "answer", "--global")
	if got := e.mess("alice", "recv"); !strings.Contains(got, "answer") {
		t.Fatalf("the correctly-aimed reply never reached the real sender: %q", got)
	}
}

// The fan-out paths grew --room in the same change, so they must not leak
// either — including host-wide, which touches every room at once.
func TestBroadcastPubAndShoutDoNotLeakTheSenderIntoOtherRooms(t *testing.T) {
	e := newHookEnv(t)
	e.mess("alice", "register", "alice")
	e.mess("carol", "register", "carol")
	e.mess("carol", "room", "join", "coord")
	e.mess("dave", "register", "dave")
	e.mess("dave", "room", "join", "ops")
	e.mess("carol", "sub", "builds")

	e.mess("alice", "broadcast", "--room", "coord", "into coord")
	e.mess("alice", "pub", "--room", "coord", "builds", "published into coord")
	e.mess("alice", "shout", "--room", "coord", "shouted into coord")
	e.mess("alice", "shout", "--host-wide", "everyone everywhere")

	got := e.agents("alice")
	for _, ghost := range []string{"coord/alice", "ops/alice"} {
		if strings.Contains(got, ghost) {
			t.Fatalf("a cross-room fan-out materialized %q:\n%s", ghost, got)
		}
	}
	// The real agents in those rooms did receive everything aimed at them.
	if out := e.mess("carol", "recv"); !strings.Contains(out, "into coord") ||
		!strings.Contains(out, "published into coord") || !strings.Contains(out, "everyone everywhere") {
		t.Fatalf("cross-room fan-out did not deliver: %q", out)
	}
	if out := e.mess("dave", "recv"); !strings.Contains(out, "everyone everywhere") {
		t.Fatalf("host-wide shout did not reach the other room: %q", out)
	}
}

// `mess reply` grew the same pair, so a cross-room conversation can be held up
// without the replier having to reconstruct the route by hand.
func TestReplyCanAimAtAnotherRoom(t *testing.T) {
	e := newHookEnv(t)
	e.mess("alice", "register", "alice")
	e.mess("carol", "register", "carol")
	e.mess("carol", "room", "join", "coord")

	e.mess("carol", "send", "alice", "question from coord", "--global")
	e.mess("alice", "recv") // makes it alice's reply target
	e.mess("alice", "reply", "--room", "coord", "answer back to coord")

	if got := e.mess("carol", "recv"); !strings.Contains(got, "answer back to coord") {
		t.Fatalf("cross-room reply never arrived: %q", got)
	}
	if got := e.agents("alice"); strings.Contains(got, "coord/alice") {
		t.Fatalf("replying into a room leaked an identity there:\n%s", got)
	}
}

// The whole point, end to end: an agent that answers a `mess ask` the natural
// way — a plain send that @mentions the asker — must satisfy the asker's wait
// instead of leaving it to time out with the answer sitting unread.
func TestAskIsAnsweredByAPlainMentionEndToEnd(t *testing.T) {
	e := newHookEnv(t)
	e.mess("alice", "register", "alice")
	e.mess("bob", "register", "bob")

	// bob has to look online for `ask` to proceed at all.
	park := e.command("bob", "recv", "--wait", "20s")
	if err := park.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = park.Process.Kill() }()
	if !e.waitListening("bob", true, 5*time.Second) {
		t.Fatal("bob never parked")
	}

	asked := make(chan string, 1)
	go func() {
		out, _ := e.command("alice", "ask", "bob", "ready to deploy?", "--timeout", "15s").CombinedOutput()
		asked <- string(out)
	}()

	time.Sleep(1500 * time.Millisecond)
	e.mess("bob", "send", "alice", "@alice yes, ready — go ahead")

	select {
	case out := <-asked:
		if !strings.Contains(out, "yes, ready") {
			t.Fatalf("ask was not satisfied by a plain @mention answer: %q", out)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("ask never returned")
	}
}

// What a woken agent should do next. The wake hook parks with --peek, but the
// step that actually delivers is a NON-peek `recv --if-idle` — so by the time
// the agent is woken its inbox is already empty and the bodies are in the
// injection. A follow-up `mess recv` finds nothing. This is pinned as a test
// because the docs claimed the opposite ("the wake only peeks, so run mess recv
// first"), which had every woken agent burning a tool call on an empty drain.
func TestAWokenAgentsInboxIsAlreadyEmpty(t *testing.T) {
	e := newHookEnv(t)
	e.mess("bob", "register", "bob")
	e.mess("alice", "register", "alice")

	h := e.wakeHook("bob")
	if !e.waitListening("bob", true, 5*time.Second) {
		t.Fatal("wake hook never parked")
	}
	e.mess("alice", "send", "bob", "the actual message body")

	code, stderr := h.wait(t, 10*time.Second)
	if code != 2 {
		t.Fatalf("want an asyncRewake delivery (exit 2), got %d", code)
	}
	if !strings.Contains(stderr, "the actual message body") {
		t.Fatalf("the wake must carry the body itself, got %q", stderr)
	}
	// The whole point: nothing is left to fetch.
	if got := e.mess("bob", "recv"); strings.TrimSpace(got) != "" {
		t.Fatalf("a woken agent's inbox should already be drained; `mess recv` returned %q", got)
	}
}

// ...with one exception worth being precise about: the delivering drain is
// --no-broadcast (matching the park's own wake filter), so a plain broadcast
// queued at wake time is NOT swept up and does still need a `mess recv`.
func TestAWakeLeavesNonWakingBacklogQueued(t *testing.T) {
	e := newHookEnv(t)
	e.mess("bob", "register", "bob")
	e.mess("alice", "register", "alice")

	h := e.wakeHook("bob")
	if !e.waitListening("bob", true, 5*time.Second) {
		t.Fatal("wake hook never parked")
	}
	e.mess("alice", "broadcast", "fleet notice nobody is woken for")
	time.Sleep(500 * time.Millisecond)
	if !e.listening("bob") {
		t.Fatal("a plain broadcast must not wake the park")
	}
	e.mess("alice", "send", "bob", "the direct message that wakes")

	code, stderr := h.wait(t, 10*time.Second)
	if code != 2 || !strings.Contains(stderr, "the direct message that wakes") {
		t.Fatalf("want the direct message delivered on wake, got exit=%d stderr=%q", code, stderr)
	}
	if strings.Contains(stderr, "fleet notice") {
		t.Fatal("a plain broadcast should not be swept into the wake injection")
	}
	if got := e.mess("bob", "recv"); !strings.Contains(got, "fleet notice") {
		t.Fatalf("the non-waking broadcast should still be queued for a later recv, got %q", got)
	}
}

// mess-ask-notify.sh had drifted furthest from its siblings precisely because
// nothing could drive it: it hardcoded the binary path with no MESS_BIN
// override, and had lost the Grok session mapping, so on Grok it silently did
// nothing while the other three worked. Now that the preamble is shared, it is
// testable — which is the point of sharing it.
func TestAskNotifyHookResolvesItsIdentityLikeTheOthers(t *testing.T) {
	e := newHookEnv(t)
	e.mess("alice", "register", "alice")

	run := func(env []string, stdin string) (string, int) {
		cmd := exec.Command("sh", filepath.Join(e.hook, "mess-ask-notify.sh"))
		cmd.Env = env
		cmd.Stdin = strings.NewReader(stdin)
		out, err := cmd.CombinedOutput()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
		return string(out), code
	}
	const askInput = `{"tool_name":"AskUserQuestion","tool_input":{"questions":[{"question":"ship it?"}]}}`

	// A session with an identity gets past the guard (notify-send may be absent
	// here, which is fine — the hook degrades silently and still exits 0).
	if _, code := run(e.env("alice"), askInput); code != 0 {
		t.Fatalf("hook should exit 0 for an identified session, got %d", code)
	}

	// Grok injects GROK_SESSION_ID instead of MESS_SESSION_ID. The shared
	// preamble maps it across; without that this hook resolved no identity and
	// silently did nothing, unlike its three siblings.
	grok := []string{}
	for _, kv := range e.env("alice") {
		if !strings.HasPrefix(kv, "MESS_SESSION_ID=") {
			grok = append(grok, kv)
		}
	}
	grok = append(grok, "GROK_SESSION_ID=sess-alice")
	cmd := exec.Command("sh", filepath.Join(e.hook, "mess-common.sh"))
	cmd.Env = grok
	_ = cmd.Run() // sourcing it standalone is a no-op; the check below is the real one

	probe := exec.Command("sh", "-c", `. "$1/mess-common.sh"; printf '%s' "$who"`, "sh", e.hook)
	probe.Env = grok
	out, err := probe.Output()
	if err != nil {
		t.Fatalf("probing the shared preamble: %v", err)
	}
	if string(out) != "alice" {
		t.Fatalf("the shared preamble must map GROK_SESSION_ID into an identity, resolved %q", out)
	}
}

// A missing shared preamble must make a hook no-op, not misbehave with unset
// variables — the failure mode of factoring it out at all.
func TestHooksNoOpIfTheSharedPreambleIsMissing(t *testing.T) {
	e := newHookEnv(t)
	bare := t.TempDir()
	for _, h := range []string{"mess-wake.sh", "mess-steer.sh", "mess-session-end.sh", "mess-ask-notify.sh"} {
		src, err := os.ReadFile(filepath.Join(e.hook, h))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bare, h), src, 0o755); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("sh", filepath.Join(bare, h))
		cmd.Env = e.env("alice")
		cmd.Stdin = strings.NewReader("{}")
		out, err := cmd.CombinedOutput()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
		if code != 0 {
			t.Fatalf("%s should stand down quietly without mess-common.sh, got exit %d: %s", h, code, out)
		}
	}
}
