package main

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// ccEnv writes a fake Claude Code session registry and points ccSessions at it.
type ccEnv struct {
	t    *testing.T
	dir  string
	live map[int]string // pid -> comm
}

func newCCEnv(t *testing.T) *ccEnv {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "sessions"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLAUDE_CONFIG_DIR", root)
	e := &ccEnv{t: t, dir: root, live: map[int]string{}}
	prev := ccProcessComm
	ccProcessComm = func(pid int) string { return e.live[pid] }
	t.Cleanup(func() { ccProcessComm = prev })
	return e
}

// write records a session. sock != "" creates a real socket file, because the
// code checks the socket exists rather than trusting the recorded path.
func (e *ccEnv) write(pid int, name, status, cwd string, sock bool) string {
	e.t.Helper()
	rec := map[string]any{"pid": pid, "name": name, "status": status, "cwd": cwd}
	sockPath := ""
	if sock {
		// Keep it short: a unix socket path has a low length limit.
		sockPath = filepath.Join(e.t.TempDir(), "s.sock")
		l, err := net.Listen("unix", sockPath)
		if err != nil {
			e.t.Fatal(err)
		}
		e.t.Cleanup(func() { l.Close() })
		rec["messagingSocketPath"] = sockPath
	}
	b, err := json.Marshal(rec)
	if err != nil {
		e.t.Fatal(err)
	}
	f := filepath.Join(e.dir, "sessions", strconv.Itoa(pid)+".json")
	if err := os.WriteFile(f, b, 0o600); err != nil {
		e.t.Fatal(err)
	}
	e.live[pid] = "claude"
	return sockPath
}

func find(t *testing.T, ss []ccSession, name string) ccSession {
	t.Helper()
	for _, s := range ss {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("session %q not in roster %+v", name, ss)
	return ccSession{}
}

// The whole point of reading Claude Code's registry is deliverability: a session
// with a record but no socket cannot be reached by SendMessage even though it is
// running, and that difference is what the roster must show.
func TestCCSessionsReportAddressability(t *testing.T) {
	e := newCCEnv(t)
	e.write(101, "with-socket", "idle", "/home/x/proj", true)
	e.write(102, "no-socket", "busy", "/home/x/other", false)

	got := ccSessions()
	if len(got) != 2 {
		t.Fatalf("want 2 sessions, got %d: %+v", len(got), got)
	}
	if s := find(t, got, "with-socket"); !s.Addressable() {
		t.Fatalf("session with a live socket should be addressable: %+v", s)
	}
	if s := find(t, got, "no-socket"); s.Addressable() {
		t.Fatalf("session without a socket must not read as addressable: %+v", s)
	}
	if s := find(t, got, "no-socket"); s.Status != "busy" {
		t.Fatalf("status should come from the record: %+v", s)
	}
}

// Claude Code deletes the record on clean exit, so a leftover file is a crashed
// session. Reporting it would recreate the stale-presence bug mess already fixed
// for its own roster.
func TestCCSessionsSkipDeadProcesses(t *testing.T) {
	e := newCCEnv(t)
	e.write(201, "alive", "idle", "/home/x", true)
	e.write(202, "crashed", "idle", "/home/x", true)
	delete(e.live, 202) // process is gone; the file was never cleaned up

	got := ccSessions()
	if len(got) != 1 || got[0].Name != "alive" {
		t.Fatalf("dead session should be skipped, got %+v", got)
	}
}

// A recycled pid must not be mistaken for a session: the file says pid 301, but
// pid 301 is now something else entirely.
func TestCCSessionsIgnoreRecycledPIDs(t *testing.T) {
	e := newCCEnv(t)
	e.write(301, "ghost", "idle", "/home/x", true)
	e.live[301] = "postgres"

	if got := ccSessions(); len(got) != 0 {
		t.Fatalf("a pid running something else must not count as a session: %+v", got)
	}
}

// A recorded socket path that no longer exists is not deliverability. The path
// is written at startup; the socket can go away underneath it.
func TestCCSessionsRequireTheSocketToExist(t *testing.T) {
	e := newCCEnv(t)
	sock := e.write(401, "vanished", "idle", "/home/x", true)
	if err := os.Remove(sock); err != nil {
		t.Fatal(err)
	}

	if s := find(t, ccSessions(), "vanished"); s.Addressable() {
		t.Fatalf("a recorded-but-missing socket must not read as addressable: %+v", s)
	}
}

// Reading another tool's private files must never take mess ps down.
func TestCCSessionsDegradeQuietly(t *testing.T) {
	e := newCCEnv(t)
	if err := os.WriteFile(filepath.Join(e.dir, "sessions", "not-a-pid.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(e.dir, "sessions", "999.json"), []byte(`{"pid":`), 0o600); err != nil {
		t.Fatal(err)
	}
	e.live[999] = "claude"
	e.write(501, "good", "idle", "/home/x", true)

	if got := ccSessions(); len(got) != 1 || got[0].Name != "good" {
		t.Fatalf("malformed entries should be skipped, not fatal: %+v", got)
	}

	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(e.dir, "does-not-exist"))
	if got := ccSessions(); got != nil {
		t.Fatalf("a missing directory should yield no sessions, got %+v", got)
	}
}

// The annotation is the whole user-visible point of the join, and each branch
// says something different about what a peer can do.
func TestCCAnnotationDistinguishesTheCasesThatMatter(t *testing.T) {
	live := ccSession{Name: "uring-bpf", Socket: "/run/x.sock"}
	if got := ccAnnotation("uring-bpf", live); got != "  [cc]" {
		t.Fatalf("agreeing names should not repeat the name: %q", got)
	}

	differing := ccSession{Name: "claude-bf", Socket: "/run/x.sock"}
	if got := ccAnnotation("uring-bpf", differing); got != "  [cc: claude-bf]" {
		t.Fatalf("a differing CC name should be shown: %q", got)
	}

	// Alive in mess, undeliverable by SendMessage — the case that actually
	// changes a peer's options, so it must never render as a plain [cc].
	noSock := ccSession{Name: "claude-bf"}
	if got := ccAnnotation("uring-bpf", noSock); got != "  [cc: no socket]" {
		t.Fatalf("an unreachable session must say so: %q", got)
	}
}

func TestCompactHomeShortensUnderHome(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		t.Skip("no home directory")
	}
	if got := compactHome(filepath.Join(home, "git", "mess")); got != "~/git/mess" {
		t.Fatalf("want ~/git/mess, got %q", got)
	}
	if got := compactHome(home); got != "~" {
		t.Fatalf("want ~, got %q", got)
	}
	if got := compactHome("/etc/hosts"); got != "/etc/hosts" {
		t.Fatalf("paths outside home must be untouched, got %q", got)
	}
}
