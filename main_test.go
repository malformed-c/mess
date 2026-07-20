package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// --- detectRepoRoom ---
//
// mess register auto-joins a room derived from a repo's mess.json, so
// agents working on the same codebase naturally group together instead of
// sharing one noisy global room by default — but only for a repo that
// explicitly opts in. There's deliberately no fallback to the repo
// directory's basename (there used to be — removed because a directory
// name is often a meaningless generated path, a temp checkout or worktree,
// producing confusing/ugly room names for a repo that never asked for
// auto-join at all).

func TestDetectRepoRoomDormantWithoutMessJSON(t *testing.T) {
	dir := t.TempDir()
	if out, err := exec.Command("git", "-C", dir, "init", "-q").CombinedOutput(); err != nil {
		t.Skipf("git not available: %v: %s", err, out)
	}
	if got := detectRepoRoom(dir); got != "" {
		t.Fatalf("expected no auto-join without a mess.json, got %q", got)
	}
}

func TestDetectRepoRoomOutsideGitRepo(t *testing.T) {
	dir := t.TempDir() // no `git init` — not a repo
	if got := detectRepoRoom(dir); got != "" {
		t.Fatalf("expected no room outside a git repo, got %q", got)
	}
}

// MESS_NO_AUTO_ROOM overrides even an explicit mess.json — proven by giving
// the repo one that WOULD otherwise activate auto-join.
func TestDetectRepoRoomRespectsOptOut(t *testing.T) {
	dir := t.TempDir()
	if out, err := exec.Command("git", "-C", dir, "init", "-q").CombinedOutput(); err != nil {
		t.Skipf("git not available: %v: %s", err, out)
	}
	if err := os.WriteFile(filepath.Join(dir, "mess.json"), []byte(`{"room":"custom-room"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MESS_NO_AUTO_ROOM", "1")
	if got := detectRepoRoom(dir); got != "" {
		t.Fatalf("expected MESS_NO_AUTO_ROOM to suppress detection even with a mess.json present, got %q", got)
	}
}

// mess.json's "room" key overrides the repo directory's basename — for a
// repo whose directory name isn't the room you want, or several repos that
// should share one room.
func TestDetectRepoRoomHonorsMessJSONOverride(t *testing.T) {
	dir := t.TempDir()
	if out, err := exec.Command("git", "-C", dir, "init", "-q").CombinedOutput(); err != nil {
		t.Skipf("git not available: %v: %s", err, out)
	}
	if err := os.WriteFile(filepath.Join(dir, "mess.json"), []byte(`{"room":"custom-room"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := detectRepoRoom(dir); got != "custom-room" {
		t.Fatalf("expected mess.json's room override, got %q", got)
	}
}

// A malformed mess.json must not break registration itself — auto-join just
// stays dormant for that repo (the warning it also prints isn't checked
// here, only that detectRepoRoom doesn't error or panic).
func TestDetectRepoRoomDormantOnMalformedMessJSON(t *testing.T) {
	dir := t.TempDir()
	if out, err := exec.Command("git", "-C", dir, "init", "-q").CombinedOutput(); err != nil {
		t.Skipf("git not available: %v: %s", err, out)
	}
	if err := os.WriteFile(filepath.Join(dir, "mess.json"), []byte(`{not valid json`), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := detectRepoRoom(dir); got != "" {
		t.Fatalf("expected auto-join to stay dormant on a malformed mess.json, got %q", got)
	}
}

func TestReadRepoConfigMissingFileIsNotAnError(t *testing.T) {
	cfg, err := readRepoConfig(filepath.Join(t.TempDir(), "mess.json"))
	if err != nil {
		t.Fatalf("a missing mess.json should not be an error, got %v", err)
	}
	if cfg.Room != "" {
		t.Fatalf("expected an empty config, got %+v", cfg)
	}
}

func TestReadRepoConfigMalformedFileIsAnError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mess.json")
	if err := os.WriteFile(path, []byte(`{not valid json`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readRepoConfig(path); err == nil {
		t.Fatal("expected a malformed mess.json to be a hard error, not silently ignored")
	}
}

// --- bodyFrom --file ---
//
// The actual fix for a real papercut: a message body containing backticks
// (very common — pasting `go build ./...`, file/cmd names) gets command-
// substituted by the SENDER's own shell before mess ever sees the arg, so
// the peer receives a silently mangled message and the sender's shell runs
// an arbitrary local command. mess can't detect this after the fact (the
// original backticks are already gone by the time argv is read) — --file
// sidesteps the shell entirely by reading the body straight off disk.

func TestBodyFromReadsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "body.txt")
	if err := os.WriteFile(path, []byte("contains `backticks` and $(command) literally\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := bodyFrom(nil, path)
	if err != nil {
		t.Fatal(err)
	}
	if got != "contains `backticks` and $(command) literally" {
		t.Fatalf("expected the file's literal content (trailing newline trimmed), got %q", got)
	}
}

func TestBodyFromErrorsOnMissingFile(t *testing.T) {
	if _, err := bodyFrom(nil, filepath.Join(t.TempDir(), "nope.txt")); err == nil {
		t.Fatal("expected an error for a missing --file path")
	}
}

func TestBodyFromRejectsFileAndArgsTogether(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "body.txt")
	if err := os.WriteFile(path, []byte("from file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := bodyFrom([]string{"also", "on", "argv"}, path); err == nil {
		t.Fatal("expected an error when both --file and a body argument are given")
	}
}

func TestSetAttachComputesHashAndStat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := []byte("hello attachment")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	wantHash := "sha256:" + hex.EncodeToString(sum[:])

	var req Request
	if err := setAttach(&req, path); err != nil {
		t.Fatal(err)
	}
	if req.AttachHash != wantHash {
		t.Fatalf("hash mismatch: got %q want %q", req.AttachHash, wantHash)
	}
	if req.AttachSize != int64(len(content)) {
		t.Fatalf("expected size %d, got %d", len(content), req.AttachSize)
	}
	if !filepath.IsAbs(req.AttachPath) {
		t.Fatalf("expected an absolute path, got %q", req.AttachPath)
	}
	if req.AttachMTime.IsZero() {
		t.Fatal("expected a non-zero mtime")
	}
}

// A missing/unreadable file is a hard error before any daemon round trip —
// a wrong attachment reference is a correctness bug, not a best-effort nicety.
func TestSetAttachErrorsOnMissingFile(t *testing.T) {
	var req Request
	if err := setAttach(&req, filepath.Join(t.TempDir(), "does-not-exist.yaml")); err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

func TestFormatMessageLineAppendsAttachmentSuffix(t *testing.T) {
	m := Message{From: "alice", Kind: KindDirect, Body: "see this",
		AttachPath: "/tmp/cfg.yaml", AttachHash: "sha256:" + strings.Repeat("a", 64), AttachSize: 2355}
	line := formatMessageLine("12:00:00", m)
	if !strings.Contains(line, "[attached: /tmp/cfg.yaml") {
		t.Fatalf("expected an attachment suffix, got %q", line)
	}
	if strings.Contains(line, strings.Repeat("a", 64)) {
		t.Fatalf("expected the text rendering to truncate the hash, got %q", line)
	}
	if !strings.Contains(line, "sha256:aaaaaaaaaaaa") { // 12 hex chars
		t.Fatalf("expected a 12-char truncated hash, got %q", line)
	}
}

func TestFormatMessageLineOmitsSuffixWithoutAttachment(t *testing.T) {
	m := Message{From: "alice", Kind: KindDirect, Body: "plain message"}
	line := formatMessageLine("12:00:00", m)
	if strings.Contains(line, "[attached:") {
		t.Fatalf("expected no attachment suffix, got %q", line)
	}
}

// An ask root gets a prominent, distinct marker telling the recipient a
// plain reply won't satisfy the asker's wait — this is the actual fix for
// "devops answered via plain send and the asker's `mess ask` timed out
// anyway" (nothing else about the message looked any different).
func TestFormatMessageLinePrependsAskMarker(t *testing.T) {
	m := Message{ID: "m42", From: "alice", Kind: KindDirect, Body: "status?", Ask: true}
	line := formatMessageLine("12:00:00", m)
	if !strings.Contains(line, "[question m42") || !strings.Contains(line, "mess reply") {
		t.Fatalf("expected a prominent question marker naming the token, got %q", line)
	}
}

func TestFormatMessageLineOmitsAskMarkerForOrdinaryMessage(t *testing.T) {
	m := Message{From: "alice", Kind: KindDirect, Body: "just a status update"}
	line := formatMessageLine("12:00:00", m)
	if strings.Contains(line, "[question") {
		t.Fatalf("expected no question marker on an ordinary message, got %q", line)
	}
}

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0B"},
		{500, "500B"},
		{1024, "1.0KB"},
		{2355, "2.3KB"},
		{1024 * 1024, "1.0MB"},
		{1024 * 1024 * 1024, "1.0GB"},
	}
	for _, c := range cases {
		if got := humanBytes(c.n); got != c.want {
			t.Errorf("humanBytes(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

// --- mess replay count parsing ---
//
// Real bug: `mess replay --since 3d` produced "invalid count \"--since\"" —
// replay has no registered --since flag, so parseAnywhere's documented
// "unknown dash-tokens are literal text" behavior left it as a positional,
// which then failed Atoi with a baffling message. mess log DOES support
// --since (against the unbounded journal); replay only ever shows a bounded
// recent window. Fixed by giving that specific failure mode a clear error.

func TestParseReplayCountAcceptsPlainNumber(t *testing.T) {
	n, err := parseReplayCount([]string{"20"})
	if err != nil {
		t.Fatal(err)
	}
	if n != 20 {
		t.Fatalf("expected 20, got %d", n)
	}
}

func TestParseReplayCountDefaultsToZeroWithNoArgs(t *testing.T) {
	n, err := parseReplayCount(nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 (whole history), got %d", n)
	}
}

func TestParseReplayCountErrorsClearlyOnUnsupportedFlag(t *testing.T) {
	_, err := parseReplayCount([]string{"--since", "3d"})
	if err == nil {
		t.Fatal("expected an error for an unsupported flag like --since")
	}
	if !strings.Contains(err.Error(), "mess log") {
		t.Fatalf("expected the error to point at `mess log` for filtered queries, got %q", err.Error())
	}
}

// --- mess reply --thread routing ---

// The empty case is the original bug's fix: an unknown/empty thread must
// error clearly, not silently let the id fall through as literal body text.
func TestRouteFromThreadMessagesErrorsOnEmpty(t *testing.T) {
	_, _, _, err := routeFromThreadMessages("bob", "m99", nil)
	if err == nil {
		t.Fatal("expected an error for an empty/unknown thread")
	}
}

func TestRouteFromThreadMessagesPrefersRootForDirect(t *testing.T) {
	msgs := []Message{
		{ID: "m1", Kind: KindDirect, From: "alice", To: "bob"}, // root
		{ID: "m2", Kind: KindDirect, From: "bob", To: "alice", ThreadID: "m1"},
	}
	kind, topic, to, err := routeFromThreadMessages("bob", "m1", msgs)
	if err != nil {
		t.Fatal(err)
	}
	if kind != KindDirect || topic != "" || to != "alice" {
		t.Fatalf("expected to route back to alice, got kind=%q topic=%q to=%q", kind, topic, to)
	}
}

func TestRouteFromThreadMessagesPrefersRootForTopic(t *testing.T) {
	msgs := []Message{
		{ID: "m1", Kind: KindTopic, Topic: "eng", From: "alice"}, // root
		{ID: "m2", Kind: KindTopic, Topic: "eng", From: "bob", ThreadID: "m1"},
	}
	kind, topic, to, err := routeFromThreadMessages("bob", "m1", msgs)
	if err != nil {
		t.Fatal(err)
	}
	if kind != KindTopic || topic != "eng" || to != "" {
		t.Fatalf("expected to route to #eng, got kind=%q topic=%q to=%q", kind, topic, to)
	}
}

// If the root itself scrolled out of view, fall back to a reply we did see.
func TestRouteFromThreadMessagesFallsBackWithoutRoot(t *testing.T) {
	msgs := []Message{
		{ID: "m2", Kind: KindDirect, From: "alice", To: "bob", ThreadID: "m1"},
	}
	kind, topic, to, err := routeFromThreadMessages("bob", "m1", msgs)
	if err != nil {
		t.Fatal(err)
	}
	if kind != KindDirect || topic != "" || to != "alice" {
		t.Fatalf("expected to route back to alice from the fallback reply, got kind=%q topic=%q to=%q", kind, topic, to)
	}
}

// Fallback must route to the OTHER party, not myself, when I'm the sender
// of the only reply we happen to have in view.
func TestRouteFromThreadMessagesFallbackUsesOtherParty(t *testing.T) {
	msgs := []Message{
		{ID: "m2", Kind: KindDirect, From: "bob", To: "alice", ThreadID: "m1"},
	}
	kind, _, to, err := routeFromThreadMessages("bob", "m1", msgs)
	if err != nil {
		t.Fatal(err)
	}
	if kind != KindDirect || to != "alice" {
		t.Fatalf("expected to route to alice (the other party), got kind=%q to=%q", kind, to)
	}
}

// --- stale open thread warning ---
//
// Root cause of a real incident: an agent had an old open thread (never
// `mess thread close`d) and received a brand new, unrelated ask. A bare
// `mess reply` kept posting into the STALE thread with zero indication
// anything was wrong, so the asker's `mess ask` timed out despite a good
// answer existing — just on the wrong thread.

func TestStaleOpenThreadWarningFiresWhenLastMsgIsADifferentThread(t *testing.T) {
	open := openThreadInfo{ThreadID: "m4320", Kind: KindDirect, To: "alice"}
	last := lastMsgInfo{ID: "m4384", Kind: KindDirect, From: "bob"}
	warn := staleOpenThreadWarning(open, last, true)
	if warn == "" {
		t.Fatal("expected a warning when the open thread differs from the most recently received message")
	}
	if !strings.Contains(warn, "m4320") || !strings.Contains(warn, "m4384") || !strings.Contains(warn, "--thread m4384") {
		t.Fatalf("expected the warning to name both threads and the fix, got %q", warn)
	}
}

func TestStaleOpenThreadWarningSilentWhenSameThread(t *testing.T) {
	open := openThreadInfo{ThreadID: "m1", Kind: KindDirect, To: "alice"}
	last := lastMsgInfo{ID: "m1", Kind: KindDirect, From: "alice"}
	if warn := staleOpenThreadWarning(open, last, true); warn != "" {
		t.Fatalf("expected no warning when continuing the same thread, got %q", warn)
	}
}

func TestStaleOpenThreadWarningSilentWithNoLastMsg(t *testing.T) {
	open := openThreadInfo{ThreadID: "m1", Kind: KindDirect, To: "alice"}
	if warn := staleOpenThreadWarning(open, lastMsgInfo{}, false); warn != "" {
		t.Fatalf("expected no warning with no last-seen message, got %q", warn)
	}
}
