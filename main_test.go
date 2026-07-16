package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
