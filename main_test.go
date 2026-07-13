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
	if !strings.Contains(line, "[ask m42") || !strings.Contains(line, "mess reply") {
		t.Fatalf("expected a prominent ask marker naming the token, got %q", line)
	}
}

func TestFormatMessageLineOmitsAskMarkerForOrdinaryMessage(t *testing.T) {
	m := Message{From: "alice", Kind: KindDirect, Body: "just a status update"}
	line := formatMessageLine("12:00:00", m)
	if strings.Contains(line, "[ask") {
		t.Fatalf("expected no ask marker on an ordinary message, got %q", line)
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
