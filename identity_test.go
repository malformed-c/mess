package main

import "testing"

// clearSessionEnv blanks every recognized session-id env var so a test isn't
// affected by whichever host agent (Claude Code, Codex, ...) is actually running
// it, then a test can set just the one it cares about.
func clearSessionEnv(t *testing.T) {
	for _, env := range sessionEnvVars {
		t.Setenv(env, "")
	}
}

func TestIdentityFileRoundTrip(t *testing.T) {
	t.Setenv("MESS_DIR", t.TempDir())
	clearSessionEnv(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "sess-123")
	p := resolvePaths()

	if got := readIdentity(p); got != "" {
		t.Fatalf("expected no identity initially, got %q", got)
	}
	if err := writeIdentity(p, "alice"); err != nil {
		t.Fatal(err)
	}
	if got := readIdentity(p); got != "alice" {
		t.Fatalf("expected alice, got %q", got)
	}
}

func TestIdentityRequiresSessionID(t *testing.T) {
	t.Setenv("MESS_DIR", t.TempDir())
	clearSessionEnv(t)
	p := resolvePaths()
	if err := writeIdentity(p, "bob"); err == nil {
		t.Fatal("expected error when no session id is available")
	}
}

// A Codex session (CODEX_THREAD_ID) gets a persistent identity just like Claude
// Code, and MESS_SESSION_ID overrides any host's id.
func TestSessionIDSupportsCodexAndOverride(t *testing.T) {
	t.Setenv("MESS_DIR", t.TempDir())
	clearSessionEnv(t)
	t.Setenv("CODEX_THREAD_ID", "codex-thread-1")
	p := resolvePaths()
	if err := writeIdentity(p, "cx"); err != nil {
		t.Fatal(err)
	}
	if got := readIdentity(p); got != "cx" {
		t.Fatalf("codex identity not persisted, got %q", got)
	}
	// MESS_SESSION_ID takes priority, so it keys a *different* identity file.
	t.Setenv("MESS_SESSION_ID", "override-9")
	if got := readIdentity(p); got != "" {
		t.Fatalf("override should key a fresh (empty) identity, got %q", got)
	}
}

func TestAgentNamePrecedence(t *testing.T) {
	t.Setenv("MESS_DIR", t.TempDir())
	clearSessionEnv(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "sess-abc")
	t.Setenv("MESS_AGENT", "from-env")
	p := resolvePaths()

	// Env only.
	if got, _ := agentName(p, ""); got != "from-env" {
		t.Fatalf("expected env identity, got %q", got)
	}
	// Mid-session registration beats env.
	if err := writeIdentity(p, "from-file"); err != nil {
		t.Fatal(err)
	}
	if got, _ := agentName(p, ""); got != "from-file" {
		t.Fatalf("expected file identity to win over env, got %q", got)
	}
	// Explicit flag beats everything.
	if got, _ := agentName(p, "from-flag"); got != "from-flag" {
		t.Fatalf("expected flag identity to win, got %q", got)
	}
}

func TestAgentNameNoneSet(t *testing.T) {
	t.Setenv("MESS_DIR", t.TempDir())
	clearSessionEnv(t)
	t.Setenv("MESS_AGENT", "")
	p := resolvePaths()
	if _, err := agentName(p, ""); err == nil {
		t.Fatal("expected error when no identity is resolvable")
	}
}
