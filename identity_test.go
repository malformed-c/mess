package main

import "testing"

func TestIdentityFileRoundTrip(t *testing.T) {
	t.Setenv("MESS_DIR", t.TempDir())
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
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	p := resolvePaths()
	if err := writeIdentity(p, "bob"); err == nil {
		t.Fatal("expected error when no session id is available")
	}
}

func TestAgentNamePrecedence(t *testing.T) {
	t.Setenv("MESS_DIR", t.TempDir())
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
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	t.Setenv("MESS_AGENT", "")
	p := resolvePaths()
	if _, err := agentName(p, ""); err == nil {
		t.Fatal("expected error when no identity is resolvable")
	}
}
