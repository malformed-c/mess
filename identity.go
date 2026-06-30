package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Identity resolution order (most to least specific):
//   1. an explicit --as flag
//   2. a mid-session identity registered with `mess register <name>`, persisted
//      in a file keyed by the host agent's session id (survives across turns)
//   3. the MESS_AGENT environment variable (set at launch)
//
// This lets a session that was started without MESS_AGENT join the network at
// any point, and that choice persists for the rest of the session.

// sessionEnvVars are the per-session id env vars set by supported host agents,
// in priority order. MESS_SESSION_ID is an explicit override for any other host.
//   - CLAUDE_CODE_SESSION_ID — Claude Code
//   - CODEX_THREAD_ID        — OpenAI Codex CLI
var sessionEnvVars = []string{"MESS_SESSION_ID", "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID"}

// sessionID returns the host agent's session identifier, or "" when run outside
// a recognized agent (e.g. a plain shell).
func sessionID() string {
	for _, env := range sessionEnvVars {
		if v := os.Getenv(env); v != "" {
			return v
		}
	}
	return ""
}

// identityPath returns the file storing this session's mess identity, or "" if
// there's no session id to key on (run outside a supported agent).
func identityPath(p paths) string {
	sid := sessionID()
	if sid == "" {
		return ""
	}
	return filepath.Join(p.dir, "ident", filepath.Base(sid))
}

// readIdentity returns the persisted mid-session identity, or "".
func readIdentity(p paths) string {
	path := identityPath(p)
	if path == "" {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// writeIdentity persists a mid-session identity for the current session.
func writeIdentity(p paths, name string) error {
	path := identityPath(p)
	if path == "" {
		return fmt.Errorf("no session id (CLAUDE_CODE_SESSION_ID / CODEX_THREAD_ID / MESS_SESSION_ID); cannot persist a mid-session identity (pass --as or set MESS_AGENT instead)")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(name+"\n"), 0o600)
}

// clearIdentity removes this session's persisted mid-session identity (the
// inverse of writeIdentity). Absent file is not an error.
func clearIdentity(p paths) error {
	path := identityPath(p)
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
