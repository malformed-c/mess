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
//      in a file keyed by CLAUDE_CODE_SESSION_ID (survives across turns)
//   3. the MESS_AGENT environment variable (set at launch)
//
// This lets a session that was started without MESS_AGENT join the network at
// any point, and that choice persists for the rest of the session.

// identityPath returns the file storing this Claude Code session's mess
// identity, or "" if there's no session id to key on (run outside Claude Code).
func identityPath(p paths) string {
	sid := os.Getenv("CLAUDE_CODE_SESSION_ID")
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
		return fmt.Errorf("no CLAUDE_CODE_SESSION_ID; cannot register a mid-session identity (pass --as or set MESS_AGENT instead)")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(name+"\n"), 0o600)
}
