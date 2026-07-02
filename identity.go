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
//      in a file keyed by the host agent's session id (survives across turns,
//      compaction, and resume — the session id is stable for the whole session)
//   3. the MESS_AGENT environment variable (set at launch)
//
// This lets a session that was started without MESS_AGENT join the network at
// any point, and that choice persists for the rest of the session.
//
// There is deliberately no terminal-anchored fallback. A host session id
// (CLAUDE_CODE_SESSION_ID / CODEX_THREAD_ID) is stable for the entire life of a
// session — it does not change across /compact, --continue, or --resume — and a
// brand-new session always gets a fresh unique id. A per-terminal fallback keyed
// on the tty/pane/window therefore buys nothing on a rotation (there is none)
// while actively causing harm: a new session launched in a terminal a prior
// agent used — or a terminal id recycled to a new tab (e.g. Konsole reusing
// /Sessions/N) — would inherit the prior occupant's name. Keying on the session
// id alone is both sufficient and correct.

// sessionEnvVars are the per-session id env vars set by supported host agents,
// in priority order. MESS_SESSION_ID is an explicit override for any other host.
//   - CLAUDE_CODE_SESSION_ID — Claude Code
//   - CODEX_THREAD_ID        — OpenAI Codex CLI
var sessionEnvVars = []string{"MESS_SESSION_ID", "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID"}

// sessionID returns the host agent's session identifier, or "" when run outside
// a recognized agent (e.g. a plain shell). It is the sole key for identity: it is
// stable for a session's whole life and unique per session, so it neither leaks a
// name to a new session nor loses one across turns/compaction/resume.
func sessionID() string {
	return firstEnv(sessionEnvVars)
}

func firstEnv(names []string) string {
	for _, env := range names {
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

// readIdentity returns the persisted mid-session identity for this session id, or
// "". Keyed solely on the (stable) host session id, so it never leaks to a
// different session sharing the same terminal.
func readIdentity(p paths) string {
	return readIdentFile(identityPath(p))
}

func readIdentFile(path string) string {
	if path == "" {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// writeIdentity persists a mid-session identity, keyed on the session id.
func writeIdentity(p paths, name string) error {
	path := identityPath(p)
	if path == "" {
		return fmt.Errorf("no session id (CLAUDE_CODE_SESSION_ID / CODEX_THREAD_ID / MESS_SESSION_ID); cannot persist a mid-session identity (pass --as or set MESS_AGENT instead)")
	}
	return writeIdentFile(path, name)
}

func writeIdentFile(path, name string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(name+"\n"), 0o600)
}

// clearIdentity removes this session's persisted identity (the inverse of
// writeIdentity). An absent file is not an error.
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
