package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Identity resolution order (most to least specific):
//   1. an explicit --as flag
//   2. a mid-session identity registered with `mess register <name>`, persisted
//      in a file keyed by the host agent's session id (survives across turns),
//      with a terminal-anchored fallback that survives a session-id rotation
//   3. the MESS_AGENT environment variable (set at launch)
//
// This lets a session that was started without MESS_AGENT join the network at
// any point, and that choice persists for the rest of the session.

// sessionEnvVars are the per-session id env vars set by supported host agents,
// in priority order. MESS_SESSION_ID is an explicit override for any other host.
//   - CLAUDE_CODE_SESSION_ID — Claude Code
//   - CODEX_THREAD_ID        — OpenAI Codex CLI
var sessionEnvVars = []string{"MESS_SESSION_ID", "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID"}

// anchorEnvVars are stable per-terminal identifiers, in priority order. Unlike a
// session id they survive a resume/relaunch of the host agent in the same
// terminal, so they key a fallback identity that outlives session-id rotation.
// MESS_ANCHOR is an explicit override; the rest are set by common terminals
// (most-granular — per pane/tab — first).
var anchorEnvVars = []string{"MESS_ANCHOR", "TMUX_PANE", "STY", "TERM_SESSION_ID", "KONSOLE_DBUS_SESSION", "WINDOWID"}

// sessionID returns the host agent's session identifier, or "" when run outside
// a recognized agent (e.g. a plain shell).
func sessionID() string {
	return firstEnv(sessionEnvVars)
}

// stableAnchor returns a per-terminal identifier that survives a session-id
// change, or "" when none is available (e.g. headless). Used both as the
// rotation-proof identity fallback and to tell a rotated session (same terminal)
// apart from a genuine name collision (different terminal).
func stableAnchor() string {
	return firstEnv(anchorEnvVars)
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

// anchorPath returns the terminal-anchored fallback identity file, or "" when no
// stable anchor is available. Keyed on a hash of the anchor so any value (dbus
// paths, window ids, ...) maps to a safe filename.
func anchorPath(p paths) string {
	a := stableAnchor()
	if a == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(a))
	return filepath.Join(p.dir, "anchor", hex.EncodeToString(sum[:8]))
}

// readIdentity returns the persisted mid-session identity, or "". It first tries
// the session-id file, then the terminal-anchored fallback — so an identity set
// before the host's session id rotated is still recovered.
func readIdentity(p paths) string {
	if name := readIdentFile(identityPath(p)); name != "" {
		return name
	}
	return readIdentFile(anchorPath(p))
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

// writeIdentity persists a mid-session identity, keyed on the session id and
// (best-effort) on the terminal anchor so it survives a session-id rotation.
func writeIdentity(p paths, name string) error {
	path := identityPath(p)
	if path == "" {
		return fmt.Errorf("no session id (CLAUDE_CODE_SESSION_ID / CODEX_THREAD_ID / MESS_SESSION_ID); cannot persist a mid-session identity (pass --as or set MESS_AGENT instead)")
	}
	if err := writeIdentFile(path, name); err != nil {
		return err
	}
	if ap := anchorPath(p); ap != "" {
		_ = writeIdentFile(ap, name) // best-effort rotation-proof fallback
	}
	return nil
}

func writeIdentFile(path, name string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(name+"\n"), 0o600)
}

// clearIdentity removes this session's persisted identity — both the session-id
// file and the terminal-anchored fallback (the inverse of writeIdentity). An
// absent file is not an error.
func clearIdentity(p paths) error {
	for _, path := range []string{identityPath(p), anchorPath(p)} {
		if path == "" {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
