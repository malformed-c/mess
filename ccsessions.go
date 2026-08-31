package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Claude Code's own session registry, read so `mess ps` can show one machine
// instead of two overlapping halves.
//
// Claude Code records every live session as ~/.claude/sessions/<pid>.json and,
// when cross-session messaging is available to it, a socket alongside it. That
// registry is keyed by pid and knows nothing about mess names; mess is keyed by
// name and records the owning pid. The pid is the join, so neither side needs
// to learn the other's naming scheme.
//
// The two rosters genuinely differ, which is the reason to show both: a session
// started before cross-session messaging was enabled has a session file but no
// socket, so it is unreachable by SendMessage while being perfectly reachable by
// mess. Reporting "has a socket" is therefore reporting deliverability, not
// decoration.
//
// This is a best-effort read of another tool's private files: every failure
// degrades to "no Claude Code sessions known" rather than an error, so mess ps
// keeps working if the layout changes or the directory isn't there.

// ccProcessComm resolves a pid to its executable name. Indirected so tests can
// describe a fleet without spawning processes named "claude".
var ccProcessComm = processComm

// ccSession is one live Claude Code session.
type ccSession struct {
	PID    int
	Name   string // the name peers address it by (cwd-derived unless overridden)
	Status string // idle | busy, as Claude Code last recorded it
	CWD    string
	Socket string // cross-session inbox socket; "" means unreachable by SendMessage
}

// Addressable reports whether a peer could reach this session with SendMessage.
func (c ccSession) Addressable() bool { return c.Socket != "" }

// ccSessionsDir is where Claude Code keeps its per-pid session records.
func ccSessionsDir() string {
	if dir := os.Getenv("CLAUDE_CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, "sessions")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".claude", "sessions")
}

// ccSessions returns the live Claude Code sessions, newest name order. Records
// whose process is gone are skipped: Claude Code removes them on clean exit, so
// a leftover file means a crashed session, and reporting it would recreate
// exactly the stale-presence problem mess already fixed for its own roster.
func ccSessions() []ccSession {
	dir := ccSessionsDir()
	if dir == "" {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []ccSession
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSuffix(name, ".json"))
		if err != nil || pid <= 0 {
			continue
		}
		if !hostSessionComms[ccProcessComm(pid)] {
			continue // exited, or the pid was recycled by something else
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		var rec struct {
			PID                 int    `json:"pid"`
			Name                string `json:"name"`
			Status              string `json:"status"`
			CWD                 string `json:"cwd"`
			MessagingSocketPath string `json:"messagingSocketPath"`
		}
		if json.Unmarshal(b, &rec) != nil {
			continue
		}
		s := ccSession{PID: pid, Name: rec.Name, Status: rec.Status, CWD: rec.CWD}
		// The path is recorded at startup; the socket is what actually exists.
		if rec.MessagingSocketPath != "" {
			if _, err := os.Stat(rec.MessagingSocketPath); err == nil {
				s.Socket = rec.MessagingSocketPath
			}
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].PID < out[j].PID
	})
	return out
}

// ccAnnotation renders the roster suffix for a mess agent whose session Claude
// Code also knows about.
//
// The name is echoed only when the two systems disagree: after setting
// CLAUDE_CODE_SESSION_NAME to the mess identity they agree, and printing the
// same word twice would be noise. "no socket" is called out because it is the
// one case that changes what a peer can do — the session is alive and reachable
// by mess, but SendMessage cannot deliver to it.
func ccAnnotation(agentName string, s ccSession) string {
	switch {
	case !s.Addressable():
		return "  [cc: no socket]"
	case s.Name != "" && s.Name != agentName:
		return "  [cc: " + s.Name + "]"
	default:
		return "  [cc]"
	}
}

// compactHome shortens a path under $HOME to ~/... so a roster line stays
// readable when several sessions sit deep in the same tree.
func compactHome(path string) string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return path
	}
	if path == home {
		return "~"
	}
	if rest, ok := strings.CutPrefix(path, home+string(filepath.Separator)); ok {
		return "~/" + rest
	}
	return path
}

// byPID indexes sessions for joining against a mess roster.
func ccByPID(sessions []ccSession) map[int]ccSession {
	m := make(map[int]ccSession, len(sessions))
	for _, s := range sessions {
		m[s.PID] = s
	}
	return m
}
