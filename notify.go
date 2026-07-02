package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"sync"
)

// notifyEnabled gates desktop notifications for messages aimed at the human
// operator. On by default; set MESS_NO_NOTIFY=1 (in the daemon's environment) to
// silence, e.g. on a headless host.
var notifyEnabled = os.Getenv("MESS_NO_NOTIFY") == ""

// userHandles is the set of names that mean "the human operator": the literal
// "user" plus the current login name ($USER / the OS user). Lowercased; computed
// once. A direct `mess send <handle>` reaches the human's mailbox and pings them
// via notify-send, and an @mention of any of these also pings them.
var userHandles = sync.OnceValue(func() map[string]bool {
	handles := map[string]bool{"user": true}
	if u, err := user.Current(); err == nil && u.Username != "" {
		handles[strings.ToLower(u.Username)] = true
	}
	if env := os.Getenv("USER"); env != "" {
		handles[strings.ToLower(env)] = true
	}
	return handles
})

// isUserHandle reports whether name is one of the human operator's mailbox names.
func isUserHandle(name string) bool {
	return userHandles()[strings.ToLower(name)]
}

// mentionsUser reports whether body @-mentions the human operator, returning the
// matched handle.
func mentionsUser(body string) (string, bool) {
	return matchesTargets(body, userHandles())
}

// matchesTargets returns the first @-mention in body whose (lowercased) handle is
// in targets. Split out so it can be tested without depending on the login name.
func matchesTargets(body string, targets map[string]bool) (string, bool) {
	for tok := range mentionsIn(body) {
		if targets[strings.ToLower(tok)] {
			return tok, true
		}
	}
	return "", false
}

// userNotice decides whether a message should ping the human, and with what
// summary. A message pings them if it is sent directly to one of their mailbox
// handles (to), or if its body @-mentions them. Returns ("", false) otherwise.
// Pure (no I/O) so it's unit-testable.
func userNotice(from, to, body string) (summary string, ok bool) {
	if from == "" {
		from = "someone"
	}
	if isUserHandle(to) {
		return fmt.Sprintf("mess: %s messaged you", from), true
	}
	if h, ok := mentionsUser(body); ok {
		return fmt.Sprintf("mess: %s mentioned @%s", from, h), true
	}
	return "", false
}

// notifyUser fires a best-effort desktop notification when a message is aimed at
// the human operator (direct to their mailbox, or an @mention), so they're pinged
// even when no agent is watching. to is "" for broadcast/topic (mention-only).
// Non-blocking; a missing notify-send or display is silently skipped.
func notifyUser(from, to, body string) {
	if !notifyEnabled {
		return
	}
	summary, ok := userNotice(from, to, body)
	if !ok {
		return
	}
	desktopNotify(summary, body)
}

// desktopNotify runs notify-send without blocking the daemon. A missing notifier
// or display is skipped; a start error is logged at debug level.
func desktopNotify(summary, body string) {
	path, err := exec.LookPath("notify-send")
	if err != nil {
		return // no notifier available (e.g. headless) — best effort
	}
	cmd := exec.Command(path, "-a", "mess", "-i", "dialog-information", summary, truncate(body, 200))
	if err := cmd.Start(); err != nil {
		dlog("notify-send failed: %v", err)
		return
	}
	go func() { _ = cmd.Wait() }() // reap the child without blocking the daemon
}

// truncate shortens s to at most n runes, appending an ellipsis when it cuts.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
