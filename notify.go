package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"sync"
)

// notifyEnabled gates desktop notifications for messages that @-mention the human
// operator. On by default; set MESS_NO_NOTIFY=1 (in the daemon's environment) to
// silence, e.g. on a headless host.
var notifyEnabled = os.Getenv("MESS_NO_NOTIFY") == ""

// userMentionTargets is the set of @-handles that mean "the human operator": the
// literal "user" plus the current login name ($USER / the OS user). Lowercased;
// computed once.
var userMentionTargets = sync.OnceValue(func() map[string]bool {
	targets := map[string]bool{"user": true}
	if u, err := user.Current(); err == nil && u.Username != "" {
		targets[strings.ToLower(u.Username)] = true
	}
	if env := os.Getenv("USER"); env != "" {
		targets[strings.ToLower(env)] = true
	}
	return targets
})

// mentionsUser reports whether body @-mentions the human operator (@user or the
// current login name), case-insensitively, returning the matched handle.
func mentionsUser(body string) (string, bool) {
	return matchesTargets(body, userMentionTargets())
}

// matchesTargets returns the first @-mention in body whose (lowercased) handle is
// in targets. Split out from mentionsUser so it can be tested without depending
// on the host's login name.
func matchesTargets(body string, targets map[string]bool) (string, bool) {
	for tok := range mentionsIn(body) {
		if targets[strings.ToLower(tok)] {
			return tok, true
		}
	}
	return "", false
}

// notifyUserMention fires a best-effort desktop notification (notify-send) when a
// message body @-mentions the human operator, so the user is pinged even when no
// agent is watching. Non-blocking; a missing notify-send or display is silently
// skipped (a start error is logged at debug level).
func notifyUserMention(from, body string) {
	if !notifyEnabled {
		return
	}
	handle, ok := mentionsUser(body)
	if !ok {
		return
	}
	path, err := exec.LookPath("notify-send")
	if err != nil {
		return // no notifier available (e.g. headless) — best effort
	}
	summary := fmt.Sprintf("mess: %s mentioned @%s", from, handle)
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
