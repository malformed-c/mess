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
	bridgeIfAway(summary, body)
}

// notifyUserLoud unconditionally pings the human operator regardless of
// whether the message mentions them — for a caller-flagged "make sure a human
// sees this" broadcast (mess broadcast --loud), since a plain broadcast only
// reaches other agents and easily gets missed for something significant
// (e.g. a live daemon restart).
func notifyUserLoud(from, body string) {
	if !notifyEnabled {
		return
	}
	if from == "" {
		from = "someone"
	}
	summary := fmt.Sprintf("mess: %s broadcast (loud)", from)
	desktopNotify(summary, body)
	bridgeIfAway(summary, body)
}

// presenceAway reports whether the human operator is away from their desktop
// right now, i.e. a desktop notification alone likely won't be seen and a
// bridge to another device is worth the noise. There's no reliable signal
// this daemon can observe automatically today (aliveLocked-style presence is
// computed for registered agents, never for the human's own mailbox, and the
// one real OS-level candidate — systemd-logind's per-session idle hint — has
// DE-version-dependent reliability not worth trusting blind) — so v1 is an
// explicit, manual gate: MESS_PRESENCE=away turns bridging on; unset or
// "present" keeps today's desktop-only behavior (the default, so this is a
// zero-regression-risk opt-in). Overridable package var, mirroring notifySend,
// so tests don't depend on the real environment.
var presenceAway = func() bool {
	return strings.EqualFold(os.Getenv("MESS_PRESENCE"), "away")
}

// bridgeEnabled gates the human bridge entirely; MESS_NO_BRIDGE=1 turns it
// off regardless of presence, mirroring MESS_NO_NOTIFY.
var bridgeEnabled = os.Getenv("MESS_NO_BRIDGE") == ""

// bridgeIfAway relays a human-notice-worthy message to the configured bridge
// channel(s) when presenceAway reports the human isn't at their desktop to
// see a plain desktop notification. Best-effort and non-blocking, same
// philosophy as desktopNotify — a bridge failing (or not being configured)
// must never make the triggering `mess send`/`broadcast`/`pub` fail or block.
// Only one channel (kdeconnect) exists today; calls it directly rather than
// through a slice/interface of channels — that's ceremony worth adding once a
// second channel (ntfy/email) actually exists, not before.
func bridgeIfAway(summary, body string) {
	if !bridgeEnabled || !presenceAway() {
		return
	}
	kdeconnectBridge(summary, body)
}

// kdeconnectDevice is the target device name for the kdeconnect bridge,
// overridable via MESS_KDECONNECT_DEVICE for a different phone/host.
func kdeconnectDevice() string {
	if d := os.Getenv("MESS_KDECONNECT_DEVICE"); d != "" {
		return d
	}
	return "engipixel"
}

// kdeconnectBridge pings the configured device via kdeconnect-cli, mirroring
// notifySend's exact pattern: LookPath, non-blocking Start, reaped in a
// goroutine, silent skip if the tool isn't installed or the call fails — a
// missing/unreachable device must never surface as an error to the sender.
// Package var so tests can swap it for a recording stub.
var kdeconnectBridge = func(summary, body string) {
	path, err := exec.LookPath("kdeconnect-cli")
	if err != nil {
		return // kdeconnect not installed — best effort
	}
	msg := summary
	if body != "" {
		msg = summary + ": " + truncate(body, 200)
	}
	cmd := exec.Command(path, "--name", kdeconnectDevice(), "--ping-msg", msg)
	if err := cmd.Start(); err != nil {
		dlog("kdeconnect-cli failed: %v", err)
		return
	}
	go func() { _ = cmd.Wait() }() // reap the child without blocking the daemon
}

// desktopNotify runs notify-send without blocking the daemon. A missing notifier
// or display is skipped; a start error is logged at debug level. Delegates to the
// overridable notifySend so tests can assert on calls without spawning a real
// notifier or depending on one being installed/reachable (e.g. a headless CI box).
func desktopNotify(summary, body string) {
	notifySend(summary, truncate(body, 200))
}

// notifySend is the actual notify-send invocation, factored out as a package
// variable so tests can swap it for a recording stub (see notify_test.go) —
// mirrors the injectable-clock pattern already used for Broker.now.
var notifySend = func(summary, body string) {
	path, err := exec.LookPath("notify-send")
	if err != nil {
		return // no notifier available (e.g. headless) — best effort
	}
	cmd := exec.Command(path, "-a", "mess", "-i", "dialog-information", summary, body)
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
