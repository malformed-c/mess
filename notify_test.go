package main

import (
	"os"
	"strings"
	"testing"
)

// TestMain replaces notifySend for the ENTIRE test binary run, not just the
// tests in this file — any test in the package that exercises dispatch/send
// (e.g. a daemon_test.go case sending to "user") would otherwise spawn a real
// notify-send process. A real notify_test.go test that wants to assert on
// calls still overrides notifySend itself (see mockNotifySend); this default
// just no-ops instead of touching the OS.
func TestMain(m *testing.M) {
	notifySend = func(string, string) {}
	os.Exit(m.Run())
}

func TestMatchesTargets(t *testing.T) {
	targets := map[string]bool{"user": true, "engi": true}
	cases := []struct {
		body    string
		wantTok string
		wantHit bool
	}{
		{"hey @user can you check this", "user", true},
		{"@engi ping", "engi", true},
		{"case-insensitive @User works", "User", true}, // matched case-insensitively
		{"and @ENGI too", "ENGI", true},                // login name, any case
		{"no mention here", "", false},
		{"email user@host is not an @-mention", "", false}, // needs a leading @ after start/space
		{"@userland is a different handle", "", false},     // token boundary: not "user"
		{"talk to @alice about it", "", false},             // some other agent
	}
	for _, c := range cases {
		gotTok, gotHit := matchesTargets(c.body, targets)
		if gotHit != c.wantHit || (c.wantHit && gotTok != c.wantTok) {
			t.Errorf("matchesTargets(%q) = (%q,%v), want (%q,%v)", c.body, gotTok, gotHit, c.wantTok, c.wantHit)
		}
	}
}

// The literal "user" handle is always a target regardless of the host login name.
func TestMentionsUserLiteral(t *testing.T) {
	if _, ok := mentionsUser("ping @user please"); !ok {
		t.Fatal("@user should always notify the operator")
	}
	if _, ok := mentionsUser("nothing to see"); ok {
		t.Fatal("no mention should not match")
	}
}

// userNotice pings the human on a direct send to their mailbox handle, or on an
// @mention in the body, and stays quiet for ordinary agent-to-agent traffic.
func TestUserNotice(t *testing.T) {
	if _, ok := userNotice("alice", "user", "here is the report"); !ok {
		t.Fatal("a direct send to @user's mailbox should notify")
	}
	if _, ok := userNotice("alice", "bob", "@user take a look"); !ok {
		t.Fatal("an @user mention (to another agent) should still notify")
	}
	if _, ok := userNotice("alice", "bob", "regular status update"); ok {
		t.Fatal("ordinary agent-to-agent traffic must not notify")
	}
	if _, ok := userNotice("alice", "", "broadcast with no mention"); ok {
		t.Fatal("a broadcast with no mention must not notify")
	}
	// The sender is named in the summary; missing sender degrades gracefully.
	if s, _ := userNotice("", "user", "x"); s == "" {
		t.Fatal("expected a non-empty summary even without a sender")
	}
}

// mockNotifySend swaps in a recording stub for notifySend for the duration of
// the test, so desktopNotify/notifyUser can be exercised end-to-end without
// spawning a real notify-send or depending on one being installed (e.g. a
// headless CI box) — mirrors the injectable-clock pattern used for Broker.now.
func mockNotifySend(t *testing.T) *[]string {
	t.Helper()
	var calls []string
	orig := notifySend
	notifySend = func(summary, body string) {
		calls = append(calls, summary+"|"+body)
	}
	t.Cleanup(func() { notifySend = orig })
	return &calls
}

func TestNotifyUserFiresMockedNotifySend(t *testing.T) {
	calls := mockNotifySend(t)

	notifyUser("alice", "user", "here is the report")
	if len(*calls) != 1 {
		t.Fatalf("expected one notification, got %v", *calls)
	}
	if !strings.Contains((*calls)[0], "alice messaged you") {
		t.Fatalf("unexpected summary: %q", (*calls)[0])
	}
}

func TestNotifyUserStaysQuietForOrdinaryTraffic(t *testing.T) {
	calls := mockNotifySend(t)

	notifyUser("alice", "bob", "regular status update")
	if len(*calls) != 0 {
		t.Fatalf("ordinary agent-to-agent traffic must not notify, got %v", *calls)
	}
}

// MESS_NO_NOTIFY (checked once into notifyEnabled at package init) silences
// notifications entirely, e.g. on a headless daemon host.
func TestNotifyUserRespectsNotifyEnabledFlag(t *testing.T) {
	calls := mockNotifySend(t)
	orig := notifyEnabled
	notifyEnabled = false
	t.Cleanup(func() { notifyEnabled = orig })

	notifyUser("alice", "user", "should be silenced")
	if len(*calls) != 0 {
		t.Fatalf("MESS_NO_NOTIFY should silence notifications, got %v", *calls)
	}
}

// mockKdeconnectBridge swaps in a recording stub for kdeconnectBridge for the
// duration of the test — mirrors mockNotifySend exactly.
func mockKdeconnectBridge(t *testing.T) *[]string {
	t.Helper()
	var calls []string
	orig := kdeconnectBridge
	kdeconnectBridge = func(summary, body string) {
		calls = append(calls, summary+"|"+body)
	}
	t.Cleanup(func() { kdeconnectBridge = orig })
	return &calls
}

// presenceAway defaults to "present" (no bridging) unless MESS_PRESENCE=away —
// a zero-regression-risk default, since desktop-only is today's behavior.
func TestPresenceAwayDefaultsToPresent(t *testing.T) {
	t.Setenv("MESS_PRESENCE", "")
	if presenceAway() {
		t.Fatal("expected present (no bridging) when MESS_PRESENCE is unset")
	}
	t.Setenv("MESS_PRESENCE", "present")
	if presenceAway() {
		t.Fatal("expected present (no bridging) when MESS_PRESENCE=present")
	}
	t.Setenv("MESS_PRESENCE", "away")
	if !presenceAway() {
		t.Fatal("expected away (bridging) when MESS_PRESENCE=away")
	}
	t.Setenv("MESS_PRESENCE", "AWAY") // case-insensitive
	if !presenceAway() {
		t.Fatal("expected MESS_PRESENCE to be matched case-insensitively")
	}
}

// bridgeIfAway only fires the configured channels when presenceAway is true —
// a present (default) human never gets bridged, only desktop-notified.
func TestBridgeIfAwayFiresOnlyWhenAway(t *testing.T) {
	calls := mockKdeconnectBridge(t)
	origPresence := presenceAway
	t.Cleanup(func() { presenceAway = origPresence })

	presenceAway = func() bool { return false }
	bridgeIfAway("summary", "body")
	if len(*calls) != 0 {
		t.Fatalf("must not bridge when present, got %v", *calls)
	}

	presenceAway = func() bool { return true }
	bridgeIfAway("summary", "body")
	if len(*calls) != 1 {
		t.Fatalf("expected exactly one bridge call when away, got %v", *calls)
	}
}

// MESS_NO_BRIDGE (bridgeEnabled) silences bridging even when away, mirroring
// MESS_NO_NOTIFY's kill-switch shape.
func TestBridgeIfAwayRespectsBridgeEnabledFlag(t *testing.T) {
	calls := mockKdeconnectBridge(t)
	origPresence, origEnabled := presenceAway, bridgeEnabled
	t.Cleanup(func() { presenceAway, bridgeEnabled = origPresence, origEnabled })

	presenceAway = func() bool { return true }
	bridgeEnabled = false
	bridgeIfAway("summary", "body")
	if len(*calls) != 0 {
		t.Fatalf("MESS_NO_BRIDGE should silence bridging even when away, got %v", *calls)
	}
}

// notifyUser (the actual call site) fires the bridge too when away, not just
// desktopNotify — end-to-end through the real choke point.
func TestNotifyUserBridgesWhenAway(t *testing.T) {
	mockNotifySend(t)
	bridgeCalls := mockKdeconnectBridge(t)
	origPresence := presenceAway
	presenceAway = func() bool { return true }
	t.Cleanup(func() { presenceAway = origPresence })

	notifyUser("alice", "user", "need your input")
	if len(*bridgeCalls) != 1 {
		t.Fatalf("expected notifyUser to bridge when away, got %v", *bridgeCalls)
	}

	// Ordinary agent-to-agent traffic still must not notify or bridge.
	notifyUser("alice", "bob", "regular status update")
	if len(*bridgeCalls) != 1 {
		t.Fatalf("ordinary traffic must not trigger a bridge, got %v", *bridgeCalls)
	}
}

// kdeconnectBridge must degrade silently (no panic, no error surfaced) when
// kdeconnect-cli isn't installed — the same best-effort contract notifySend
// already has for a missing notify-send.
func TestKdeconnectBridgeDegradesSilentlyWithoutTool(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // guarantee kdeconnect-cli can't be found
	kdeconnectBridge("summary", "body")
}

// desktopNotify truncates a long body before handing it to the notifier, so a
// giant message doesn't blow up the notification popup.
func TestDesktopNotifyTruncatesLongBody(t *testing.T) {
	calls := mockNotifySend(t)

	desktopNotify("summary", strings.Repeat("x", 300))
	if len(*calls) != 1 {
		t.Fatalf("expected one call, got %v", *calls)
	}
	body := strings.SplitN((*calls)[0], "|", 2)[1]
	if !strings.HasSuffix(body, "…") {
		t.Fatalf("expected truncated body to end with an ellipsis, got %q", body)
	}
	if len([]rune(body)) != 201 { // 200 runes + the ellipsis
		t.Fatalf("expected 201 runes (200 + ellipsis), got %d: %q", len([]rune(body)), body)
	}
}
