package main

import "testing"

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
