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
