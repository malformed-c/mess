package main

import (
	"strings"
	"testing"
)

func TestStateKey(t *testing.T) {
	if got := stateKey("a1b2c3d4-e5f6-7890-abcd-ef1234567890"); got != "a1b2c3d4-e5f6-7890-abcd-ef1234567890" {
		t.Fatalf("plain uuid mangled: %q", got)
	}

	if got := stateKey("dsh:session/../../etc"); strings.ContainsAny(got, "/:") {
		t.Fatalf("stateKey kept hostile characters: %q", got)
	}

	got := stateKey("dsh:session-4a2b989a")
	if !strings.HasPrefix(got, "dshsession-") {
		t.Fatalf("stateKey truncated labeled id: %q", got)
	}

	if got := stateKey("a--b"); got != "a-b" {
		t.Fatalf("dash run not collapsed: %q", got)
	}

	if got := stateKey("///"); got != "anon" {
		t.Fatalf("hostile id not made anon: %q", got)
	}

	if got := stateKey(strings.Repeat("x", 300)); len(got) > 120 {
		t.Fatalf("long id not bounded: %d", len(got))
	}
}
