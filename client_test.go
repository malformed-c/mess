package main

import "testing"

// --- withRoom / --global ---
//
// Room=="" is ambiguous on its own: it could mean "target the global room"
// or "no override, use ambient." --global (Request.Global) disambiguates it
// — this is what lets `mess send bob --global` reach the global room even
// when the caller has joined one, which a bare Room=="" alone can't do
// (townlife hit this directly: their planned `--room ''` silently fell back
// to ambient instead of forcing global).

func TestWithRoomFillsAmbientRoomByDefault(t *testing.T) {
	p := paths{dir: t.TempDir()}
	t.Setenv("MESS_ROOM", "ambient-room")
	req := withRoom(p, Request{})
	if req.Room != "ambient-room" {
		t.Fatalf("expected the ambient room to be filled in, got %q", req.Room)
	}
}

func TestWithRoomGlobalSkipsAmbientFill(t *testing.T) {
	p := paths{dir: t.TempDir()}
	t.Setenv("MESS_ROOM", "ambient-room")
	req := withRoom(p, Request{Global: true})
	if req.Room != "" {
		t.Fatalf("expected --global to force Room empty despite an ambient room, got %q", req.Room)
	}
}

func TestWithRoomExplicitRoomWins(t *testing.T) {
	p := paths{dir: t.TempDir()}
	t.Setenv("MESS_ROOM", "ambient-room")
	req := withRoom(p, Request{Room: "explicit-room"})
	if req.Room != "explicit-room" {
		t.Fatalf("expected the explicit room to be preserved, got %q", req.Room)
	}
}
