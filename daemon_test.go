package main

import (
	"bytes"
	"log"
	"net"
	"testing"
	"time"
)

// The dedup logger collapses a run of identical messages into one line with a
// (×N) count, and writes distinct messages verbatim.
func TestEventLogDeduplicates(t *testing.T) {
	var buf bytes.Buffer
	origOut, origFlags := log.Writer(), log.Flags()
	defer func() { log.SetOutput(origOut); log.SetFlags(origFlags) }()
	log.SetOutput(&buf)
	log.SetFlags(0)

	e := &eventLog{}
	e.log("recv x parked")
	e.log("recv x parked")
	e.log("recv x parked") // 3 in a row -> one "(×3)" line
	e.log("send a -> b")   // distinct: flushes the run, then pends
	e.flush()              // flush the trailing single line

	want := "recv x parked (×3)\nsend a -> b\n"
	if got := buf.String(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// A parked recv --wait whose client disconnects must release its listener count,
// not leak it (which would show a false "listening" in ps).
func TestRecvReleasesListenerOnDisconnect(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	client, server := net.Pipe()

	done := make(chan Response, 1)
	go func() { done <- d.recv(server, Request{Op: "recv", As: "bob", Wait: true}) }()

	// Wait until the blocking recv has registered as a listener.
	deadline := time.Now().Add(time.Second)
	for !d.broker.IsListening("bob") {
		if time.Now().After(deadline) {
			t.Fatal("recv never registered as listening")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Client dies.
	client.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recv did not return after client disconnect")
	}
	if d.broker.IsListening("bob") {
		t.Fatal("listener leaked after client disconnect (false 'listening')")
	}
}

// A parked recv --wait must stop (not linger as a ghost listener) when its agent
// is removed or renamed out from under it.
func TestRecvEvictedOnRemove(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	_, server := net.Pipe()

	done := make(chan Response, 1)
	go func() { done <- d.recv(server, Request{Op: "recv", As: "ghost", Wait: true}) }()

	deadline := time.Now().Add(time.Second)
	for !d.broker.IsListening("ghost") {
		if time.Now().After(deadline) {
			t.Fatal("recv never registered as listening")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Removing the agent (rm / rename / cleanup) must evict the parked waiter.
	d.broker.RemoveAgent("ghost")

	select {
	case resp := <-done:
		if resp.Count != 0 { // evicted returns empty so the hook won't wake/re-park
			t.Fatalf("evicted recv should return empty, got %d messages", resp.Count)
		}
	case <-time.After(time.Second):
		t.Fatal("parked recv did not stop after its agent was removed (ghost listener)")
	}
	if d.broker.IsListening("ghost") {
		t.Fatal("listener still present after eviction")
	}
}

// A leading "#" on a topic argument (a natural typo, since topics are always
// *displayed* as #name) must be stripped so `sub #trail` and `pub trail` land on
// the same topic instead of silently creating a distinct "#trail" one.
func TestDispatchStripsLeadingHashFromTopic(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}

	if resp := d.dispatch(Request{Op: "sub", As: "bob", Topic: "#trail"}); !resp.OK {
		t.Fatalf("sub failed: %+v", resp)
	}
	resp := d.dispatch(Request{Op: "pub", As: "alice", Topic: "trail", Body: "hi"})
	if !resp.OK {
		t.Fatalf("pub failed: %+v", resp)
	}
	if resp.Count != 1 {
		t.Fatalf("got %d subscriber(s), want 1 (sub #trail and pub trail should be the same topic)", resp.Count)
	}
	// The "#" strip must happen BEFORE room compositing, or "#trail" and "trail"
	// would composite to two different topic keys within the same room.
	if resp := d.dispatch(Request{Op: "sub", As: "carol", Room: "A", Topic: "#deploy"}); !resp.OK {
		t.Fatalf("sub failed: %+v", resp)
	}
	resp = d.dispatch(Request{Op: "pub", As: "dave", Room: "A", Topic: "deploy", Body: "ship"})
	if resp.Count != 1 {
		t.Fatalf("got %d subscriber(s), want 1 (room-scoped #-strip should still unify)", resp.Count)
	}
}

// --- rooms ---

// Two identical (As, To) sends in different rooms must not cross-deliver —
// the daemon-level composite-key path (agentKey(req.Room, ...)), not just the
// broker's own room-aware methods tested directly in broker_test.go.
func TestDispatchRoomScopesSend(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}

	if resp := d.dispatch(Request{Op: "send", As: "alice", To: "bob", Room: "A", Body: "for room A's bob"}); !resp.OK {
		t.Fatalf("send failed: %+v", resp)
	}
	if resp := d.dispatch(Request{Op: "send", As: "alice", To: "bob", Room: "B", Body: "for room B's bob"}); !resp.OK {
		t.Fatalf("send failed: %+v", resp)
	}
	gotA := d.recv(nil, Request{Op: "recv", As: "bob", Room: "A"})
	if len(gotA.Messages) != 1 || gotA.Messages[0].Body != "for room A's bob" {
		t.Fatalf("room A's bob got wrong mail: %+v", gotA.Messages)
	}
	gotB := d.recv(nil, Request{Op: "recv", As: "bob", Room: "B"})
	if len(gotB.Messages) != 1 || gotB.Messages[0].Body != "for room B's bob" {
		t.Fatalf("room B's bob got wrong mail: %+v", gotB.Messages)
	}
}

// The human mailbox ("user") is a single global handle regardless of the
// sender's room — a room-joined agent's `mess send user "..."` must still land
// in the one shared mailbox, not a room-scoped "A/user" that nobody reads.
func TestDispatchUserHandleIgnoresRoom(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}

	if resp := d.dispatch(Request{Op: "send", As: "alice", Room: "A", To: "user", Body: "hi operator"}); !resp.OK {
		t.Fatalf("send to user failed: %+v", resp)
	}
	// Read it back with NO room set at all — proving it landed in the single
	// global mailbox, not "A\x00user".
	got := d.recv(nil, Request{Op: "recv", As: "user"})
	if len(got.Messages) != 1 || got.Messages[0].Body != "hi operator" {
		t.Fatalf("message did not land in the global user mailbox: %+v", got.Messages)
	}
}

func TestRoomJoinRejectsCollisionUnlessForced(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}

	if resp := d.dispatch(Request{Op: "room-join", As: "admin", Room: "A", Session: "sess1"}); !resp.OK {
		t.Fatalf("first join should succeed: %+v", resp)
	}
	resp := d.dispatch(Request{Op: "room-join", As: "admin", Room: "A", Session: "sess2"})
	if resp.Error == "" {
		t.Fatal("a different live session joining the same room's name should collide")
	}
	if resp := d.dispatch(Request{Op: "room-join", As: "admin", Room: "A", Session: "sess2", Force: true}); !resp.OK {
		t.Fatalf("--force should take it over: %+v", resp)
	}
	// A different ROOM with the same name must never have collided in the first place.
	if resp := d.dispatch(Request{Op: "room-join", As: "admin", Room: "B", Session: "sess3"}); !resp.OK {
		t.Fatalf("a different room's identical name should never collide: %+v", resp)
	}
}

func TestRoomLeaveRevertsToGlobal(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}

	d.dispatch(Request{Op: "room-join", As: "alice", Room: "A"})
	if resp := d.dispatch(Request{Op: "room-leave", As: "alice", Room: "A"}); resp.Count != 1 {
		t.Fatalf("leave should remove the room-scoped registration: %+v", resp)
	}
	agents, _ := d.broker.Ps("A", false)
	if len(agents) != 0 {
		t.Fatalf("room A should be empty after leave: %+v", agents)
	}
	// Idempotent: leaving again (already gone) is not an error.
	if resp := d.dispatch(Request{Op: "room-leave", As: "alice", Room: "A"}); resp.Count != 0 {
		t.Fatalf("second leave should be a no-op, got %+v", resp)
	}
}
