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

// The daemon-level "bridge" op resolves the local side from req.Room (the
// caller's ambient room, already filled by client.go's withRoom before this
// ever reaches dispatch) and relays a publish across it.
func TestDispatchBridgeUsesCallerRoomAsLocalSide(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}

	d.dispatch(Request{Op: "sub", As: "bob", Room: "B", Topic: "ops"})
	resp := d.dispatch(Request{Op: "bridge", As: "alice", Room: "A", Topic: "deploy", RemoteRoom: "B", RemoteTopic: "ops"})
	if !resp.OK || len(resp.Bridges) != 1 {
		t.Fatalf("bridge creation failed: %+v", resp)
	}
	if resp.Bridges[0].ARoom != "A" || resp.Bridges[0].ATopic != "deploy" {
		t.Fatalf("local side should resolve from req.Room: %+v", resp.Bridges[0])
	}

	d.dispatch(Request{Op: "pub", As: "alice", Room: "A", Topic: "deploy", Body: "ship it"})
	got := d.recv(nil, Request{Op: "recv", As: "bob", Room: "B"})
	if len(got.Messages) != 1 || got.Messages[0].Body != "ship it" {
		t.Fatalf("bridge did not relay through dispatch: %+v", got.Messages)
	}
}

// --local-room overriding the caller's actual current room requires --force —
// otherwise any caller could bridge on behalf of a room it isn't even in.
func TestDispatchBridgeLocalRoomOverrideRequiresForce(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}

	resp := d.dispatch(Request{Op: "bridge", As: "alice", Room: "A", LocalRoom: "C", Topic: "deploy", RemoteRoom: "B", RemoteTopic: "ops"})
	if resp.Error == "" {
		t.Fatal("overriding local-room without --force should be refused")
	}
	resp = d.dispatch(Request{Op: "bridge", As: "alice", Room: "A", LocalRoom: "C", Topic: "deploy", RemoteRoom: "B", RemoteTopic: "ops", Force: true})
	if !resp.OK || resp.Bridges[0].ARoom != "C" {
		t.Fatalf("--force should allow the override: %+v", resp)
	}
}

func TestDispatchUnbridgeAndBridgesList(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}

	resp := d.dispatch(Request{Op: "bridge", As: "alice", Room: "A", Topic: "deploy", RemoteRoom: "B", RemoteTopic: "ops"})
	id := resp.Bridges[0].ID

	list := d.dispatch(Request{Op: "bridges"})
	if len(list.Bridges) != 1 || list.Bridges[0].ID != id {
		t.Fatalf("expected the bridge listed: %+v", list.Bridges)
	}

	unb := d.dispatch(Request{Op: "unbridge", As: "alice", BridgeID: id})
	if unb.Count != 1 {
		t.Fatalf("unbridge should succeed: %+v", unb)
	}
	unb = d.dispatch(Request{Op: "unbridge", As: "alice", BridgeID: id})
	if unb.Count != 0 {
		t.Fatalf("second unbridge should be a no-op: %+v", unb)
	}
	list = d.dispatch(Request{Op: "bridges"})
	if len(list.Bridges) != 0 {
		t.Fatalf("bridge should be gone from the list: %+v", list.Bridges)
	}
}

// req.ThreadID flows through dispatch's "pub" case into PubThreaded, and
// "recv" with a ThreadID filters to that thread instead of kind-filtering.
func TestDispatchThreadIDFlowsThroughPubAndRecv(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}

	d.dispatch(Request{Op: "sub", As: "bob", Topic: "eng"})
	root := d.dispatch(Request{Op: "pub", As: "alice", Topic: "eng", Body: "root"})
	if !root.OK {
		t.Fatalf("pub failed: %+v", root)
	}
	// Recover the root's ID via a plain recv, then reply in that thread.
	got := d.recv(nil, Request{Op: "recv", As: "bob"})
	if len(got.Messages) != 1 {
		t.Fatalf("expected the root message, got %+v", got.Messages)
	}
	rootID := got.Messages[0].ID

	d.dispatch(Request{Op: "pub", As: "alice", Topic: "eng", Body: "reply", ThreadID: rootID})
	d.dispatch(Request{Op: "pub", As: "alice", Topic: "eng", Body: "unrelated"})

	threadOnly := d.recv(nil, Request{Op: "recv", As: "bob", ThreadID: rootID, Peek: true})
	if len(threadOnly.Messages) != 1 || threadOnly.Messages[0].Body != "reply" {
		t.Fatalf("expected only the threaded reply, got %+v", threadOnly.Messages)
	}
}

// req.Loud on "broadcast" both marks the delivered Message as Loud (verified
// via the wake trigger, since dispatch doesn't expose Message directly) and
// unconditionally pings the human operator, regardless of @mention.
func TestDispatchBroadcastLoudWakesAndNotifies(t *testing.T) {
	calls := mockNotifySend(t)
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	d.broker.Register("bob")
	noBroadcast := map[string]bool{KindDirect: true, KindTopic: true}

	d.dispatch(Request{Op: "broadcast", As: "alice", Body: "quiet one"})
	if d.broker.HasPending("bob", noBroadcast) {
		t.Fatal("a plain broadcast must not satisfy a --no-broadcast wake trigger")
	}
	if len(*calls) != 0 {
		t.Fatalf("a plain, unmentioning broadcast must not notify the human: %v", *calls)
	}
	d.broker.DrainKinds("bob", false, 0, nil)

	d.dispatch(Request{Op: "broadcast", As: "alice", Body: "loud one", Loud: true})
	if !d.broker.HasPending("bob", noBroadcast) {
		t.Fatal("a --loud broadcast should satisfy the wake trigger even under --no-broadcast")
	}
	if len(*calls) != 1 {
		t.Fatalf("a --loud broadcast should notify the human unconditionally, got %v", *calls)
	}
}
