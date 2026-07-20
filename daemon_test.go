package main

import (
	"bytes"
	"log"
	"net"
	"path/filepath"
	"strings"
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

// Two waiters parked on the SAME agent at once is a documented anti-pattern
// ("one receiver per agent" — see CLAUDE.md/skill docs, and mess itself warns
// on stderr if a second waiter starts), not something mess enforces at the
// daemon level. This is the concrete test for what actually happens if it's
// violated anyway (e.g. a manual `mess recv --wait` left running alongside
// the auto-wake hook): every drain path (DrainKinds et al.) is mutex-
// protected, so the real question is whether that protects the OUTCOME too
// — no double-delivery of one message, no message silently dropped between
// two racing wakes, no deadlock/panic. Run repeatedly (not just once) since
// a race, if one existed, wouldn't necessarily show on a single run.
func TestTwoParkedWaitersOnSameAgentNoDoubleDeliveryNoLoss(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
		_, serverA := net.Pipe()
		_, serverB := net.Pipe()

		// A short timeout so the "loser" (the one that doesn't get the
		// message) returns on its own instead of parking forever — with
		// only one message ever sent, exactly one waiter has nothing left
		// to drain.
		req := Request{Op: "recv", As: "bob", Wait: true, Timeout: "300ms"}
		doneA := make(chan Response, 1)
		doneB := make(chan Response, 1)
		go func() { doneA <- d.recv(serverA, req) }()
		go func() { doneB <- d.recv(serverB, req) }()

		listenerCount := func() int {
			d.broker.mu.Lock()
			defer d.broker.mu.Unlock()
			return d.broker.listeners["bob"]
		}
		deadline := time.Now().Add(time.Second)
		for listenerCount() < 2 {
			if time.Now().After(deadline) {
				t.Fatalf("iter %d: both waiters never registered as listening (count=%d)", iter, listenerCount())
			}
			time.Sleep(time.Millisecond)
		}

		d.broker.Send("alice", "bob", "only one copy of this exists")

		var respA, respB Response
		select {
		case respA = <-doneA:
		case <-time.After(2 * time.Second):
			t.Fatalf("iter %d: waiter A never returned (deadlock?)", iter)
		}
		select {
		case respB = <-doneB:
		case <-time.After(2 * time.Second):
			t.Fatalf("iter %d: waiter B never returned (deadlock?)", iter)
		}

		total := len(respA.Messages) + len(respB.Messages)
		if total != 1 {
			t.Fatalf("iter %d: expected exactly 1 message delivered across both waiters, got %d (A=%d B=%d)",
				iter, total, len(respA.Messages), len(respB.Messages))
		}
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
	d.broker.RegisterOwned(agentKey("A", "bob"), "", false)
	d.broker.RegisterOwned(agentKey("B", "bob"), "", false)

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

// req.HostWide (plain `mess broadcast --loud`, as opposed to --loud-room)
// crosses room boundaries; without it a loud broadcast still stays room-scoped.
func TestDispatchBroadcastHostWideCrossesRooms(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	d.broker.Register(agentKey("A", "alice"))
	d.broker.Register(agentKey("B", "carol"))

	resp := d.dispatch(Request{Op: "broadcast", As: agentKey("A", "alice"), Body: "room-scoped loud", Loud: true})
	if resp.Count != 0 {
		t.Fatalf("--loud-room (HostWide false) must not cross rooms, got count %d", resp.Count)
	}

	resp = d.dispatch(Request{Op: "broadcast", As: agentKey("A", "alice"), Body: "host-wide loud", Loud: true, HostWide: true})
	if resp.Count != 1 {
		t.Fatalf("--loud should reach carol in room B, got count %d", resp.Count)
	}
}

func TestDispatchThreadList(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	d.dispatch(Request{Op: "sub", As: "bob", Topic: "eng"})
	d.dispatch(Request{Op: "pub", As: "alice", Topic: "eng", Body: "root message"})

	msgs := d.broker.DrainKinds("bob", true, 0, nil) // peek: leave it queued for thread-list to see too
	if len(msgs) != 1 {
		t.Fatalf("expected bob to have received the root, got %+v", msgs)
	}
	rootID := msgs[0].ID
	d.dispatch(Request{Op: "pub", As: "alice", Topic: "eng", Body: "a reply", ThreadID: rootID})

	resp := d.dispatch(Request{Op: "thread-list", As: "bob"})
	if !resp.OK || len(resp.Threads) != 1 {
		t.Fatalf("expected exactly 1 thread, got %+v", resp)
	}
	th := resp.Threads[0]
	if th.ID != rootID || th.Topic != "eng" || th.Replies != 1 {
		t.Fatalf("unexpected thread summary: %+v", th)
	}
}

// recv --if-idle must stand down (Busy: true, no messages) while the agent
// is busy, and drain normally once it's not — exercised through d.recv, the
// same entry point the CLI's non-blocking path uses.
func TestDispatchRecvIfIdleStandsDownWhenBusy(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	d.broker.Send("alice", "bob", "hello")
	d.broker.SetBusy("bob", time.Minute)

	resp := d.recv(nil, Request{Op: "recv", As: "bob", IfIdle: true})
	if !resp.OK || !resp.Busy || len(resp.Messages) != 0 {
		t.Fatalf("expected a busy stand-down with no messages, got %+v", resp)
	}

	d.broker.ClearBusy("bob")
	resp = d.recv(nil, Request{Op: "recv", As: "bob", IfIdle: true})
	if !resp.OK || resp.Busy || len(resp.Messages) != 1 {
		t.Fatalf("expected the message once idle, got %+v", resp)
	}
}

// --- ask/await ---

// ask (async) creates a threaded message and returns its own id as the
// token, without waiting; await on that token then blocks until a reply
// (a plain threaded send, exactly what `mess reply` issues) arrives.
func TestDispatchAskAsyncThenAwaitBlocks(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	d.broker.RegisterOwned("bob", "", false)
	_, server := net.Pipe()

	askResp := d.askOrAwait(nil, Request{Op: "ask", As: "alice", To: "bob", Body: "status?", Wait: false})
	if !askResp.OK || askResp.ID == "" || len(askResp.Messages) != 0 {
		t.Fatalf("expected an async ask to return a token with no messages, got %+v", askResp)
	}
	token := askResp.ID

	done := make(chan Response, 1)
	go func() { done <- d.askOrAwait(server, Request{Op: "await", As: "alice", ThreadID: token, Wait: true}) }()

	// Wait until the await has registered as a listener, then have bob answer.
	deadline := time.Now().Add(time.Second)
	for !d.broker.IsListening("alice") {
		if time.Now().After(deadline) {
			t.Fatal("await never registered as listening")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := d.broker.SendThreaded("bob", "alice", "all green", token); err != nil {
		t.Fatal(err)
	}

	select {
	case resp := <-done:
		if !resp.OK || len(resp.Messages) != 1 || resp.Messages[0].Body != "all green" {
			t.Fatalf("expected the threaded reply, got %+v", resp)
		}
		if resp.ID != token {
			t.Fatalf("expected Response.ID to echo the token, got %q", resp.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("await did not return after the reply arrived")
	}
}

// A non-blocking await on an already-answered token returns it immediately,
// without parking (mirrors recv's own non-blocking-vs-wait split).
func TestDispatchAwaitNonBlockingReturnsAlreadyAnsweredToken(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	root, err := d.broker.Send("alice", "bob", "question")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.broker.SendThreaded("bob", "alice", "answer", root.ID); err != nil {
		t.Fatal(err)
	}

	resp := d.askOrAwait(nil, Request{Op: "await", As: "alice", ThreadID: root.ID, Wait: false})
	if !resp.OK || len(resp.Messages) != 1 || resp.Messages[0].Body != "answer" {
		t.Fatalf("expected the already-queued reply, got %+v", resp)
	}
}

// await with no token at all is a clear error, not a silent no-op.
func TestDispatchAwaitRequiresToken(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	resp := d.askOrAwait(nil, Request{Op: "await", As: "alice", Wait: false})
	if resp.Error == "" {
		t.Fatal("expected an error for await with no token")
	}
}

// Asking a name that was never registered must fail immediately with a clear
// error — not silently create a phantom agentState (ensure() would do that
// for a plain send/pub, but ask blocks for a reply, so it would otherwise
// hang forever (no --timeout) or waste one waiting for an answer nothing can
// ever send) and not show up as a ghost in `mess ps`.
func TestDispatchAskRejectsUnregisteredRecipient(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	resp := d.askOrAwait(nil, Request{Op: "ask", As: "alice", To: "ghost-bob", Body: "are you there?", Wait: false})
	if resp.Error == "" {
		t.Fatal("expected an error for asking a never-registered name")
	}
	if d.broker.IsRegistered("ghost-bob") {
		t.Fatal("asking an unregistered name must not register it")
	}
	agents, _ := d.broker.Ps("", false)
	for _, a := range agents {
		if a.Name == "ghost-bob" {
			t.Fatalf("asking an unregistered name must not create a ghost ps entry, got %+v", a)
		}
	}
}

// A registered-but-currently-offline recipient is still a valid ask target —
// only a *never-registered* name is rejected.
// A registered recipient who's gone properly offline (no listener, not busy,
// no recent activity) is rejected too — nobody's there to answer.
func TestDispatchAskRejectsRegisteredButOfflineRecipient(t *testing.T) {
	now := time.Unix(0, 0)
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	d.broker.now = func() time.Time { return now }
	d.broker.RegisterOwned("bob", "", false)
	now = now.Add(3 * time.Minute) // past the 2-minute "recently active" window

	resp := d.askOrAwait(nil, Request{Op: "ask", As: "alice", To: "bob", Body: "status?", Wait: false})
	if resp.Error == "" {
		t.Fatalf("expected an error for a registered-but-offline recipient, got %+v", resp)
	}
}

// A registered recipient who's actually online (a live listener, e.g. a
// parked recv --wait) is a valid ask target.
func TestDispatchAskAllowsRegisteredAndOnlineRecipient(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	d.broker.RegisterOwned("bob", "", false)
	d.broker.AddListener("bob") // genuinely online: has a live listener
	resp := d.askOrAwait(nil, Request{Op: "ask", As: "alice", To: "bob", Body: "status?", Wait: false})
	if !resp.OK || resp.ID == "" {
		t.Fatalf("expected ask to succeed for a registered, online recipient, got %+v", resp)
	}
}

// Asking the human operator's mailbox is always allowed, even though "user"
// is never registered via RegisterOwned — it's a reserved handle.
func TestDispatchAskAllowsUserHandleWithoutRegistration(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	resp := d.askOrAwait(nil, Request{Op: "ask", As: "alice", To: "user", Body: "need your input", Wait: false})
	if !resp.OK || resp.ID == "" {
		t.Fatalf("expected ask to the human's mailbox to succeed, got %+v", resp)
	}
}

// --- send: ghost guard ---
//
// Real incident: aphelion-frontend (global room) sent to aphelion-frontend-2
// (a name actually registered in room "frontend") — send silently created a
// same-named-but-disconnected ghost in the global room instead of ever
// reaching the real, already-registered agent. Fixed by extending the same
// "must be a previously-registered recipient" guard ask already had to send.

func TestDispatchSendRejectsNeverRegisteredRecipient(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	resp := d.dispatch(Request{Op: "send", As: "alice", To: "ghost-bob", Body: "hi"})
	if resp.Error == "" {
		t.Fatal("expected an error for sending to a never-registered name")
	}
	if d.broker.IsRegistered("ghost-bob") {
		t.Fatal("sending to an unregistered name must not register it")
	}
	agents, _ := d.broker.Ps("", false)
	for _, a := range agents {
		if a.Name == "ghost-bob" {
			t.Fatalf("sending to an unregistered name must not create a ghost ps entry, got %+v", a)
		}
	}
}

// The specific footgun: bob exists, but in a different room than the
// sender's — this must be rejected with a room-specific hint, not silently
// create a same-named ghost in the sender's own room.
func TestDispatchSendRejectsCrossRoomGhost(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	d.broker.RegisterOwned(agentKey("frontend", "bob"), "", false)

	resp := d.dispatch(Request{Op: "send", As: "alice", To: "bob", Body: "hi"}) // no Room: global
	if resp.Error == "" {
		t.Fatal("expected an error for a name registered in a different room")
	}
	if !strings.Contains(resp.Error, "frontend") {
		t.Fatalf("expected the error to name the other room, got %q", resp.Error)
	}
	if _, ok := d.broker.agents["bob"]; ok {
		t.Fatal("must not create a same-named ghost in the sender's own (global) room")
	}
}

// Explicit --room bypasses the guard entirely — this is the legitimate way
// to reach an agent registered in a different room.
func TestDispatchSendAllowsExplicitRoomTargeting(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	d.broker.RegisterOwned(agentKey("frontend", "bob"), "", false)

	resp := d.dispatch(Request{Op: "send", As: "alice", To: "bob", Room: "frontend", Body: "hi"})
	if !resp.OK {
		t.Fatalf("explicit --room targeting should succeed, got %+v", resp)
	}
	got := d.broker.Drain(agentKey("frontend", "bob"), false, 0)
	if len(got) != 1 || got[0].Body != "hi" {
		t.Fatalf("message should have reached the real frontend-room bob, got %+v", got)
	}
}

// A registered-but-offline recipient is still a valid send target (unlike
// ask) — send doesn't block, so "message waits for them to come back" is
// exactly the intended fire-and-forget behavior.
func TestDispatchSendAllowsRegisteredButOfflineRecipient(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	d.broker.RegisterOwned("bob", "", false)
	resp := d.dispatch(Request{Op: "send", As: "alice", To: "bob", Body: "hi"})
	if !resp.OK {
		t.Fatalf("expected send to a registered-but-offline recipient to succeed, got %+v", resp)
	}
}

// Sending to the human operator's mailbox is always allowed, unregistered
// or not — it's a reserved handle, exempt from the registration guard.
func TestDispatchSendAllowsUserHandleWithoutRegistration(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	resp := d.dispatch(Request{Op: "send", As: "alice", To: "user", Body: "hi"})
	if !resp.OK {
		t.Fatalf("expected send to the human's mailbox to succeed, got %+v", resp)
	}
}

// ask's existing "no such agent" error gains the same room-specific hint
// when the name is registered elsewhere.
func TestDispatchAskCrossRoomHint(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	d.broker.RegisterOwned(agentKey("frontend", "bob"), "", false)

	resp := d.askOrAwait(nil, Request{Op: "ask", As: "alice", To: "bob", Body: "status?", Wait: false})
	if resp.Error == "" {
		t.Fatal("expected an error for a name registered in a different room")
	}
	if !strings.Contains(resp.Error, "frontend") {
		t.Fatalf("expected the error to name the other room, got %q", resp.Error)
	}
}

// --- register/room-join/rename: reject a slash in the name ---

func TestDispatchRegisterRejectsSlashName(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	resp := d.dispatch(Request{Op: "register", As: "game/grok-game"})
	if resp.Error == "" {
		t.Fatal("expected an error registering a name containing '/'")
	}
	if d.broker.IsRegistered("game/grok-game") {
		t.Fatal("a slash-containing name must not be registered")
	}
}

func TestDispatchRoomJoinRejectsSlashName(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	resp := d.dispatch(Request{Op: "room-join", As: "game/grok-game", Room: "game"})
	if resp.Error == "" {
		t.Fatal("expected an error room-joining a name containing '/'")
	}
}

func TestDispatchRenameRejectsSlashName(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	d.broker.RegisterOwned("alice", "", false)
	resp := d.dispatch(Request{Op: "rename", As: "alice", To: "room/alice"})
	if resp.Error == "" {
		t.Fatal("expected an error renaming to a name containing '/'")
	}
}

// --- room-join: identity migration end-to-end through dispatch ---

func TestDispatchRoomJoinMigratesFromPreviousRoom(t *testing.T) {
	d := &daemon{broker: NewBroker(), stop: make(chan struct{})}
	d.broker.RegisterOwned("bob", "", false)
	d.broker.send("peer", "bob", "queued before joining", "", false, nil, false)

	resp := d.dispatch(Request{Op: "room-join", As: "bob", Room: "frontend", FromRoom: ""})
	if !resp.OK {
		t.Fatalf("room-join should succeed: %+v", resp)
	}
	if _, ok := d.broker.agents["bob"]; ok {
		t.Fatal("stale bare-global agent should be gone after joining a room")
	}
	got := d.broker.Drain(agentKey("frontend", "bob"), false, 0)
	if len(got) != 1 || got[0].Body != "queued before joining" {
		t.Fatalf("inbox should have migrated into the room-scoped identity, got %+v", got)
	}
}

// --- log ---

func newTestDaemonWithJournal(t *testing.T) *daemon {
	t.Helper()
	path := filepath.Join(t.TempDir(), "journal.jsonl")
	jw, err := openJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { jw.close() })
	return &daemon{broker: NewBroker(), journal: jw, paths: paths{journal: path}, stop: make(chan struct{})}
}

// send/broadcast/pub all journal what they deliver, and "log" queries it back
// — end-to-end through dispatch, not the journal package directly.
func TestDispatchSendBroadcastPubAllJournal(t *testing.T) {
	d := newTestDaemonWithJournal(t)
	d.broker.RegisterOwned("bob", "", false)
	d.dispatch(Request{Op: "send", As: "alice", To: "bob", Body: "direct hello"})
	d.dispatch(Request{Op: "broadcast", As: "alice", Body: "broadcast hello"})
	d.dispatch(Request{Op: "sub", As: "bob", Topic: "eng"})
	d.dispatch(Request{Op: "pub", As: "alice", Topic: "eng", Body: "pub hello"})

	resp := d.dispatch(Request{Op: "log", As: "alice", All: true})
	if !resp.OK || len(resp.Messages) != 3 {
		t.Fatalf("expected all 3 journaled messages, got %+v", resp)
	}
	bodies := map[string]bool{}
	for _, m := range resp.Messages {
		bodies[m.Body] = true
	}
	for _, want := range []string{"direct hello", "broadcast hello", "pub hello"} {
		if !bodies[want] {
			t.Fatalf("expected %q in the journal, got %+v", want, resp.Messages)
		}
	}
}

// log defaults to the caller's own room, matching Ps/Broadcast.
func TestDispatchLogScopesToCallerRoomByDefault(t *testing.T) {
	d := newTestDaemonWithJournal(t)
	d.broker.RegisterOwned(agentKey("A", "bob"), "", false)
	d.broker.RegisterOwned(agentKey("B", "dave"), "", false)
	d.dispatch(Request{Op: "send", As: "alice", To: "bob", Room: "A", Body: "room A message"})
	d.dispatch(Request{Op: "send", As: "carol", To: "dave", Room: "B", Body: "room B message"})

	resp := d.dispatch(Request{Op: "log", As: "alice", Room: "A"})
	if !resp.OK || len(resp.Messages) != 1 || resp.Messages[0].Body != "room A message" {
		t.Fatalf("expected only room A's message, got %+v", resp)
	}

	resp = d.dispatch(Request{Op: "log", As: "alice", All: true})
	if !resp.OK || len(resp.Messages) != 2 {
		t.Fatalf("expected both rooms with --all, got %+v", resp)
	}
}

// An invalid --since is a clear error, not a silently-empty result.
func TestDispatchLogRejectsInvalidSince(t *testing.T) {
	d := newTestDaemonWithJournal(t)
	resp := d.dispatch(Request{Op: "log", As: "alice", Since: "nonsense"})
	if resp.Error == "" {
		t.Fatal("expected an error for an invalid --since")
	}
}

// --- expire ---

// The core no-silent-loss invariant: an expired message must be durably
// journaled (Event=="expired") before/as part of being dropped, so `mess
// log` can always show what was auto-removed.
func TestDispatchExpireJournalsBeforeDropping(t *testing.T) {
	d := newTestDaemonWithJournal(t)
	now := time.Unix(0, 0)
	d.broker.now = func() time.Time { return now }
	d.broker.Send("alice", "bob", "ancient mail")
	now = now.Add(15 * 24 * time.Hour)

	resp := d.dispatch(Request{Op: "expire", Timeout: (14 * 24 * time.Hour).String()})
	if !resp.OK || resp.Expired != 1 {
		t.Fatalf("expected 1 expired message, got %+v", resp)
	}
	if got := d.broker.Drain("bob", false, 0); len(got) != 0 {
		t.Fatalf("expected the inbox to be empty after expiry, got %+v", got)
	}

	logResp := d.dispatch(Request{Op: "log", As: "alice", All: true})
	if !logResp.OK || len(logResp.Messages) != 1 || logResp.Messages[0].Body != "ancient mail" {
		t.Fatalf("expected the expired message to be journaled and findable via log, got %+v", logResp)
	}
}

// --dry-run (Peek) previews without journaling or dropping anything.
func TestDispatchExpireDryRunDoesNotJournalOrDrop(t *testing.T) {
	d := newTestDaemonWithJournal(t)
	now := time.Unix(0, 0)
	d.broker.now = func() time.Time { return now }
	d.broker.Send("alice", "bob", "ancient mail")
	now = now.Add(15 * 24 * time.Hour)

	resp := d.dispatch(Request{Op: "expire", Timeout: (14 * 24 * time.Hour).String(), Peek: true})
	if !resp.OK || resp.Expired != 1 {
		t.Fatalf("expected a preview count of 1, got %+v", resp)
	}
	if got := d.broker.Drain("bob", false, 0); len(got) != 1 {
		t.Fatalf("dry-run must not drop anything, got %+v", got)
	}
	logResp := d.dispatch(Request{Op: "log", As: "alice", All: true})
	if !logResp.OK || len(logResp.Messages) != 0 {
		t.Fatalf("dry-run must not journal anything, got %+v", logResp)
	}
}

// If the journal write fails, nothing in that batch is committed — resweeping
// later beats a partially-committed, partially-audited drop.
func TestExpireDurablySkipsCommitOnJournalFailure(t *testing.T) {
	d := newTestDaemonWithJournal(t)
	now := time.Unix(0, 0)
	d.broker.now = func() time.Time { return now }
	d.broker.Send("alice", "bob", "ancient mail")
	now = now.Add(15 * 24 * time.Hour)

	if err := d.journal.close(); err != nil { // force the next append to fail
		t.Fatal(err)
	}

	expired := d.expireDurably(14 * 24 * time.Hour)
	if expired != nil {
		t.Fatalf("expected no commit when journaling fails, got %+v", expired)
	}
	if got := d.broker.Drain("bob", false, 0); len(got) != 1 {
		t.Fatalf("message must remain queued when the journal write failed, got %+v", got)
	}
}
