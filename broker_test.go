package main

import (
	"testing"
	"time"
)

func newTestBroker() *Broker {
	b := NewBroker()
	b.now = func() time.Time { return time.Unix(0, 0) }
	return b
}

func TestSendAndDrain(t *testing.T) {
	b := newTestBroker()
	if _, err := b.Send("alice", "bob", "hi"); err != nil {
		t.Fatal(err)
	}
	msgs := b.Drain("bob", false, 0)
	if len(msgs) != 1 || msgs[0].Body != "hi" || msgs[0].From != "alice" {
		t.Fatalf("unexpected messages: %+v", msgs)
	}
	if msgs[0].Kind != KindDirect {
		t.Fatalf("expected direct, got %q", msgs[0].Kind)
	}
	// Drained: inbox now empty.
	if again := b.Drain("bob", false, 0); len(again) != 0 {
		t.Fatalf("expected empty after drain, got %+v", again)
	}
}

func TestSendRequiresRecipient(t *testing.T) {
	b := newTestBroker()
	if _, err := b.Send("alice", "", "hi"); err == nil {
		t.Fatal("expected error for empty recipient")
	}
}

func TestPeekDoesNotConsume(t *testing.T) {
	b := newTestBroker()
	b.Send("a", "b", "one")
	if got := b.Drain("b", true, 0); len(got) != 1 {
		t.Fatalf("peek got %d", len(got))
	}
	if got := b.Drain("b", false, 0); len(got) != 1 {
		t.Fatalf("after peek expected 1, got %d", len(got))
	}
}

func TestDrainMax(t *testing.T) {
	b := newTestBroker()
	for _, body := range []string{"1", "2", "3"} {
		b.Send("a", "b", body)
	}
	first := b.Drain("b", false, 2)
	if len(first) != 2 || first[0].Body != "1" || first[1].Body != "2" {
		t.Fatalf("unexpected first batch: %+v", first)
	}
	rest := b.Drain("b", false, 0)
	if len(rest) != 1 || rest[0].Body != "3" {
		t.Fatalf("unexpected rest: %+v", rest)
	}
}

func TestBroadcastExcludesSender(t *testing.T) {
	b := newTestBroker()
	b.Register("alice")
	b.Register("bob")
	b.Register("carol")
	_, n := b.Broadcast("alice", "hello all")
	if n != 2 {
		t.Fatalf("expected 2 recipients, got %d", n)
	}
	if got := b.Drain("alice", false, 0); len(got) != 0 {
		t.Fatalf("sender should not receive own broadcast: %+v", got)
	}
	if got := b.Drain("bob", false, 0); len(got) != 1 || got[0].Kind != KindBroadcast {
		t.Fatalf("bob should have broadcast: %+v", got)
	}
}

func TestTopicPubSub(t *testing.T) {
	b := newTestBroker()
	b.Sub("bob", "builds")
	b.Sub("carol", "builds")
	b.Sub("alice", "builds")
	_, n := b.Pub("alice", "builds", "green")
	if n != 2 { // alice is a subscriber but also the sender; excluded
		t.Fatalf("expected 2 deliveries, got %d", n)
	}
	got := b.Drain("bob", false, 0)
	if len(got) != 1 || got[0].Topic != "builds" || got[0].Kind != KindTopic {
		t.Fatalf("unexpected topic message: %+v", got)
	}
}

func TestUnsubStopsDelivery(t *testing.T) {
	b := newTestBroker()
	b.Sub("bob", "builds")
	b.Unsub("bob", "builds")
	_, n := b.Pub("alice", "builds", "green")
	if n != 0 {
		t.Fatalf("expected no deliveries after unsub, got %d", n)
	}
	_, topics := b.Ps()
	if len(topics) != 0 {
		t.Fatalf("empty topic should be removed: %+v", topics)
	}
}

func TestWaitChanFiresOnDelivery(t *testing.T) {
	b := newTestBroker()
	ch := b.waitChan("bob", nil)
	select {
	case <-ch:
		t.Fatal("waiter fired before any delivery")
	default:
	}
	b.Send("alice", "bob", "ping")
	select {
	case <-ch:
		// good
	case <-time.After(time.Second):
		t.Fatal("waiter did not fire after delivery")
	}
}

func TestWaitChanFiresImmediatelyIfPending(t *testing.T) {
	b := newTestBroker()
	b.Send("alice", "bob", "already here")
	ch := b.waitChan("bob", nil)
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected immediate fire when inbox non-empty")
	}
}

func TestAckFiresAutomaticallyOnRead(t *testing.T) {
	b := newTestBroker()
	_, ackCh, err := b.SendAck("alice", "bob", "did you see this?")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ackCh:
		t.Fatal("acked before the recipient read it")
	default:
	}
	// A plain recv (no special ack step) must trigger the receipt.
	b.Drain("bob", false, 0)
	select {
	case <-ackCh:
	case <-time.After(time.Second):
		t.Fatal("ack did not fire after the message was read")
	}
}

func TestPeekDoesNotAck(t *testing.T) {
	b := newTestBroker()
	_, ackCh, _ := b.SendAck("alice", "bob", "hi")
	b.Drain("bob", true, 0) // peek: not a read
	select {
	case <-ackCh:
		t.Fatal("peek should not ack")
	default:
	}
}

func TestPlainSendHasNoAckChannel(t *testing.T) {
	b := newTestBroker()
	if _, err := b.Send("alice", "bob", "fire and forget"); err != nil {
		t.Fatal(err)
	}
	if len(b.pendingAcks) != 0 {
		t.Fatalf("plain send should not register an ack: %v", b.pendingAcks)
	}
}

func TestCancelAckPreventsSignal(t *testing.T) {
	b := newTestBroker()
	m, ackCh, _ := b.SendAck("alice", "bob", "timed out")
	b.CancelAck(m.ID) // sender gave up
	b.Drain("bob", false, 0)
	select {
	case <-ackCh:
		t.Fatal("cancelled ack should not fire")
	default:
	}
}

func TestDrainKindsFiltersAndPreserves(t *testing.T) {
	b := newTestBroker()
	b.Register("bob")
	b.Send("alice", "bob", "direct one")
	b.Broadcast("alice", "shout") // bob is registered, receives it
	b.Send("alice", "bob", "direct two")
	// bob now has: direct, broadcast, direct (broadcast excludes sender alice)

	// Drain only direct: the broadcast stays put.
	got := b.DrainKinds("bob", false, 0, map[string]bool{KindDirect: true})
	if len(got) != 2 || got[0].Body != "direct one" || got[1].Body != "direct two" {
		t.Fatalf("expected the two directs, got %+v", got)
	}
	// The broadcast is still queued and readable.
	rest := b.Drain("bob", false, 0)
	if len(rest) != 1 || rest[0].Kind != KindBroadcast {
		t.Fatalf("broadcast should remain after filtered drain: %+v", rest)
	}
}

func TestHasPendingTriggerAndDrainAll(t *testing.T) {
	b := newTestBroker()
	b.Register("bob")
	directOnly := map[string]bool{KindDirect: true, KindTopic: true} // --no-broadcast

	// A broadcast alone must NOT satisfy a direct/topic wake trigger.
	b.Broadcast("alice", "fyi")
	if b.HasPending("bob", directOnly) {
		t.Fatal("a broadcast should not trigger a --no-broadcast waiter")
	}
	if !b.HasPending("bob", nil) {
		t.Fatal("broadcast should still count as pending for an unfiltered check")
	}

	// A direct message triggers the wake...
	b.Send("alice", "bob", "do this")
	if !b.HasPending("bob", directOnly) {
		t.Fatal("a direct message should trigger the waiter")
	}
	// ...and draining all (nil) consumes the broadcast too — nothing left behind.
	got := b.DrainKinds("bob", false, 0, nil)
	if len(got) != 2 {
		t.Fatalf("wake should drain all queued messages, got %d: %+v", len(got), got)
	}
	if b.HasPending("bob", nil) {
		t.Fatal("inbox should be empty after draining all")
	}
}

func TestPsReportsOldestPending(t *testing.T) {
	b := newTestBroker() // clock fixed at time.Unix(0,0)
	b.Register("bob")
	find := func() AgentInfo {
		agents, _ := b.Ps()
		for _, a := range agents {
			if a.Name == "bob" {
				return a
			}
		}
		t.Fatal("bob not found")
		return AgentInfo{}
	}
	if !find().Oldest.IsZero() {
		t.Fatal("no pending -> Oldest should be zero")
	}
	b.Send("alice", "bob", "first")
	got := find()
	if got.Pending != 1 || got.Oldest.IsZero() || !got.Oldest.Equal(b.now()) {
		t.Fatalf("expected Oldest = first message time, got %+v", got)
	}
}

func TestBusyStatusAndExpiry(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.Register("bob")
	working := func() bool {
		agents, _ := b.Ps()
		for _, a := range agents {
			if a.Name == "bob" {
				return a.Working
			}
		}
		return false
	}
	if working() {
		t.Fatal("not busy initially")
	}
	b.SetBusy("bob", time.Minute)
	if !working() {
		t.Fatal("should be working after SetBusy")
	}
	// Clears explicitly.
	b.ClearBusy("bob")
	if working() {
		t.Fatal("should not be working after ClearBusy")
	}
	// And expires on its own (crash backstop).
	b.SetBusy("bob", time.Minute)
	now = now.Add(2 * time.Minute)
	if working() {
		t.Fatal("busy should expire after its TTL")
	}
}

func TestListenerTracking(t *testing.T) {
	b := newTestBroker()
	if b.IsListening("alice") {
		t.Fatal("no listener yet")
	}
	b.AddListener("alice")
	b.AddListener("alice") // two concurrent listeners
	if !b.IsListening("alice") {
		t.Fatal("expected listening after AddListener")
	}
	// agent becomes known so it shows in ps and gets broadcasts
	agents, _ := b.Ps()
	if len(agents) != 1 || !agents[0].Listening {
		t.Fatalf("ps should report alice listening: %+v", agents)
	}
	b.RemoveListener("alice")
	if !b.IsListening("alice") {
		t.Fatal("still one listener left")
	}
	b.RemoveListener("alice")
	if b.IsListening("alice") {
		t.Fatal("expected not listening after all removed")
	}
}

func TestStatReportsPendingAndListening(t *testing.T) {
	b := newTestBroker()
	if p, l := b.Stat("bob"); p != 0 || l {
		t.Fatalf("unknown agent: want 0/false, got %d/%v", p, l)
	}
	b.Send("alice", "bob", "one")
	b.Send("alice", "bob", "two")
	if p, l := b.Stat("bob"); p != 2 || l {
		t.Fatalf("after 2 sends: want 2/false, got %d/%v", p, l)
	}
	b.AddListener("bob")
	if p, l := b.Stat("bob"); p != 2 || !l {
		t.Fatalf("with listener: want 2/true, got %d/%v", p, l)
	}
	b.RemoveListener("bob")
	if _, l := b.Stat("bob"); l {
		t.Fatal("listener removed: want false")
	}
}

func TestPersistenceRoundTrip(t *testing.T) {
	b := newTestBroker()
	b.Send("alice", "bob", "keep me")
	b.Sub("bob", "builds")
	snap := b.snapshot()

	b2 := newTestBroker()
	b2.load(snap)
	if got := b2.Drain("bob", false, 0); len(got) != 1 || got[0].Body != "keep me" {
		t.Fatalf("inbox not restored: %+v", got)
	}
	// seq should be preserved so IDs don't collide after reload.
	if b2.seq != b.seq {
		t.Fatalf("seq not restored: %d vs %d", b2.seq, b.seq)
	}
	_, topics := b2.Ps()
	if len(topics) != 1 || topics[0].Name != "builds" {
		t.Fatalf("topics not restored: %+v", topics)
	}
}

func TestOnChangeFires(t *testing.T) {
	b := newTestBroker()
	calls := 0
	b.onChange = func(snapshot) { calls++ }
	b.Send("a", "b", "x")
	b.Drain("b", false, 0)
	if calls < 2 { // one for send, one for consuming drain
		t.Fatalf("expected onChange to fire on mutations, got %d", calls)
	}
}
