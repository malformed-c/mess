package main

import (
	"encoding/json"
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
	_, n := b.Broadcast("alice", "hello all", false, false)
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

// A plain broadcast doesn't satisfy a wake trigger that's filtered out
// KindBroadcast (the standard auto-wake hook parks with --no-broadcast) --
// --loud (Message.Loud) is the override that bypasses the kind filter.
func TestLoudBroadcastBypassesKindFilter(t *testing.T) {
	b := newTestBroker()
	b.Register("bob")
	noBroadcast := map[string]bool{KindDirect: true, KindTopic: true} // --no-broadcast

	b.Broadcast("alice", "quiet to a --no-broadcast waiter", false, false)
	if b.HasPending("bob", noBroadcast) {
		t.Fatal("a plain broadcast must not satisfy a --no-broadcast wake trigger")
	}
	b.Drain("bob", false, 0)

	b.Broadcast("alice", "loud, should wake anyway", true, false)
	if !b.HasPending("bob", noBroadcast) {
		t.Fatal("a --loud broadcast should satisfy the wake trigger even under --no-broadcast")
	}
	ch := b.waitChan("bob", noBroadcast)
	select {
	case <-ch:
		// expected: fires immediately since a loud broadcast is already pending
	default:
		t.Fatal("waitChan should fire immediately for a pending loud broadcast")
	}
}

func TestTopicPubSub(t *testing.T) {
	b := newTestBroker()
	b.Sub("bob", "builds")
	b.Sub("carol", "builds")
	b.Sub("alice", "builds")
	_, n, _ := b.Pub("alice", "builds", "green")
	if n != 2 { // alice is a subscriber but also the sender; excluded
		t.Fatalf("expected 2 deliveries, got %d", n)
	}
	got := b.Drain("bob", false, 0)
	if len(got) != 1 || got[0].Topic != "builds" || got[0].Kind != KindTopic {
		t.Fatalf("unexpected topic message: %+v", got)
	}
}

func TestTopicMentionWakesOnlyMentioned(t *testing.T) {
	b := newTestBroker()
	b.Sub("bob", "work")
	b.Sub("carol", "work")
	bobCh := b.waitChan("bob", nil)
	carolCh := b.waitChan("carol", nil)

	_, delivered, woke := b.Pub("alice", "work", "@bob please handle the deploy")
	if delivered != 2 || woke != 1 {
		t.Fatalf("want delivered=2 woke=1, got delivered=%d woke=%d", delivered, woke)
	}
	select { // mentioned bob is woken
	case <-bobCh:
	default:
		t.Fatal("mentioned bob should be woken")
	}
	select { // unmentioned carol is NOT woken
	case <-carolCh:
		t.Fatal("unmentioned carol should not be woken")
	default:
	}
	// ...but carol still receives the message.
	if got := b.Drain("carol", false, 0); len(got) != 1 {
		t.Fatalf("carol should still receive the topic message: %+v", got)
	}
	if got := b.Drain("bob", false, 0); len(got) != 1 {
		t.Fatalf("bob should receive it too: %+v", got)
	}
}

func TestTopicNoMentionWakesAll(t *testing.T) {
	b := newTestBroker()
	b.Sub("bob", "work")
	b.Sub("carol", "work")
	bobCh := b.waitChan("bob", nil)
	carolCh := b.waitChan("carol", nil)
	_, delivered, woke := b.Pub("alice", "work", "email me at me@host — no mentions")
	if delivered != 2 || woke != 2 {
		t.Fatalf("no @mention should wake all: got delivered=%d woke=%d", delivered, woke)
	}
	select {
	case <-bobCh:
	default:
		t.Fatal("bob should wake")
	}
	select {
	case <-carolCh:
	default:
		t.Fatal("carol should wake")
	}
}

func TestQuietTopicMessageDoesNotTriggerWake(t *testing.T) {
	b := newTestBroker()
	b.Sub("carol", "work")
	b.Pub("alice", "work", "@bob only, not carol") // carol gets a quiet copy

	topic := map[string]bool{KindTopic: true}
	// A quiet topic message must NOT satisfy the wake trigger (so a later-parking
	// recv doesn't wake, and the steer notice skips it).
	if b.HasPending("carol", topic) {
		t.Fatal("a quiet topic message must not trigger a wake")
	}
	ch := b.waitChan("carol", topic)
	select {
	case <-ch:
		t.Fatal("quiet message should not fire waitChan immediately")
	default:
	}
	// ...but carol still receives it on a normal recv.
	if got := b.Drain("carol", false, 0); len(got) != 1 || !got[0].Quiet {
		t.Fatalf("carol should still receive the (quiet) message: %+v", got)
	}
}

func TestUnsubStopsDelivery(t *testing.T) {
	b := newTestBroker()
	b.Sub("bob", "builds")
	b.Unsub("bob", "builds")
	_, n, _ := b.Pub("alice", "builds", "green")
	if n != 0 {
		t.Fatalf("expected no deliveries after unsub, got %d", n)
	}
	_, topics := b.Ps("", false)
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

// --- ask/await primitives ---

func TestWaitChanThreadFiresOnMatchingReply(t *testing.T) {
	b := newTestBroker()
	root, err := b.Send("alice", "bob", "question")
	if err != nil {
		t.Fatal(err)
	}
	ch := b.waitChanThread("alice", root.ID)
	select {
	case <-ch:
		t.Fatal("waiter fired before any reply")
	default:
	}
	if _, err := b.SendThreaded("bob", "alice", "the answer", root.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
		// good
	case <-time.After(time.Second):
		t.Fatal("waiter did not fire after the threaded reply")
	}
}

// waitChanThread mirrors waitChan's contract exactly: any delivery wakes a
// parked waiter (deliver() wakes all waiters unconditionally, see deliver()'s
// own doc comment), and it's the caller's job to re-check the real predicate
// (HasPendingThread) on wake — parkAndDrain's loop does exactly that, so an
// unrelated delivery causes a spurious wake-and-reloop, not a wrong answer.
func TestHasPendingThreadDistinguishesSpuriousWakeFromRealAnswer(t *testing.T) {
	b := newTestBroker()
	root, _ := b.Send("alice", "bob", "question")
	b.Send("carol", "alice", "unrelated")
	if b.HasPendingThread("alice", root.ID) {
		t.Fatal("an unrelated message must not satisfy HasPendingThread")
	}
	b.SendThreaded("bob", "alice", "the answer", root.ID)
	if !b.HasPendingThread("alice", root.ID) {
		t.Fatal("expected the threaded reply to satisfy HasPendingThread")
	}
}

func TestWaitChanThreadFiresImmediatelyIfAlreadyAnswered(t *testing.T) {
	b := newTestBroker()
	root, err := b.Send("alice", "bob", "question")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.SendThreaded("bob", "alice", "already answered", root.ID); err != nil {
		t.Fatal(err)
	}
	ch := b.waitChanThread("alice", root.ID)
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected immediate fire when a reply is already queued")
	}
}

func TestHasPendingThreadIgnoresOtherThreads(t *testing.T) {
	b := newTestBroker()
	root, _ := b.Send("alice", "bob", "q1")
	other, _ := b.Send("alice", "bob", "q2")
	b.SendThreaded("carol", "alice", "reply to q2", other.ID)

	if b.HasPendingThread("alice", root.ID) {
		t.Fatal("must not see a reply belonging to a different thread")
	}
	if !b.HasPendingThread("alice", other.ID) {
		t.Fatal("expected the reply belonging to this thread")
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
	b.Broadcast("alice", "shout", false, false) // bob is registered, receives it
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
	b.Broadcast("alice", "fyi", false, false)
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
		agents, _ := b.Ps("", false)
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
		agents, _ := b.Ps("", false)
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
	agents, _ := b.Ps("", false)
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

func TestCleanupPrunesIdleNotListening(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.Register("old")       // lastSeen = 1000
	b.AddListener("parked") // listening; lastSeen = 1000
	now = now.Add(48 * time.Hour)
	b.Register("recent") // fresh: lastSeen = now

	present := func(name string) bool {
		_, ok := b.agents[name]
		return ok
	}

	// Dry-run: reports the one idle, non-listening agent, removes nothing.
	if preview := b.Cleanup(24*time.Hour, true); len(preview) != 1 || preview[0] != "old" {
		t.Fatalf("dry-run want [old], got %v", preview)
	}
	if !present("old") {
		t.Fatal("dry-run must not remove anything")
	}

	// Real run: prunes "old" only. "parked" is idle 48h but listening (kept);
	// "recent" was just seen (kept).
	removed := b.Cleanup(24*time.Hour, false)
	if len(removed) != 1 || removed[0] != "old" {
		t.Fatalf("want removed [old], got %v", removed)
	}
	if present("old") {
		t.Fatal("'old' should be pruned")
	}
	if !present("parked") || !present("recent") {
		t.Fatal("listening and recently-seen agents must be kept")
	}
}

func TestRegisterOwnedGuard(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }

	if ok, _ := b.RegisterOwned("arise", "sessA", false); !ok {
		t.Fatal("first claim of a free name should succeed")
	}
	if ok, _ := b.RegisterOwned("arise", "sessA", false); !ok {
		t.Fatal("same session re-registering its own name should succeed")
	}
	// A different live session claiming the same name -> collision, regardless of
	// terminal (the host session id is stable, so a new id is a new session).
	if ok, msg := b.RegisterOwned("arise", "sessB", false); ok || msg == "" {
		t.Fatalf("expected a collision, got ok=%v msg=%q", ok, msg)
	}
	// ...but --force takes it over.
	if ok, _ := b.RegisterOwned("arise", "sessB", true); !ok {
		t.Fatal("force should take over")
	}
	// Once the owner is no longer live, another session may take the name.
	now = now.Add(3 * time.Minute)
	if ok, _ := b.RegisterOwned("arise", "sessC", false); !ok {
		t.Fatal("takeover of a non-live owner should be allowed")
	}
}

// ClaimIdentity is the defense-in-depth gate: a different live session may not
// act (send/recv/...) under a name it doesn't own, but the owner itself and a
// free/dead name are fine. A "" session id disables the check.
func TestClaimIdentityGuard(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }

	// A live agent owns "alice".
	if ok, _ := b.RegisterOwned("alice", "sessA", false); !ok {
		t.Fatal("register should succeed")
	}
	// The owning session may act as alice.
	if ok, _ := b.ClaimIdentity("alice", "sessA"); !ok {
		t.Fatal("owner should be allowed to act as its own name")
	}
	// A different live session must be rejected.
	if ok, msg := b.ClaimIdentity("alice", "sessB"); ok || msg == "" {
		t.Fatalf("a foreign live session must be rejected, got ok=%v", ok)
	}
	// No session id -> no enforcement (bare MESS_AGENT run).
	if ok, _ := b.ClaimIdentity("alice", ""); !ok {
		t.Fatal("empty session id should skip the ownership check")
	}
	// A free name is claimable by first live use.
	if ok, _ := b.ClaimIdentity("bob", "sessB"); !ok {
		t.Fatal("first live use of a free name should claim it")
	}
	if ok, msg := b.ClaimIdentity("bob", "sessA"); ok || msg == "" {
		t.Fatalf("bob is now owned by sessB; sessA must be rejected, got ok=%v", ok)
	}
	// Once alice's owner goes stale, a new session may take over.
	now = now.Add(3 * time.Minute)
	if ok, _ := b.ClaimIdentity("alice", "sessB"); !ok {
		t.Fatal("takeover of a non-live owner should be allowed")
	}

	// The shared human mailbox ("user") is exempt: any session may read/act on it
	// (so the operator is never locked out of their own inbox).
	if ok, _ := b.ClaimIdentity("user", "sessA"); !ok {
		t.Fatal("user mailbox should be claimable")
	}
	if ok, _ := b.ClaimIdentity("user", "sessB"); !ok {
		t.Fatal("a different session must still reach the shared user mailbox")
	}
}

func TestRenameMigratesInboxAndSubscriptions(t *testing.T) {
	b := newTestBroker()
	b.RegisterOwned("old", "sessX", false)
	b.Send("peer", "old", "queued for old")
	b.Sub("old", "builds")

	if ok, msg := b.Rename("old", "new", "sessX", false); !ok {
		t.Fatalf("rename should succeed: %s", msg)
	}
	if _, ok := b.agents["old"]; ok {
		t.Fatal("old agent should be gone after rename")
	}
	// Inbox followed the rename.
	got := b.Drain("new", false, 0)
	if len(got) != 1 || got[0].Body != "queued for old" {
		t.Fatalf("inbox not migrated: %+v", got)
	}
	// Subscription moved: new is subscribed, old is not.
	if !b.topics["builds"]["new"] || b.topics["builds"]["old"] {
		t.Fatalf("subscription not migrated: %+v", b.topics["builds"])
	}
	// Ownership moved.
	if b.owners["new"].session != "sessX" {
		t.Fatal("owner not carried to new name")
	}
	if _, ok := b.owners["old"]; ok {
		t.Fatal("old owner should be cleared")
	}
}

func TestRenameHonorsCollisionGuard(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.RegisterOwned("me", "s1", false)
	b.RegisterOwned("taken", "s2", false) // a different live session

	if ok, msg := b.Rename("me", "taken", "s1", false); ok || msg == "" {
		t.Fatalf("rename onto a live name should be refused, got ok=%v", ok)
	}
	if ok, _ := b.Rename("me", "taken", "s1", true); !ok {
		t.Fatal("--force rename should take the name over")
	}
	if _, ok := b.agents["me"]; ok {
		t.Fatal("source name should be gone after a forced rename")
	}
}

func TestPsReportsOnline(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.Register("stale")     // active now, but will go stale
	b.AddListener("parked") // a live listener
	online := func(name string) bool {
		agents, _ := b.Ps("", false)
		for _, a := range agents {
			if a.Name == name {
				return a.Online
			}
		}
		return false
	}
	if !online("stale") {
		t.Fatal("a just-active agent should be online")
	}
	now = now.Add(10 * time.Minute) // stale's last activity is now old
	if online("stale") {
		t.Fatal("an agent idle for 10m should be offline")
	}
	if !online("parked") {
		t.Fatal("a listening agent should stay online")
	}
	b.SetBusy("stale", time.Minute) // working again -> back online
	if !online("stale") {
		t.Fatal("a working agent should be online")
	}
}

func TestWarningAutoClearsAndExpires(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.Register("bob")
	warn := func() string {
		agents, _ := b.Ps("", false)
		for _, a := range agents {
			if a.Name == "bob" {
				return a.Warning
			}
		}
		return ""
	}

	b.SetWarn("bob", "API error", time.Minute)
	if warn() != "API error" {
		t.Fatalf("warning not reported, got %q", warn())
	}
	// Becoming active (a new turn) clears the stale warning.
	b.SetBusy("bob", time.Minute)
	if warn() != "" {
		t.Fatalf("warning should clear on activity, got %q", warn())
	}
	// Re-registering (a resumed session) also clears it.
	b.SetWarn("bob", "again", time.Minute)
	b.RegisterOwned("bob", "s", false)
	if warn() != "" {
		t.Fatalf("warning should clear on re-register, got %q", warn())
	}
	// And it self-expires even if the agent never recovers.
	b.SetWarn("bob", "still down", time.Minute)
	now = now.Add(2 * time.Minute)
	if warn() != "" {
		t.Fatalf("expired warning should not be reported, got %q", warn())
	}
	// Empty text clears explicitly.
	now = now.Add(-2 * time.Minute) // back within TTL
	b.SetWarn("bob", "x", time.Minute)
	b.SetWarn("bob", "", time.Minute)
	if warn() != "" {
		t.Fatalf("empty SetWarn should clear, got %q", warn())
	}
}

func TestDrainQuietNoTouchNoAck(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	_, ackCh, _ := b.SendAck("peer", "dead", "did you read this?") // ack-requested

	got := b.DrainQuiet("dead", 0)
	if len(got) != 1 {
		t.Fatalf("drain should return the queued message, got %d", len(got))
	}
	// It must NOT mark the target active (so it stays eligible for cleanup).
	if _, ok := b.lastSeen["dead"]; ok {
		t.Fatal("drain must not touch the target agent")
	}
	// It must NOT fire the read receipt (the operator read it, not the agent).
	select {
	case <-ackCh:
		t.Fatal("drain must not fire the ack")
	default:
	}
	// Inbox is cleared.
	if len(b.DrainQuiet("dead", 0)) != 0 {
		t.Fatal("inbox should be empty after drain")
	}
}

func TestCleanupPrunesByStaleMail(t *testing.T) {
	now := time.Unix(0, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	// "dead" never acts (no lastSeen) but has mail sitting in its inbox.
	b.Send("peer", "dead", "old mail")
	now = now.Add(48 * time.Hour) // the mail is now 48h old; dead is offline
	removed := b.Cleanup(24*time.Hour, false)
	found := false
	for _, n := range removed {
		if n == "dead" {
			found = true
		}
	}
	if !found {
		t.Fatalf("agent with 48h-old undrained mail should be pruned, got %v", removed)
	}
}

// --- backlog TTL (ExpireInbox) ---

// Unlike Cleanup, ExpireInbox must drop old unread mail even from an agent
// that's currently alive (listening/working) — a live-but-sporadic agent can
// still be sitting on ancient unread mail Cleanup would never touch.
func TestExpireInboxDropsOldMailEvenFromAliveAgent(t *testing.T) {
	now := time.Unix(0, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.Send("alice", "bob", "old mail")
	b.AddListener("bob") // alive: Cleanup would skip this agent entirely
	now = now.Add(15 * 24 * time.Hour)

	expired := b.ExpireInbox(14*24*time.Hour, false)
	if len(expired) != 1 || expired[0].Body != "old mail" {
		t.Fatalf("expected the old message to expire despite bob being alive, got %+v", expired)
	}
	if got := b.Drain("bob", false, 0); len(got) != 0 {
		t.Fatalf("expired message should be gone from the inbox, got %+v", got)
	}
}

// Only the old messages in a mixed inbox expire; recent ones stay queued.
func TestExpireInboxKeepsRecentMessages(t *testing.T) {
	now := time.Unix(0, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.Send("alice", "bob", "old")
	now = now.Add(15 * 24 * time.Hour)
	b.Send("alice", "bob", "recent")

	expired := b.ExpireInbox(14*24*time.Hour, false)
	if len(expired) != 1 || expired[0].Body != "old" {
		t.Fatalf("expected only the old message to expire, got %+v", expired)
	}
	got := b.Drain("bob", false, 0)
	if len(got) != 1 || got[0].Body != "recent" {
		t.Fatalf("expected the recent message to remain queued, got %+v", got)
	}
}

// dryRun (--dry-run) previews without mutating anything.
func TestExpireInboxDryRunLeavesInboxUntouched(t *testing.T) {
	now := time.Unix(0, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.Send("alice", "bob", "old mail")
	now = now.Add(15 * 24 * time.Hour)

	expired := b.ExpireInbox(14*24*time.Hour, true)
	if len(expired) != 1 {
		t.Fatalf("expected 1 eligible message in the preview, got %+v", expired)
	}
	if got := b.Drain("bob", false, 0); len(got) != 1 {
		t.Fatalf("dry-run must not mutate the inbox, got %+v", got)
	}
}

// Same carve-out as Cleanup: the human's mailbox is never auto-expired.
func TestExpireInboxNeverDropsUserHandle(t *testing.T) {
	now := time.Unix(0, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.Send("alice", "user", "please look at this")
	now = now.Add(365 * 24 * time.Hour)

	expired := b.ExpireInbox(14*24*time.Hour, false)
	if len(expired) != 0 {
		t.Fatalf("the human's mailbox must never auto-expire, got %+v", expired)
	}
	if got := b.Drain("user", false, 0); len(got) != 1 {
		t.Fatalf("expected the message to remain queued for the human, got %+v", got)
	}
}

func TestReplayReturnsConsumedHistory(t *testing.T) {
	b := newTestBroker()
	for _, body := range []string{"one", "two", "three"} {
		b.Send("peer", "bob", body)
	}
	// Nothing consumed yet -> empty history.
	if got := b.Replay("bob", 0); len(got) != 0 {
		t.Fatalf("no history before consume, got %d", len(got))
	}
	// Consume them (like a recv / wake) -> they enter history.
	b.Drain("bob", false, 0)
	if got := b.Replay("bob", 0); len(got) != 3 || got[0].Body != "one" || got[2].Body != "three" {
		t.Fatalf("replay should return the 3 consumed in order, got %+v", got)
	}
	// A peek must NOT add to history.
	b.Send("peer", "bob", "four")
	b.Drain("bob", true, 0) // peek
	if got := b.Replay("bob", 0); len(got) != 3 {
		t.Fatalf("peek should not extend history, got %d", len(got))
	}
	// Last-N.
	b.Drain("bob", false, 0) // consumes "four"
	if got := b.Replay("bob", 2); len(got) != 2 || got[0].Body != "three" || got[1].Body != "four" {
		t.Fatalf("replay 2 should return the last two, got %+v", got)
	}
}

func TestLastSeenPersists(t *testing.T) {
	b := newTestBroker() // now fixed at Unix(0,0)
	b.Register("bob")
	b2 := newTestBroker()
	b2.load(b.snapshot())
	if got, ok := b2.lastSeen["bob"]; !ok || !got.Equal(time.Unix(0, 0)) {
		t.Fatalf("lastSeen not restored: got %v ok=%v", got, ok)
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
	_, topics := b2.Ps("", false)
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

// --- rooms ---

func TestAgentKeyRoundTrip(t *testing.T) {
	if got := agentKey("", "alice"); got != "alice" {
		t.Fatalf("empty room should collapse to the bare name, got %q", got)
	}
	key := agentKey("teamA", "alice")
	room, name := splitAgentKey(key)
	if room != "teamA" || name != "alice" {
		t.Fatalf("round trip failed: room=%q name=%q", room, name)
	}
	if room, name := splitAgentKey("alice"); room != "" || name != "alice" {
		t.Fatalf("bare key should split to empty room: room=%q name=%q", room, name)
	}
}

// The same name in two different rooms must be able to register independently
// — this is the whole point of rooms, and it should fall out of the composite
// key with no special-case collision code.
func TestRoomedIdentitiesDontCollide(t *testing.T) {
	b := newTestBroker()
	if ok, msg := b.RegisterOwned(agentKey("A", "admin"), "sessA", false); !ok {
		t.Fatalf("first room's admin should register: %s", msg)
	}
	if ok, msg := b.RegisterOwned(agentKey("B", "admin"), "sessB", false); !ok {
		t.Fatalf("second room's admin should register independently: %s", msg)
	}
	// But within the SAME room, the usual collision guard still applies.
	if ok, msg := b.RegisterOwned(agentKey("A", "admin"), "sessC", false); ok || msg == "" {
		t.Fatalf("a different session claiming the same room's name should collide, got ok=%v", ok)
	}

	b.Send(agentKey("A", "admin"), agentKey("A", "bob"), "hi from room A")
	b.Send(agentKey("B", "admin"), agentKey("B", "bob"), "hi from room B")
	gotA := b.Drain(agentKey("A", "bob"), false, 0)
	gotB := b.Drain(agentKey("B", "bob"), false, 0)
	if len(gotA) != 1 || gotA[0].From != "admin" || gotA[0].Body != "hi from room A" {
		t.Fatalf("room A delivery wrong: %+v", gotA)
	}
	if len(gotB) != 1 || gotB[0].From != "admin" || gotB[0].Body != "hi from room B" {
		t.Fatalf("room B delivery wrong: %+v", gotB)
	}
}

func TestBroadcastScopedToRoom(t *testing.T) {
	b := newTestBroker()
	b.Register(agentKey("A", "alice"))
	b.Register(agentKey("A", "bob"))
	b.Register(agentKey("B", "carol")) // different room, must not receive

	_, n := b.Broadcast(agentKey("A", "alice"), "hello room A", false, false)
	if n != 1 {
		t.Fatalf("expected 1 same-room recipient, got %d", n)
	}
	if got := b.Drain(agentKey("A", "bob"), false, 0); len(got) != 1 {
		t.Fatalf("room A's bob should receive the broadcast: %+v", got)
	}
	if got := b.Drain(agentKey("B", "carol"), false, 0); len(got) != 0 {
		t.Fatalf("a different room must not leak the broadcast: %+v", got)
	}
}

func TestLoudHostWideBroadcastCrossesRooms(t *testing.T) {
	b := newTestBroker()
	b.Register(agentKey("A", "alice"))
	b.Register(agentKey("A", "bob"))
	b.Register(agentKey("B", "carol"))

	// Plain --loud (not --loud-room) sets hostWide: true and must reach every
	// room, unlike an ordinary or --loud-room broadcast (TestBroadcastScopedToRoom).
	_, n := b.Broadcast(agentKey("A", "alice"), "restarting the daemon", true, true)
	if n != 2 {
		t.Fatalf("expected 2 host-wide recipients (bob + carol), got %d", n)
	}
	if got := b.Drain(agentKey("B", "carol"), false, 0); len(got) != 1 || !got[0].Loud {
		t.Fatalf("carol in a different room should still receive a host-wide loud broadcast: %+v", got)
	}
}

func TestLoudRoomBroadcastStaysRoomScoped(t *testing.T) {
	b := newTestBroker()
	b.Register(agentKey("A", "alice"))
	b.Register(agentKey("A", "bob"))
	b.Register(agentKey("B", "carol"))

	// --loud-room: loud, but hostWide stays false, so it must NOT cross rooms.
	_, n := b.Broadcast(agentKey("A", "alice"), "loud but room-scoped", true, false)
	if n != 1 {
		t.Fatalf("expected 1 same-room recipient, got %d", n)
	}
	if got := b.Drain(agentKey("B", "carol"), false, 0); len(got) != 0 {
		t.Fatalf("--loud-room must not leak across rooms: %+v", got)
	}
}

func TestPubTopicScopedToRoom(t *testing.T) {
	b := newTestBroker()
	b.Sub(agentKey("A", "bob"), topicKey("A", "deploy"))
	b.Sub(agentKey("B", "carol"), topicKey("B", "deploy")) // same topic NAME, different room

	_, delivered, _ := b.Pub(agentKey("A", "alice"), topicKey("A", "deploy"), "ship it")
	if delivered != 1 {
		t.Fatalf("expected 1 delivery within room A, got %d", delivered)
	}
	if got := b.Drain(agentKey("A", "bob"), false, 0); len(got) != 1 || got[0].Topic != "deploy" {
		t.Fatalf("room A's bob should get the publish: %+v", got)
	}
	if got := b.Drain(agentKey("B", "carol"), false, 0); len(got) != 0 {
		t.Fatalf("room B's carol (same topic name, different room) must not receive it: %+v", got)
	}
}

func TestPsDefaultScopesToCallerRoom(t *testing.T) {
	b := newTestBroker()
	b.Register(agentKey("A", "alice"))
	b.Register(agentKey("B", "bob"))
	b.Register("global-carol")

	agents, _ := b.Ps("A", false)
	if len(agents) != 1 || agents[0].Name != "alice" || agents[0].Room != "A" {
		t.Fatalf("expected only room A's alice, got %+v", agents)
	}
	agents, _ = b.Ps("", false) // the global/default room
	if len(agents) != 1 || agents[0].Name != "global-carol" {
		t.Fatalf("expected only the global room's carol, got %+v", agents)
	}
}

func TestPsAllShowsEveryRoomWithRoomField(t *testing.T) {
	b := newTestBroker()
	b.Register(agentKey("A", "alice"))
	b.Register(agentKey("B", "bob"))
	b.Sub(agentKey("A", "alice"), topicKey("A", "deploy"))

	agents, topics := b.Ps("", true)
	if len(agents) != 2 {
		t.Fatalf("expected both rooms' agents, got %+v", agents)
	}
	if len(topics) != 1 || topics[0].Room != "A" || topics[0].Name != "deploy" {
		t.Fatalf("expected room A's deploy topic, got %+v", topics)
	}
}

// A never-joined agent (room=="") sees exactly today's pre-rooms behavior: the
// full global fleet, nothing missing, nothing extra — the backward-
// compatibility bar this whole feature is held to.
func TestPsUnjoinedAgentSeesFullGlobalFleetUnchanged(t *testing.T) {
	b := newTestBroker()
	b.Register("k")
	b.Register("a")
	b.Register("l")
	b.Register(agentKey("someproject", "admin")) // a room-joined agent elsewhere

	agents, _ := b.Ps("", false)
	if len(agents) != 3 {
		t.Fatalf("expected exactly the 3 global agents, got %+v", agents)
	}
	for _, a := range agents {
		if a.Room != "" {
			t.Fatalf("global-room ps leaked a room field: %+v", a)
		}
	}
}

// Regression test: a room-scoped `mess ps` must still surface the human
// operator's mailbox, even though it's registered in the global room — a
// room-joined agent otherwise has no way to see "user" is reachable without
// already knowing the (undocumented-in-ps) always-global convention. Same
// exception class as TestCleanupNeverPrunesUserHandleInAnyRoom.
func TestPsSurfacesUserHandleAcrossRooms(t *testing.T) {
	b := newTestBroker()
	b.Register(agentKey("A", "alice"))
	b.Register("user") // the human's global mailbox

	agents, _ := b.Ps("A", false)
	names := map[string]bool{}
	for _, a := range agents {
		names[a.Name] = true
	}
	if !names["alice"] || !names["user"] {
		t.Fatalf("expected both alice and the user mailbox in room A's ps, got %+v", agents)
	}

	// A different, unrelated room must also see the human mailbox.
	b.Register(agentKey("B", "bob"))
	agents, _ = b.Ps("B", false)
	names = map[string]bool{}
	for _, a := range agents {
		names[a.Name] = true
	}
	if !names["bob"] || !names["user"] {
		t.Fatalf("expected both bob and the user mailbox in room B's ps, got %+v", agents)
	}
}

// Regression test for a real bug class introduced by room-scoping: Cleanup's
// "never prune the human's mailbox" guard must check the bare name, not the
// composite map key (isUserHandle("A\x00user") is false, but isUserHandle on
// the split bare name "user" is true).
func TestCleanupNeverPrunesUserHandleInAnyRoom(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.Register(agentKey("A", "user"))
	now = now.Add(48 * time.Hour) // long past any maxAge, and never "alive"

	removed := b.Cleanup(time.Hour, false)
	if len(removed) != 0 {
		t.Fatalf("the human mailbox must never be pruned, even room-scoped: %+v", removed)
	}
	if _, ok := b.agents[agentKey("A", "user")]; !ok {
		t.Fatal("room-scoped user handle should still be present")
	}
}

func TestRenameStaysWithinRoom(t *testing.T) {
	b := newTestBroker()
	b.RegisterOwned(agentKey("A", "old"), "sessX", false)
	b.Send("peer", agentKey("A", "old"), "queued for old")
	b.Sub(agentKey("A", "old"), topicKey("A", "builds"))

	if ok, msg := b.Rename(agentKey("A", "old"), agentKey("A", "new"), "sessX", false); !ok {
		t.Fatalf("rename should succeed: %s", msg)
	}
	if got := b.Drain(agentKey("A", "new"), false, 0); len(got) != 1 || got[0].Body != "queued for old" {
		t.Fatalf("inbox not migrated within room: %+v", got)
	}
	tk := topicKey("A", "builds")
	if !b.topics[tk][agentKey("A", "new")] || b.topics[tk][agentKey("A", "old")] {
		t.Fatalf("subscription not migrated to the room-scoped topic key: %+v", b.topics[tk])
	}
}

// --- FindOtherRoom ---

func TestFindOtherRoomFindsRegisteredElsewhere(t *testing.T) {
	b := newTestBroker()
	b.RegisterOwned(agentKey("frontend", "bob"), "sess1", false)

	room, found := b.FindOtherRoom("", "bob")
	if !found || room != "frontend" {
		t.Fatalf("expected to find bob in room \"frontend\", got room=%q found=%v", room, found)
	}
}

func TestFindOtherRoomNoMatchInCallersOwnRoom(t *testing.T) {
	b := newTestBroker()
	b.RegisterOwned(agentKey("frontend", "bob"), "sess1", false)

	if _, found := b.FindOtherRoom("frontend", "bob"); found {
		t.Fatal("should not report a match when bob is registered in the caller's OWN room")
	}
}

func TestFindOtherRoomNoMatchWhenNeverRegistered(t *testing.T) {
	b := newTestBroker()
	if _, found := b.FindOtherRoom("", "nobody"); found {
		t.Fatal("should not find a match for a name that's never been registered anywhere")
	}
}

// --- JoinRoom (room-join's identity migration, not a bare duplicate) ---

// This is the actual fix for a real incident: register (bare-global) then
// room-join used to leave BOTH a stale global owner entry and the real
// room-scoped one, so mail/presence landed on whichever one a caller
// happened to address.
func TestJoinRoomMigratesFromBareGlobal(t *testing.T) {
	b := newTestBroker()
	b.RegisterOwned("bob", "sess1", false)
	b.Send("peer", "bob", "queued before joining")

	if ok, msg := b.JoinRoom("bob", agentKey("frontend", "bob"), "sess1", false); !ok {
		t.Fatalf("join should succeed: %s", msg)
	}
	if _, ok := b.agents["bob"]; ok {
		t.Fatal("stale bare-global agent should be gone after joining a room")
	}
	if _, ok := b.owners["bob"]; ok {
		t.Fatal("stale bare-global owner should be gone after joining a room")
	}
	got := b.Drain(agentKey("frontend", "bob"), false, 0)
	if len(got) != 1 || got[0].Body != "queued before joining" {
		t.Fatalf("inbox not migrated into the room-scoped identity: %+v", got)
	}
}

func TestJoinRoomHonorsCollisionGuard(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.RegisterOwned(agentKey("frontend", "taken"), "s2", false) // a different live session

	if ok, msg := b.JoinRoom("me", agentKey("frontend", "taken"), "s1", false); ok || msg == "" {
		t.Fatal("joining onto a name a different live session owns should be refused")
	}
}

// The collision guard must run even when from==who (unlike Rename's
// old==newName fast path) — a client-supplied FromRoom could coincidentally
// equal the target room for a session that never legitimately registered
// anywhere, and that must NOT bypass ownership enforcement.
func TestJoinRoomChecksCollisionEvenWhenFromEqualsWho(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.RegisterOwned(agentKey("frontend", "taken"), "s2", false) // a different live session

	// An attacker-ish session claims FromRoom == the target room, hoping to
	// hit Rename-style same-key fast-path semantics and skip the guard.
	if ok, msg := b.JoinRoom(agentKey("frontend", "taken"), agentKey("frontend", "taken"), "s1", false); ok || msg == "" {
		t.Fatal("from==who must not bypass the collision guard")
	}
}

func TestJoinRoomForceOverridesCollision(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.RegisterOwned(agentKey("frontend", "taken"), "s2", false)

	if ok, msg := b.JoinRoom("me", agentKey("frontend", "taken"), "s1", true); !ok {
		t.Fatalf("--force should override the collision guard: %s", msg)
	}
}

func TestSnapshotRoundTripsRooms(t *testing.T) {
	b := newTestBroker()
	b.Register(agentKey("A", "alice"))
	b.Sub(agentKey("A", "alice"), topicKey("A", "deploy"))
	b.Register("global-bob")
	snap := b.snapshot()

	b2 := newTestBroker()
	b2.load(snap)
	agents, topics := b2.Ps("", true)
	if len(agents) != 2 {
		t.Fatalf("expected both agents restored, got %+v", agents)
	}
	if len(topics) != 1 || topics[0].Room != "A" || topics[0].Name != "deploy" {
		t.Fatalf("expected room A's topic restored, got %+v", topics)
	}
}

// This is the single most important regression test given the live daemon's
// on-disk state: an existing state.json written by a pre-rooms daemon has
// "topics" as a bare {"name": ["sub", ...]} object, not the current room-aware
// array. It must still load, with every legacy topic landing in the global
// room.
func TestLoadLegacySnapshotTopicsMapMigrates(t *testing.T) {
	legacy := []byte(`{
		"seq": 3,
		"agents": [{"name": "bob", "topics": ["builds"]}],
		"topics": {"builds": ["bob"]}
	}`)
	var s snapshot
	if err := json.Unmarshal(legacy, &s); err != nil {
		t.Fatalf("legacy snapshot failed to parse: %v", err)
	}
	if len(s.Topics) != 1 || s.Topics[0].Room != "" || s.Topics[0].Name != "builds" {
		t.Fatalf("legacy topic did not migrate to the global room: %+v", s.Topics)
	}
	if len(s.Agents) != 1 || s.Agents[0].Room != "" || s.Agents[0].Name != "bob" {
		t.Fatalf("legacy agent should default to the global room: %+v", s.Agents)
	}

	b := newTestBroker()
	b.load(s)
	agents, topics := b.Ps("", false)
	if len(agents) != 1 || agents[0].Name != "bob" {
		t.Fatalf("legacy agent not loaded: %+v", agents)
	}
	if len(topics) != 1 || topics[0].Name != "builds" || len(topics[0].Subscribers) != 1 || topics[0].Subscribers[0] != "bob" {
		t.Fatalf("legacy topic not loaded: %+v", topics)
	}
}

// --- bridges ---

func TestBridgeRelaysToOtherRoomTopic(t *testing.T) {
	b := newTestBroker()
	b.Sub(agentKey("A", "alice"), topicKey("A", "deploy"))
	b.Sub(agentKey("B", "bob"), topicKey("B", "ops"))

	if _, err := b.Bridge("A", "deploy", "B", "ops", bridgeBoth, "alice", 0, false); err != nil {
		t.Fatalf("bridge creation failed: %v", err)
	}
	_, delivered, _ := b.Pub(agentKey("A", "alice"), topicKey("A", "deploy"), "shipping v2")
	if delivered != 0 {
		t.Fatalf("no other local subscriber in room A, expected 0 direct deliveries, got %d", delivered)
	}
	got := b.Drain(agentKey("B", "bob"), false, 0)
	if len(got) != 1 || got[0].Body != "shipping v2" || got[0].OriginRoom != "A" || got[0].OriginTopic != "deploy" {
		t.Fatalf("bridge did not relay correctly: %+v", got)
	}
}

// A "both" bridge relays either way; "out"/"in" (relative to the creation
// order a->b) only relay one way.
func TestBridgeDirectionRespected(t *testing.T) {
	b := newTestBroker()
	b.Sub(agentKey("A", "alice"), topicKey("A", "x"))
	b.Sub(agentKey("B", "bob"), topicKey("B", "y"))

	if _, err := b.Bridge("A", "x", "B", "y", bridgeAToB, "alice", 0, false); err != nil {
		t.Fatalf("bridge creation failed: %v", err)
	}
	// A -> B: bob should get it.
	b.Pub(agentKey("A", "someone"), topicKey("A", "x"), "a to b")
	if got := b.Drain(agentKey("B", "bob"), false, 0); len(got) != 1 {
		t.Fatalf("out-direction bridge should relay A->B: %+v", got)
	}
	b.Drain(agentKey("A", "alice"), false, 0) // clear alice's own direct copy of that first publish
	// B -> A: alice should NOT get a relayed copy (direction disallows this way).
	b.Pub(agentKey("B", "someone"), topicKey("B", "y"), "b to a, should not relay")
	if got := b.Drain(agentKey("A", "alice"), false, 0); len(got) != 0 {
		t.Fatalf("out-direction bridge must not relay B->A: %+v", got)
	}
}

// A cycle of bridges (A<->B<->A) must not ping-pong forever — the visited-set
// guard, not the hop cap, should be what stops it (each topic is only entered
// once per publish).
func TestBridgeLoopPreventionOnCycle(t *testing.T) {
	b := newTestBroker()
	b.Sub(agentKey("A", "alice"), topicKey("A", "x"))
	b.Sub(agentKey("B", "bob"), topicKey("B", "y"))
	// Two bridges forming a cycle: A/x <-> B/y, and B/y <-> A/x again (a second,
	// distinct bridge between the same two topics — forced, since it would
	// otherwise be treated as a duplicate).
	if _, err := b.Bridge("A", "x", "B", "y", bridgeBoth, "alice", 0, false); err != nil {
		t.Fatalf("first bridge failed: %v", err)
	}
	if _, err := b.Bridge("B", "y", "A", "x", bridgeBoth, "alice", 0, true); err != nil {
		t.Fatalf("second (cycle-forming) bridge failed: %v", err)
	}

	_, delivered, _ := b.Pub(agentKey("A", "alice"), topicKey("A", "x"), "should not infinite-loop")
	if delivered != 0 { // alice is the sender, no other local subscriber
		t.Fatalf("unexpected direct delivery count: %d", delivered)
	}
	got := b.Drain(agentKey("B", "bob"), false, 0)
	if len(got) != 1 {
		t.Fatalf("bob should receive exactly one relayed copy, not a duplicate from the cycle: %+v", got)
	}
}

// A no-mention publish still wakes direct local subscribers (as today), but
// its relayed copy on the far side of a bridge is quiet-delivered — a bridge
// between two busy rooms can't become a wake-storm amplifier. An individually
// @mentioned name on the far side still wakes, same as a direct mention would.
func TestBridgeRelayIsQuietUnlessMentioned(t *testing.T) {
	b := newTestBroker()
	b.Sub(agentKey("B", "bob"), topicKey("B", "y"))
	b.Sub(agentKey("B", "carol"), topicKey("B", "y"))
	if _, err := b.Bridge("A", "x", "B", "y", bridgeBoth, "alice", 0, false); err != nil {
		t.Fatalf("bridge creation failed: %v", err)
	}

	// No mention at all: neither far-side subscriber should wake.
	bobCh := b.waitChan(agentKey("B", "bob"), nil)
	carolCh := b.waitChan(agentKey("B", "carol"), nil)
	b.Pub(agentKey("A", "alice"), topicKey("A", "x"), "no mention, relayed")
	select {
	case <-bobCh:
		t.Fatal("an unmentioned relay recipient must not be woken")
	default:
	}
	select {
	case <-carolCh:
		t.Fatal("an unmentioned relay recipient must not be woken")
	default:
	}
	got := b.Drain(agentKey("B", "bob"), false, 0)
	if len(got) != 1 || !got[0].Quiet {
		t.Fatalf("bob should still receive the relayed message, quietly: %+v", got)
	}

	// @bob specifically: bob should wake, carol (unmentioned) should not.
	bobCh = b.waitChan(agentKey("B", "bob"), nil)
	carolCh = b.waitChan(agentKey("B", "carol"), nil)
	b.Pub(agentKey("A", "alice"), topicKey("A", "x"), "@bob check this out (relayed)")
	select {
	case <-bobCh:
		// expected: an explicit mention wakes, even across a bridge
	default:
		t.Fatal("a mentioned relay recipient should be woken")
	}
	select {
	case <-carolCh:
		t.Fatal("unmentioned carol must not be woken by a relay that mentions someone else")
	default:
	}
	got = b.Drain(agentKey("B", "bob"), false, 0)
	if len(got) != 1 || got[0].Quiet {
		t.Fatalf("mentioned bob's copy should NOT be quiet: %+v", got)
	}
}

func TestUnbridgeIsIdempotent(t *testing.T) {
	b := newTestBroker()
	br, err := b.Bridge("A", "x", "B", "y", bridgeBoth, "alice", 0, false)
	if err != nil {
		t.Fatalf("bridge creation failed: %v", err)
	}
	if ok, _ := b.Unbridge(br.id); !ok {
		t.Fatal("first unbridge should succeed")
	}
	if ok, desc := b.Unbridge(br.id); ok || desc != "" {
		t.Fatalf("second unbridge of the same id should be a no-op, got ok=%v desc=%q", ok, desc)
	}
	if len(b.bridgesByTopic[topicKey("A", "x")]) != 0 || len(b.bridgesByTopic[topicKey("B", "y")]) != 0 {
		t.Fatal("bridgesByTopic should be cleaned up after unbridge")
	}
}

func TestBridgeSnapshotRoundTrip(t *testing.T) {
	b := newTestBroker()
	if _, err := b.Bridge("A", "x", "B", "y", bridgeAToB, "alice", time.Hour, false); err != nil {
		t.Fatalf("bridge creation failed: %v", err)
	}
	snap := b.snapshot()

	b2 := newTestBroker()
	b2.load(snap)
	list := b2.ListBridges()
	if len(list) != 1 {
		t.Fatalf("expected 1 bridge restored, got %+v", list)
	}
	br := list[0]
	if br.ARoom != "A" || br.ATopic != "x" || br.BRoom != "B" || br.BTopic != "y" || br.Direction != "out" || br.Creator != "alice" {
		t.Fatalf("bridge fields not restored correctly: %+v", br)
	}
	// The relay mechanism must also work post-restore (bridgesByTopic rebuilt).
	b2.Sub(agentKey("B", "bob"), topicKey("B", "y"))
	b2.Pub(agentKey("A", "alice"), topicKey("A", "x"), "still relays after restore")
	if got := b2.Drain(agentKey("B", "bob"), false, 0); len(got) != 1 {
		t.Fatalf("restored bridge should still relay: %+v", got)
	}
}

// --- threads ---

// A no-mention threaded reply is quiet-delivered to an uninvolved subscriber
// (same class of fix as @mention: a reply shouldn't wake everyone the way a
// fresh topic message does), but wakes an existing thread participant even
// without naming them, same as an explicit @mention would.
func TestThreadedReplyWakesParticipantsNotBystanders(t *testing.T) {
	b := newTestBroker()
	b.Sub("alice", "eng")
	b.Sub("bob", "eng")
	b.Sub("carol", "eng") // never posts in the thread; should stay a bystander

	root, _, _ := b.Pub("alice", "eng", "kicking off a discussion")
	// bob replies in the thread -> he's now a participant.
	b.PubThreaded("bob", "eng", "I have thoughts", root.ID)
	// Drain everyone so waitChan's "already pending" fast path can't mask the
	// real assertion below with leftover messages from this setup.
	b.Drain("alice", false, 0)
	b.Drain("bob", false, 0)
	b.Drain("carol", false, 0)

	// alice replies again, still no @mention -> bob (participant) should wake,
	// carol (bystander) should not.
	bobCh := b.waitChan("bob", nil)
	carolCh := b.waitChan("carol", nil)
	b.PubThreaded("alice", "eng", "responding to bob", root.ID)

	select {
	case <-bobCh:
		// expected: bob is a thread participant
	default:
		t.Fatal("bob (thread participant) should be woken by a threaded reply")
	}
	select {
	case <-carolCh:
		t.Fatal("carol (never posted in the thread) should not be woken")
	default:
	}
	got := b.Drain("carol", false, 0)
	if len(got) != 1 || !got[0].Quiet {
		t.Fatalf("carol should still receive the threaded reply, quietly: %+v", got)
	}
	got = b.Drain("bob", false, 0)
	if len(got) != 1 || got[0].Quiet {
		t.Fatalf("bob's copy should NOT be quiet (he's a participant): %+v", got)
	}
}

// A direct (non-topic) threaded send is just metadata/participant-tracking —
// it doesn't change wake behavior, since there's only one recipient.
// --- attachments ---

func TestSendThreadedAttachRecordsFields(t *testing.T) {
	b := newTestBroker()
	mtime := time.Unix(1000, 0)
	attach := &Attachment{Path: "/tmp/cfg.yaml", Hash: "sha256:abcd", Size: 42, MTime: mtime}
	m, err := b.SendThreadedAttach("alice", "bob", "see this", "", attach)
	if err != nil {
		t.Fatal(err)
	}
	if m.AttachPath != "/tmp/cfg.yaml" || m.AttachHash != "sha256:abcd" || m.AttachSize != 42 || !m.AttachMTime.Equal(mtime) {
		t.Fatalf("attachment fields not recorded on the sent message: %+v", m)
	}
	got := b.Drain("bob", false, 0)
	if len(got) != 1 || got[0].AttachPath != "/tmp/cfg.yaml" {
		t.Fatalf("attachment fields not delivered to the recipient: %+v", got)
	}
}

func TestSendThreadedWithoutAttachLeavesFieldsEmpty(t *testing.T) {
	b := newTestBroker()
	m, err := b.SendThreaded("alice", "bob", "no attachment here", "")
	if err != nil {
		t.Fatal(err)
	}
	if m.AttachPath != "" || m.AttachHash != "" {
		t.Fatalf("expected no attachment fields, got %+v", m)
	}
}

func TestPubThreadedAttachRecordsFields(t *testing.T) {
	b := newTestBroker()
	b.Sub("bob", "eng")
	attach := &Attachment{Path: "/tmp/note.txt", Hash: "sha256:ef01", Size: 7}
	m, _, _ := b.PubThreadedAttach("alice", "eng", "check this", "", attach)
	if m.AttachPath != "/tmp/note.txt" || m.AttachHash != "sha256:ef01" || m.AttachSize != 7 {
		t.Fatalf("attachment fields not recorded on the published message: %+v", m)
	}
	got := b.Drain("bob", false, 0)
	if len(got) != 1 || got[0].AttachHash != "sha256:ef01" {
		t.Fatalf("attachment fields not delivered to the subscriber: %+v", got)
	}
}

// SendAsk flags the delivered message as Ask, distinguishing it from an
// ordinary SendThreaded — this is what lets recv/log rendering and the
// auto-wake injection tell the recipient a plain reply won't satisfy the
// asker's wait (only a threaded one does).
func TestSendAskFlagsMessage(t *testing.T) {
	b := newTestBroker()
	m, err := b.SendAsk("alice", "bob", "status?")
	if err != nil {
		t.Fatal(err)
	}
	if !m.Ask {
		t.Fatalf("expected the sent message to be flagged Ask, got %+v", m)
	}
	got := b.Drain("bob", false, 0)
	if len(got) != 1 || !got[0].Ask {
		t.Fatalf("expected the delivered message to carry Ask, got %+v", got)
	}
}

func TestSendThreadedIsNotFlaggedAsk(t *testing.T) {
	b := newTestBroker()
	m, err := b.SendThreaded("alice", "bob", "just a reply", "m1")
	if err != nil {
		t.Fatal(err)
	}
	if m.Ask {
		t.Fatalf("a plain threaded send/reply must not be flagged Ask, got %+v", m)
	}
}

func TestSendThreadedTagsMessageAndTracksParticipant(t *testing.T) {
	b := newTestBroker()
	m, err := b.SendThreaded("alice", "bob", "starting a DM thread", "root123")
	if err != nil {
		t.Fatal(err)
	}
	if m.ThreadID != "root123" {
		t.Fatalf("expected ThreadID stamped on the message, got %q", m.ThreadID)
	}
	if !b.threadParticipants["root123"]["alice"] {
		t.Fatal("sender should be tracked as a thread participant")
	}
	got := b.Drain("bob", false, 0)
	if len(got) != 1 || got[0].Quiet {
		t.Fatalf("a direct threaded send should still wake normally (only one recipient): %+v", got)
	}
}

func TestDrainThreadFiltersToRootAndReplies(t *testing.T) {
	b := newTestBroker()
	b.Sub("alice", "eng")
	b.Sub("bob", "eng")
	root, _, _ := b.Pub("alice", "eng", "root message")
	b.PubThreaded("alice", "eng", "reply one", root.ID)
	b.Pub("alice", "eng", "unrelated message") // no ThreadID; must NOT show up
	b.PubThreaded("alice", "eng", "reply two", root.ID)

	got := b.DrainThread("bob", root.ID, true, 0) // peek: leave the inbox intact
	if len(got) != 3 {
		t.Fatalf("expected root + 2 replies (3 messages), got %d: %+v", len(got), got)
	}
	bodies := map[string]bool{}
	for _, m := range got {
		bodies[m.Body] = true
	}
	if !bodies["root message"] || !bodies["reply one"] || !bodies["reply two"] {
		t.Fatalf("missing expected thread messages: %+v", got)
	}
	if bodies["unrelated message"] {
		t.Fatal("an unrelated (non-thread) message leaked into the thread view")
	}
	// The unrelated message and everything else should still be in the full inbox.
	full := b.Drain("bob", false, 0)
	if len(full) != 4 {
		t.Fatalf("peek should not have consumed anything; expected 4 total, got %d", len(full))
	}
}

// DrainIfIdle must not drain (or touch the inbox at all) while the agent is
// busy — this is what closes the auto-wake hook's old two-round-trip race
// (a separate "is it busy" ps call, then a separate drain call, with a real
// gap in between for a new turn to start in).
func TestDrainIfIdleStandsDownWhenBusy(t *testing.T) {
	b := newTestBroker()
	b.Send("alice", "bob", "hello")
	b.SetBusy("bob", time.Minute)

	msgs, idle := b.DrainIfIdle("bob", 0, nil)
	if idle {
		t.Fatal("expected idle=false while busy")
	}
	if msgs != nil {
		t.Fatalf("expected no messages while busy, got %+v", msgs)
	}
	// Must still be sitting in the inbox, untouched.
	full := b.Drain("bob", true, 0)
	if len(full) != 1 {
		t.Fatalf("message should still be queued after a stood-down DrainIfIdle: %+v", full)
	}
}

func TestDrainIfIdleDrainsWhenNotBusy(t *testing.T) {
	b := newTestBroker()
	b.Send("alice", "bob", "hello")

	msgs, idle := b.DrainIfIdle("bob", 0, nil)
	if !idle {
		t.Fatal("expected idle=true when not busy")
	}
	if len(msgs) != 1 || msgs[0].Body != "hello" {
		t.Fatalf("expected the queued message, got %+v", msgs)
	}
	// And it's actually consumed — a later drain finds nothing.
	if full := b.Drain("bob", false, 0); len(full) != 0 {
		t.Fatalf("expected the message to be consumed, got %+v", full)
	}
}

// The busy check and the drain must be one atomic operation — SetBusy
// between DrainIfIdle's check and its drain must be impossible since both
// happen under b.mu in the same call, not two separate round trips.
func TestDrainIfIdleClearedBusyStillDrains(t *testing.T) {
	b := newTestBroker()
	b.Send("alice", "bob", "hello")
	b.SetBusy("bob", time.Minute)
	b.ClearBusy("bob")

	msgs, idle := b.DrainIfIdle("bob", 0, nil)
	if !idle || len(msgs) != 1 {
		t.Fatalf("expected a drain once busy clears, got idle=%v msgs=%+v", idle, msgs)
	}
}

// --- export ---

// ExportTopic reads the topic's own log even for a subscriber who never
// consumed anything (unlike Replay, which is per-recipient), and even after
// everyone has unsubscribed (the topic's own history outlives its last
// subscriber leaving).
func TestExportTopicSurvivesUnsubscribe(t *testing.T) {
	b := newTestBroker()
	b.Sub("alice", "eng")
	b.Pub("alice", "eng", "first")
	b.Pub("alice", "eng", "second")
	b.Unsub("alice", "eng") // topic now has zero subscribers

	got := b.ExportTopic("eng", 0)
	if len(got) != 2 || got[0].Body != "first" || got[1].Body != "second" {
		t.Fatalf("expected both messages preserved in topic history: %+v", got)
	}
}

func TestExportTopicRespectsMax(t *testing.T) {
	b := newTestBroker()
	for _, body := range []string{"1", "2", "3"} {
		b.Pub("alice", "eng", body)
	}
	got := b.ExportTopic("eng", 2)
	if len(got) != 2 || got[0].Body != "2" || got[1].Body != "3" {
		t.Fatalf("expected the most recent 2, got %+v", got)
	}
}

// ExportThread/ExportDirect are peek-only (Drain is unaffected) and combine
// already-consumed history with whatever's still queued.
func TestExportThreadAndDirectArePeekOnly(t *testing.T) {
	b := newTestBroker()
	b.Sub("bob", "eng")
	root, _, _ := b.Pub("alice", "eng", "root")
	b.PubThreaded("alice", "eng", "reply", root.ID)
	b.Drain("bob", false, 1) // consume just the root, leaving "reply" still queued

	got := b.ExportThread("bob", root.ID, 0)
	if len(got) != 2 {
		t.Fatalf("expected root (from history) + reply (from inbox), got %+v", got)
	}
	// Peek-only: nothing was consumed by the export itself.
	if len(b.agents["bob"].inbox) != 1 {
		t.Fatalf("export must not consume the still-queued reply: %+v", b.agents["bob"].inbox)
	}

	b.Send("carol", "bob", "a DM")
	direct := b.ExportDirect("bob", "carol", 0)
	if len(direct) != 1 || direct[0].Body != "a DM" {
		t.Fatalf("expected the DM from carol: %+v", direct)
	}
}

func TestExportTopicSnapshotRoundTripSurvivesNoSubscribers(t *testing.T) {
	b := newTestBroker()
	b.Sub("alice", "eng")
	b.Pub("alice", "eng", "hello")
	b.Unsub("alice", "eng")
	snap := b.snapshot()

	b2 := newTestBroker()
	b2.load(snap)
	got := b2.ExportTopic("eng", 0)
	if len(got) != 1 || got[0].Body != "hello" {
		t.Fatalf("topic history should survive a restart even with no subscribers: %+v", got)
	}
	// And it must not resurrect a phantom subscriber entry in ps.
	_, topics := b2.Ps("", true)
	for _, top := range topics {
		if top.Name == "eng" && len(top.Subscribers) != 0 {
			t.Fatalf("restored topic should have zero subscribers, got %+v", top)
		}
	}
}

// --- thread list ---

func TestListThreadsSummarizesTopicThread(t *testing.T) {
	b := newTestBroker()
	b.Sub("alice", "eng")
	b.Sub("bob", "eng")
	root, _, _ := b.Pub("alice", "eng", "root message")
	b.PubThreaded("alice", "eng", "reply one", root.ID)
	b.Pub("alice", "eng", "unrelated message") // must not appear as its own thread
	b.PubThreaded("alice", "eng", "reply two", root.ID)

	got := b.ListThreads("bob")
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 thread, got %d: %+v", len(got), got)
	}
	th := got[0]
	if th.ID != root.ID || th.Kind != KindTopic || th.Topic != "eng" || th.RootBody != "root message" {
		t.Fatalf("unexpected thread summary: %+v", th)
	}
	if th.Replies != 2 {
		t.Fatalf("expected 2 replies, got %d: %+v", th.Replies, th)
	}
}

func TestListThreadsSummarizesDirectThread(t *testing.T) {
	b := newTestBroker()
	root, err := b.Send("alice", "bob", "root dm")
	if err != nil {
		t.Fatal(err)
	}
	// The root itself has no ThreadID; make it discoverable as a thread by
	// having a reply reference its own ID, exactly like `mess reply` does.
	if _, err := b.SendThreaded("alice", "bob", "a reply", root.ID); err != nil {
		t.Fatal(err)
	}

	got := b.ListThreads("bob")
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 thread, got %d: %+v", len(got), got)
	}
	th := got[0]
	if th.Kind != KindDirect || th.Peer != "alice" || th.Replies != 1 {
		t.Fatalf("unexpected direct thread summary: %+v", th)
	}
}

func TestListThreadsEmptyWhenNoThreadsSeen(t *testing.T) {
	b := newTestBroker()
	b.Sub("bob", "eng")
	b.Pub("alice", "eng", "just a plain message")

	if got := b.ListThreads("bob"); len(got) != 0 {
		t.Fatalf("expected no threads, got %+v", got)
	}
}

func TestListThreadsOrdersByMostRecentActivity(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.Sub("bob", "eng")
	rootOld, _, _ := b.Pub("alice", "eng", "old root")
	b.PubThreaded("alice", "eng", "old reply", rootOld.ID)

	now = time.Unix(2000, 0)
	rootNew, _, _ := b.Pub("alice", "eng", "new root")
	b.PubThreaded("alice", "eng", "new reply", rootNew.ID)

	got := b.ListThreads("bob")
	if len(got) != 2 {
		t.Fatalf("expected 2 threads, got %d: %+v", len(got), got)
	}
	if got[0].ID != rootNew.ID {
		t.Fatalf("expected most-recently-active thread first, got %+v", got)
	}
}
