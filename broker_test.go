package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
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
	_, n := b.Broadcast("alice", "", "hello all", false, false)
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

	b.Broadcast("alice", "", "quiet to a --no-broadcast waiter", false, false)
	if b.HasPending("bob", noBroadcast) {
		t.Fatal("a plain broadcast must not satisfy a --no-broadcast wake trigger")
	}
	b.Drain("bob", false, 0)

	b.Broadcast("alice", "", "loud, should wake anyway", true, false)
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
	b.Broadcast("alice", "", "shout", false, false) // bob is registered, receives it
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
	b.Broadcast("alice", "", "fyi", false, false)
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

	if ok, _ := b.RegisterOwned("arise", "sessA", 0, false); !ok {
		t.Fatal("first claim of a free name should succeed")
	}
	if ok, _ := b.RegisterOwned("arise", "sessA", 0, false); !ok {
		t.Fatal("same session re-registering its own name should succeed")
	}
	// A different live session claiming the same name -> collision, regardless of
	// terminal (the host session id is stable, so a new id is a new session).
	if ok, msg := b.RegisterOwned("arise", "sessB", 0, false); ok || msg == "" {
		t.Fatalf("expected a collision, got ok=%v msg=%q", ok, msg)
	}
	// ...but --force takes it over.
	if ok, _ := b.RegisterOwned("arise", "sessB", 0, true); !ok {
		t.Fatal("force should take over")
	}
	// Once the owner is no longer live, another session may take the name.
	now = now.Add(3 * time.Minute)
	if ok, _ := b.RegisterOwned("arise", "sessC", 0, false); !ok {
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
	if ok, _ := b.RegisterOwned("alice", "sessA", 0, false); !ok {
		t.Fatal("register should succeed")
	}
	// The owning session may act as alice.
	if ok, _ := b.ClaimIdentity("alice", "sessA", 0); !ok {
		t.Fatal("owner should be allowed to act as its own name")
	}
	// A different live session must be rejected.
	if ok, msg := b.ClaimIdentity("alice", "sessB", 0); ok || msg == "" {
		t.Fatalf("a foreign live session must be rejected, got ok=%v", ok)
	}
	// No session id -> no enforcement (bare MESS_AGENT run).
	if ok, _ := b.ClaimIdentity("alice", "", 0); !ok {
		t.Fatal("empty session id should skip the ownership check")
	}
	// A free name is claimable by first live use.
	if ok, _ := b.ClaimIdentity("bob", "sessB", 0); !ok {
		t.Fatal("first live use of a free name should claim it")
	}
	if ok, msg := b.ClaimIdentity("bob", "sessA", 0); ok || msg == "" {
		t.Fatalf("bob is now owned by sessB; sessA must be rejected, got ok=%v", ok)
	}
	// Once alice's owner goes stale, a new session may take over.
	now = now.Add(3 * time.Minute)
	if ok, _ := b.ClaimIdentity("alice", "sessB", 0); !ok {
		t.Fatal("takeover of a non-live owner should be allowed")
	}

	// The shared human mailbox ("user") is exempt: any session may read/act on it
	// (so the operator is never locked out of their own inbox).
	if ok, _ := b.ClaimIdentity("user", "sessA", 0); !ok {
		t.Fatal("user mailbox should be claimable")
	}
	if ok, _ := b.ClaimIdentity("user", "sessB", 0); !ok {
		t.Fatal("a different session must still reach the shared user mailbox")
	}
}

func TestRenameMigratesInboxAndSubscriptions(t *testing.T) {
	b := newTestBroker()
	b.RegisterOwned("old", "sessX", 0, false)
	b.Send("peer", "old", "queued for old")
	b.Sub("old", "builds")

	if ok, msg := b.Rename("old", "new", "sessX", 0, false); !ok {
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
	b.RegisterOwned("me", "s1", 0, false)
	b.RegisterOwned("taken", "s2", 0, false) // a different live session

	if ok, msg := b.Rename("me", "taken", "s1", 0, false); ok || msg == "" {
		t.Fatalf("rename onto a live name should be refused, got ok=%v", ok)
	}
	if ok, _ := b.Rename("me", "taken", "s1", 0, true); !ok {
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
	b.RegisterOwned("bob", "s", 0, false)
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

// maxHistory bounds a.history to the most recent 50 consumed messages, so a
// long-running, chatty agent's replay cache can't grow unboundedly (real
// concern: the live fleet has agents that have run continuously for days).
// This had zero test coverage before the concurrency/crash-recovery audit —
// a future refactor could silently drop the cap with nothing to catch it.
func TestHistoryCapsAtMaxHistory(t *testing.T) {
	b := newTestBroker()
	for i := 0; i < maxHistory+20; i++ {
		b.Send("peer", "bob", fmt.Sprintf("msg-%d", i))
	}
	b.Drain("bob", false, 0) // consume everything at once -> history

	got := b.Replay("bob", 0)
	if len(got) != maxHistory {
		t.Fatalf("expected history capped at %d, got %d", maxHistory, len(got))
	}
	// The cap keeps the MOST RECENT entries, not the oldest.
	if got[0].Body != fmt.Sprintf("msg-%d", 20) {
		t.Fatalf("expected history to have dropped the oldest entries, first is %q", got[0].Body)
	}
	if got[len(got)-1].Body != fmt.Sprintf("msg-%d", maxHistory+19) {
		t.Fatalf("expected the newest entry retained, last is %q", got[len(got)-1].Body)
	}
}

// Same cap, but hit incrementally (one drain per message) rather than one
// giant drain — a different code path (history append happens on every
// drainMatchingLocked call, not just a bulk one), worth covering separately
// since the two could plausibly diverge in a future edit.
func TestHistoryCapsAtMaxHistoryIncrementally(t *testing.T) {
	b := newTestBroker()
	for i := 0; i < maxHistory+20; i++ {
		b.Send("peer", "bob", fmt.Sprintf("msg-%d", i))
		b.Drain("bob", false, 0)
	}
	got := b.Replay("bob", 0)
	if len(got) != maxHistory {
		t.Fatalf("expected history capped at %d after incremental drains, got %d", maxHistory, len(got))
	}
	if got[len(got)-1].Body != fmt.Sprintf("msg-%d", maxHistory+19) {
		t.Fatalf("expected the newest entry retained, last is %q", got[len(got)-1].Body)
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
	if ok, msg := b.RegisterOwned(agentKey("A", "admin"), "sessA", 0, false); !ok {
		t.Fatalf("first room's admin should register: %s", msg)
	}
	if ok, msg := b.RegisterOwned(agentKey("B", "admin"), "sessB", 0, false); !ok {
		t.Fatalf("second room's admin should register independently: %s", msg)
	}
	// But within the SAME room, the usual collision guard still applies.
	if ok, msg := b.RegisterOwned(agentKey("A", "admin"), "sessC", 0, false); ok || msg == "" {
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

	_, n := b.Broadcast(agentKey("A", "alice"), "A", "hello room A", false, false)
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
	_, n := b.Broadcast(agentKey("A", "alice"), "A", "restarting the daemon", true, true)
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
	_, n := b.Broadcast(agentKey("A", "alice"), "A", "loud but room-scoped", true, false)
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
	b.RegisterOwned(agentKey("A", "old"), "sessX", 0, false)
	b.Send("peer", agentKey("A", "old"), "queued for old")
	b.Sub(agentKey("A", "old"), topicKey("A", "builds"))

	if ok, msg := b.Rename(agentKey("A", "old"), agentKey("A", "new"), "sessX", 0, false); !ok {
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
	b.RegisterOwned(agentKey("frontend", "bob"), "sess1", 0, false)

	rooms, found := b.FindOtherRoom("", "bob")
	if !found || len(rooms) != 1 || rooms[0] != "frontend" {
		t.Fatalf("expected to find bob in room \"frontend\", got rooms=%q found=%v", rooms, found)
	}
}

// The same bare name registered in several rooms must report ALL of them. It
// used to return the first hit from a randomized map iteration, so the
// "it lives in room X" error named a different room on each run — a guess
// presented as a fact, which a caller then acted on.
func TestFindOtherRoomReportsEveryRoomDeterministically(t *testing.T) {
	b := newTestBroker()
	b.RegisterOwned(agentKey("roomB", "dup"), "s1", 0, false)
	b.RegisterOwned(agentKey("roomA", "dup"), "s2", 0, false)

	for range 8 { // would have flapped across runs before
		rooms, found := b.FindOtherRoom("", "dup")
		if !found || len(rooms) != 2 || rooms[0] != "roomA" || rooms[1] != "roomB" {
			t.Fatalf("want both rooms, sorted, every time; got %q", rooms)
		}
	}
}

func TestFindOtherRoomNoMatchInCallersOwnRoom(t *testing.T) {
	b := newTestBroker()
	b.RegisterOwned(agentKey("frontend", "bob"), "sess1", 0, false)

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
	b.RegisterOwned("bob", "sess1", 0, false)
	b.Send("peer", "bob", "queued before joining")

	if ok, msg := b.JoinRoom("bob", agentKey("frontend", "bob"), "sess1", 0, false); !ok {
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
	b.RegisterOwned(agentKey("frontend", "taken"), "s2", 0, false) // a different live session

	if ok, msg := b.JoinRoom("me", agentKey("frontend", "taken"), "s1", 0, false); ok || msg == "" {
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
	b.RegisterOwned(agentKey("frontend", "taken"), "s2", 0, false) // a different live session

	// An attacker-ish session claims FromRoom == the target room, hoping to
	// hit Rename-style same-key fast-path semantics and skip the guard.
	if ok, msg := b.JoinRoom(agentKey("frontend", "taken"), agentKey("frontend", "taken"), "s1", 0, false); ok || msg == "" {
		t.Fatal("from==who must not bypass the collision guard")
	}
}

func TestJoinRoomForceOverridesCollision(t *testing.T) {
	now := time.Unix(1000, 0)
	b := NewBroker()
	b.now = func() time.Time { return now }
	b.RegisterOwned(agentKey("frontend", "taken"), "s2", 0, false)

	if ok, msg := b.JoinRoom("me", agentKey("frontend", "taken"), "s1", 0, true); !ok {
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

// Real bug, caught live: b.owners (registration/ownership) was never
// persisted at all — snapshot()/load() round-tripped inbox/topics/state/
// lastSeen but silently dropped ownership entirely, so EVERY daemon
// restart wiped registration status fleet-wide. b.agents (and therefore
// `mess ps`'s "online"/pending view) survived restart intact, masking it —
// an agent LOOKED present and active, but IsRegistered said no, so
// send/ask's registered-recipient guard would incorrectly reject it as a
// "no such agent" until it happened to take some action that re-claims
// ownership via ClaimIdentity. Most agents self-heal near-instantly since
// almost any activity re-registers them; a quiet or session-less one
// would not. Caught because a live agent that hadn't acted since a restart
// this session triggered genuinely got rejected.
func TestSnapshotRoundTripsOwnership(t *testing.T) {
	b := newTestBroker()
	b.RegisterOwned("alice", "sess-alice", 0, false)
	b.RegisterOwned(agentKey("frontend", "bob"), "sess-bob", 0, false)
	b.Send("peer", "carol", "hi") // carol: known (ensure'd) but never registered — a ghost

	b2 := newTestBroker()
	b2.load(b.snapshot())

	if !b2.IsRegistered("alice") {
		t.Fatal("alice's registration should survive a snapshot round trip")
	}
	if !b2.IsRegistered(agentKey("frontend", "bob")) {
		t.Fatal("bob's room-scoped registration should survive a snapshot round trip")
	}
	if b2.IsRegistered("carol") {
		t.Fatal("carol was never registered — must not become registered via the round trip")
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

// Presence used to be inferred from three proxies that can all outlive the
// session they stand for. `mess busy` carries a one-hour crash backstop, so a
// session that died mid-turn read `working` — and `online` — for up to an hour
// with no process behind it. A real probe of the owning process must override
// that.
func TestDeadSessionIsNotOnlineOrWorking(t *testing.T) {
	b := NewBroker()
	live := map[int]string{4242: "claude"}
	b.probeComm = func(pid int) string { return live[pid] }

	if ok, _ := b.RegisterOwned("alice", "sessA", 4242, false); !ok {
		t.Fatal("register failed")
	}
	b.SetBusy("alice", time.Hour) // the default backstop
	if !b.IsOnline("alice") || !b.IsWorking("alice") {
		t.Fatal("a live busy session should read online and working")
	}

	delete(live, 4242) // the session's process is gone; busyUntil is still an hour out
	if b.IsWorking("alice") {
		t.Fatal("a dead session must not read `working` just because busy hasn't expired")
	}
	if b.IsOnline("alice") {
		t.Fatal("a dead session must not read online")
	}
}

// An orphaned wake hook keeps its parked listener, which was the other way a
// dead session read online — senders saw listening=true and believed their wake
// had landed with nothing behind it.
func TestDeadSessionWithAParkedListenerIsNotOnline(t *testing.T) {
	b := NewBroker()
	live := map[int]string{99: "claude"}
	b.probeComm = func(pid int) string { return live[pid] }
	b.RegisterOwned("ghost", "sessG", 99, false)
	b.AddListener("ghost")

	if !b.IsOnline("ghost") {
		t.Fatal("a live session with a listener should read online")
	}
	delete(live, 99)
	if b.IsOnline("ghost") {
		t.Fatal("an orphaned listener must not keep a dead session online")
	}
}

// The probe must only ever act on evidence. An agent with no recorded pid (no
// session id, an unrecognized harness, no procfs) has to keep working exactly
// as before, or the fix would silently take every such agent offline.
func TestUnknownSessionPIDNeverCountsAsDead(t *testing.T) {
	b := NewBroker()
	b.probeComm = func(int) string { return "" } // nothing is alive, as far as the probe knows
	b.RegisterOwned("nopid", "sessN", 0, false)  // ...but we never recorded a pid
	b.SetBusy("nopid", time.Hour)

	if !b.IsOnline("nopid") || !b.IsWorking("nopid") {
		t.Fatal("absence of a pid must not be treated as a dead session")
	}
}

// A recycled pid must not resurrect a dead session: the recorded executable
// name has to match too.
func TestRecycledPIDIsNotTheSameSession(t *testing.T) {
	b := NewBroker()
	live := map[int]string{777: "claude"}
	b.probeComm = func(pid int) string { return live[pid] }
	b.RegisterOwned("alice", "sessA", 777, false)
	b.SetBusy("alice", time.Hour)

	live[777] = "sshd" // same pid, different program
	if b.IsOnline("alice") {
		t.Fatal("a reused pid running something else must not count as the original session")
	}
}

// This is what let an orphan poison its own replacement: the ownership guard
// asks aliveLocked, so a dead-but-online-looking owner made every operation by
// the relaunched session fail with "in use by another live session" — which
// both hooks then swallowed as "no mail".
func TestDeadOwnerReleasesTheNameToARelaunch(t *testing.T) {
	b := NewBroker()
	live := map[int]string{1: "claude"}
	b.probeComm = func(pid int) string { return live[pid] }
	b.RegisterOwned("agent", "old-session", 1, false)
	b.AddListener("agent") // its wake hook is still parked

	if ok, _ := b.ClaimIdentity("agent", "new-session", 2); ok {
		t.Fatal("setup: a genuinely live owner should still be protected")
	}
	delete(live, 1) // the old session dies without cleaning up
	if ok, msg := b.ClaimIdentity("agent", "new-session", 2); !ok {
		t.Fatalf("a relaunch must be able to reclaim its own name from a dead session: %s", msg)
	}
}

// EndSession is the clean path the SessionEnd hook takes. It must drop presence
// without destroying anything: the identity stays registered and queued mail
// stays queued, because a session ending is not the same as leaving.
func TestEndSessionDropsPresenceButKeepsIdentityAndMail(t *testing.T) {
	b := NewBroker()
	b.RegisterOwned("alice", "sessA", 0, false)
	b.Send("bob", "alice", "unread when the session ended")
	b.SetBusy("alice", time.Hour)
	evicted := b.WatchEvict("alice")

	b.EndSession("alice")

	if b.IsWorking("alice") {
		t.Fatal("session-end must clear the in-a-turn flag")
	}
	select {
	case <-evicted:
	default:
		t.Fatal("session-end must evict the parked waiter, so it releases its listener and lock")
	}
	if !b.IsRegistered("alice") {
		t.Fatal("session-end must not unregister — the same name usually comes right back")
	}
	if got := b.Drain("alice", false, 0); len(got) != 1 {
		t.Fatalf("session-end must not discard delivered mail, got %d messages", len(got))
	}
}

// A broadcast wakes nobody by design (waiters park with --no-broadcast, which
// is what stops every fleet announcement being a wake storm), so there was no
// way to single one agent out without shouting at the whole room. An @mention
// now wakes the agent it names — the mirror of topics, where a mention narrows
// the wake instead of adding one, because a topic's baseline is "wakes all".
func TestBroadcastMentionWakesOnlyTheMentioned(t *testing.T) {
	b := newTestBroker()
	b.Register("bob")
	b.Register("carol")
	noBroadcast := map[string]bool{KindDirect: true, KindTopic: true} // the auto-wake hook's filter

	b.Broadcast("alice", "", "@bob can you look at the deploy?", false, false)

	if !b.HasPending("bob", noBroadcast) {
		t.Fatal("an @mentioned agent must be woken by a broadcast")
	}
	if b.HasPending("carol", noBroadcast) {
		t.Fatal("an unmentioned agent must NOT be woken — that would make every mention a wake storm")
	}
	// Carol still RECEIVES it; she just reads it on her next recv.
	if got := b.Drain("carol", false, 0); len(got) != 1 {
		t.Fatalf("an unmentioned agent must still receive the broadcast, got %d", len(got))
	}
	// Only the mentioned copy carries the wake override.
	got := b.Drain("bob", false, 0)
	if len(got) != 1 || !got[0].Loud {
		t.Fatalf("the mentioned copy should be marked loud, got %+v", got)
	}
}

// The mention must not become a back door through the room boundary: a
// broadcast still only reaches the sender's own room unless it is host-wide.
func TestBroadcastMentionDoesNotCrossRooms(t *testing.T) {
	b := newTestBroker()
	b.Register(agentKey("coord", "bob"))
	noBroadcast := map[string]bool{KindDirect: true, KindTopic: true}

	b.Broadcast("alice", "", "@bob urgent", false, false) // alice is in the global room
	if b.HasPending(agentKey("coord", "bob"), noBroadcast) {
		t.Fatal("an @mention must not carry a broadcast across a room boundary")
	}
	if got := b.Drain(agentKey("coord", "bob"), false, 0); len(got) != 0 {
		t.Fatalf("a room-scoped broadcast must not reach another room at all, got %+v", got)
	}

	// Host-wide is the explicit opt-in, and the mention still wakes.
	b.Broadcast("alice", "", "@bob urgent", false, true)
	if !b.HasPending(agentKey("coord", "bob"), noBroadcast) {
		t.Fatal("a host-wide broadcast's @mention should wake across rooms")
	}
}

// A mention of a name nobody has must be inert, not an error or a ghost agent.
func TestBroadcastMentionOfAnUnknownNameIsHarmless(t *testing.T) {
	b := newTestBroker()
	b.Register("bob")
	_, n := b.Broadcast("alice", "", "@nobody-here ping", false, false)
	if n != 1 {
		t.Fatalf("want delivery to the one real agent, got %d", n)
	}
	if b.IsRegistered("nobody-here") {
		t.Fatal("mentioning a name must not conjure an agent")
	}
}

// --- an @mention as an answer to `mess ask` ---
//
// `mess ask` blocks until a message threaded under its token arrives, and the
// single most common way to lose an answer is to reply with a plain `mess send`
// instead of `mess reply`: the asker blocks to timeout while a perfectly good
// answer sits unthreaded in its inbox (a documented, repeatedly-hit incident).
// An @mention of the asker, from the agent asked, sent after the question, is a
// strong enough signal of "this is my answer" to count.

// askSetup wires an ask from asker to askee and returns its token.
func askSetup(t *testing.T, b *Broker, asker, askee, question string) string {
	t.Helper()
	b.Register(asker)
	b.Register(askee)
	m, err := b.SendAsk(asker, askee, question)
	if err != nil {
		t.Fatalf("ask failed: %v", err)
	}
	return m.ID
}

func TestMentionFromTheAskeeAnswersAnAsk(t *testing.T) {
	b := newTestBroker()
	token := askSetup(t, b, "alice", "bob", "ready to deploy?")

	// bob answers with a plain send — no thread, just a mention.
	b.Send("bob", "alice", "@alice yes, ready")

	if !b.HasPendingAnswer("alice", token) {
		t.Fatal("an @mention from the agent asked should count as an answer")
	}
	got := b.DrainAnswers("alice", token, false, 0)
	if len(got) != 1 || got[0].Body != "@alice yes, ready" {
		t.Fatalf("the answer should be handed to the asker, got %+v", got)
	}
}

// The canonical threaded reply must keep working exactly as before.
func TestThreadedReplyStillAnswersAnAsk(t *testing.T) {
	b := newTestBroker()
	token := askSetup(t, b, "alice", "bob", "ready?")

	b.SendThreaded("bob", "alice", "yes", token)

	if !b.HasPendingAnswer("alice", token) {
		t.Fatal("a threaded reply must still answer")
	}
	if got := b.DrainAnswers("alice", token, false, 0); len(got) != 1 {
		t.Fatalf("want the threaded reply, got %+v", got)
	}
}

// TWO questions outstanding between the same pair, and ONE mention. The mention
// carries nothing that says WHICH question it answers — the matching rule (from
// the askee, postdates the ask, mentions the asker) is satisfied by both tokens
// equally. So it must answer NEITHER: handing the same body to both waiters
// resolves one of them with an answer to a question nobody asked, and the asker
// cannot tell, because a wrong answer and a right one render identically.
//
// A threaded reply stays unambiguous by construction, which is why the fallback
// is "require the thread" rather than "pick one".
func TestAnAmbiguousMentionAnswersNeitherAsk(t *testing.T) {
	b := newTestBroker()
	first := askSetup(t, b, "alice", "bob", "deploy now?")
	second := askSetup(t, b, "alice", "bob", "roll back?")

	b.Send("bob", "alice", "@alice yes")

	if b.HasPendingAnswer("alice", first) || b.HasPendingAnswer("alice", second) {
		t.Fatal("a mention that could answer either pending ask must answer neither — " +
			"otherwise one waiter resolves on an answer to the other's question")
	}
}

// Suppressing the ambiguous mention is only half a fix: on its own it recreates
// the failure the mention rule was added for (asker blocks to timeout, answer
// sits unread). The sender has to be TOLD, so the refusal is a message and not a
// silence.
func TestAnAmbiguousMentionIsReportedToItsSender(t *testing.T) {
	b := newTestBroker()
	first := askSetup(t, b, "alice", "bob", "deploy now?")
	second := askSetup(t, b, "alice", "bob", "roll back?")

	m, err := b.Send("bob", "alice", "@alice yes")
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}

	asker, tokens := b.AmbiguousMentionTokens(m)
	if asker != "alice" {
		t.Errorf("the warning must name the asker, got %q", asker)
	}
	want := []string{first, second}
	sort.Strings(want)
	if !slices.Equal(tokens, want) {
		t.Fatalf("both open asks should be reported, sorted: got %v want %v", tokens, want)
	}
}

// The common case stays quiet: one open question is not ambiguous, and a warning
// on every ordinary answered ask would train people to ignore the warning.
func TestASingleOpenAskIsNotReportedAsAmbiguous(t *testing.T) {
	b := newTestBroker()
	token := askSetup(t, b, "alice", "bob", "deploy now?")

	m, err := b.Send("bob", "alice", "@alice yes")
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}

	if _, tokens := b.AmbiguousMentionTokens(m); len(tokens) != 0 {
		t.Fatalf("one open ask is unambiguous, got %v", tokens)
	}
	if !b.HasPendingAnswer("alice", token) {
		t.Fatal("and it must still answer, which is the whole point of the mention rule")
	}
}

// The disambiguation that still works with two outstanding: the thread. This is
// the half that must NOT regress when ambiguous mentions stop counting.
func TestAThreadedReplyStillAnswersWithTwoAsksOutstanding(t *testing.T) {
	b := newTestBroker()
	first := askSetup(t, b, "alice", "bob", "deploy now?")
	second := askSetup(t, b, "alice", "bob", "roll back?")

	b.SendThreaded("bob", "alice", "yes to the second", second)

	if b.HasPendingAnswer("alice", first) {
		t.Error("a reply threaded under the second ask must not answer the first")
	}
	if !b.HasPendingAnswer("alice", second) {
		t.Fatal("a threaded reply must answer its own ask even with another outstanding")
	}
	if got := b.DrainAnswers("alice", second, false, 0); len(got) != 1 || got[0].Body != "yes to the second" {
		t.Fatalf("want the threaded reply, got %+v", got)
	}
}

// The rule has to be tight, or every ask resolves on the first passing mention
// and the answer it returns is noise.
func TestUnqualifiedMessagesDoNotAnswerAnAsk(t *testing.T) {
	cases := []struct {
		name string
		send func(b *Broker)
	}{
		{"a mention from a third party", func(b *Broker) {
			b.Register("carol")
			b.Send("carol", "alice", "@alice unrelated chatter")
		}},
		{"the askee replying with no mention at all", func(b *Broker) {
			b.Send("bob", "alice", "yes")
		}},
		{"a mention of somebody else", func(b *Broker) {
			b.Send("bob", "alice", "@carol can you look?")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBroker()
			token := askSetup(t, b, "alice", "bob", "ready?")
			tc.send(b)
			if b.HasPendingAnswer("alice", token) {
				t.Fatalf("%s must not satisfy an ask", tc.name)
			}
		})
	}
}

// A mention that predates the question can't be its answer — otherwise an ask
// resolves instantly against stale mail already sitting in the inbox.
func TestAMentionOlderThanTheAskDoesNotAnswerIt(t *testing.T) {
	b := newTestBroker()
	b.Register("alice")
	b.Register("bob")
	b.Send("bob", "alice", "@alice about that other thing") // BEFORE the ask

	m, err := b.SendAsk("alice", "bob", "ready?")
	if err != nil {
		t.Fatal(err)
	}
	if b.HasPendingAnswer("alice", m.ID) {
		t.Fatal("a message that predates the question must not answer it")
	}
}

// recv --thread is a "show me this conversation" query, not an ask: it must
// stay strictly thread-scoped rather than sweeping up mentions.
func TestRecvThreadStaysThreadScoped(t *testing.T) {
	b := newTestBroker()
	token := askSetup(t, b, "alice", "bob", "ready?")
	b.Send("bob", "alice", "@alice yes")

	if got := b.DrainThread("alice", token, true, 0); len(got) != 0 {
		t.Fatalf("recv --thread must not pull in a mention, got %+v", got)
	}
	if got := b.DrainAnswers("alice", token, true, 0); len(got) != 1 {
		t.Fatalf("...but the ask itself should still see it, got %+v", got)
	}
}

// The pending-ask table must not grow without bound: an ask that is never
// answered and never awaited would otherwise leak an entry forever.
func TestPendingAskTableIsBounded(t *testing.T) {
	b := newTestBroker()
	b.Register("alice")
	b.Register("bob")
	for i := 0; i < maxPendingAsks+50; i++ {
		if _, err := b.SendAsk("alice", "bob", "q"); err != nil {
			t.Fatal(err)
		}
	}
	b.mu.Lock()
	n := len(b.asks)
	b.mu.Unlock()
	if n > maxPendingAsks {
		t.Fatalf("asks table grew to %d, past the %d cap", n, maxPendingAsks)
	}
}

// An @mention of someone who isn't subscribed reaches nobody, and used to do so
// in total silence on BOTH ends — the sender believed they had told someone who
// never heard it. That cost a real message: a peer was addressed inside a topic
// they were not subscribed to, and neither side had any signal.
func TestMentionOfANonSubscriberIsReported(t *testing.T) {
	b := newTestBroker()
	b.Register("trail")
	b.Register("coord")
	b.Register("fable")
	b.Sub("trail", "peri")
	b.Sub("coord", "peri")

	unreached, members := b.MentionsNotSubscribed("peri", "@fable I will write it up")
	if len(unreached) != 1 || unreached[0] != "fable" {
		t.Fatalf("an unsubscribed mention must be reported, got %v", unreached)
	}
	// The subscriber list comes back too, so the sender can fix it without a
	// second command — the field they said they'd want in front of pub output.
	if len(members) != 2 || members[0] != "coord" || members[1] != "trail" {
		t.Fatalf("want the sorted subscriber list, got %v", members)
	}

	// A mention that IS subscribed must stay silent, or the warning is noise.
	if got, _ := b.MentionsNotSubscribed("peri", "@coord please review"); len(got) != 0 {
		t.Fatalf("a reachable mention must not warn, got %v", got)
	}
	// ...and so must a message with no mentions at all.
	if got, _ := b.MentionsNotSubscribed("peri", "no mentions here"); len(got) != 0 {
		t.Fatalf("unmentioned body must not warn, got %v", got)
	}
}

// The same hole exists on broadcast, which only reaches one room.
func TestMentionOutsideTheBroadcastsRoomIsReported(t *testing.T) {
	b := newTestBroker()
	b.Register("alice")
	b.Register(agentKey("coord", "bob"))

	if got := b.MentionsNotInRoom("", "@bob urgent", false); len(got) != 1 || got[0] != "bob" {
		t.Fatalf("a mention of someone in another room must be reported, got %v", got)
	}
	// Host-wide really does reach them, so there is nothing to warn about.
	if got := b.MentionsNotInRoom("", "@bob urgent", true); len(got) != 0 {
		t.Fatalf("host-wide reaches every room; no warning expected, got %v", got)
	}
	// The operator handle is reachable from anywhere and is not a membership.
	if got := b.MentionsNotInRoom("", "@user look at this", false); len(got) != 0 {
		t.Fatalf("the human mailbox must never be reported unreachable, got %v", got)
	}
}

// A mention refused as ambiguous must STAY refused. Ambiguity is a fact about
// the moment the message was sent — how many questions were open then — so the
// verdict is recorded and never revisited. Without that the suppression
// silently expired: answering one of the two asks left the other as the only
// candidate, and the message that deliberately answered NOTHING became its
// answer. That is precisely the failure the suppression exists to prevent, just
// delayed until nobody is watching.
func TestAnAmbiguousMentionStaysRefusedAfterTheOtherAskResolves(t *testing.T) {
	b := newTestBroker()
	b.Register("alice")
	b.Register("bob")
	one, err := b.SendAsk("alice", "bob", "question one")
	if err != nil {
		t.Fatal(err)
	}
	two, err := b.SendAsk("alice", "bob", "question two")
	if err != nil {
		t.Fatal(err)
	}

	m, _ := b.Send("bob", "alice", "@alice yes")
	if _, tokens := b.AmbiguousMentionTokens(m); len(tokens) != 2 {
		t.Fatalf("a mention with two open asks should be ambiguous across both, got %v", tokens)
	}
	if b.HasPendingAnswer("alice", one.ID) || b.HasPendingAnswer("alice", two.ID) {
		t.Fatal("an ambiguous mention must answer neither ask")
	}

	// Resolve one the unambiguous way, which removes it from the open set.
	b.SendThreaded("bob", "alice", "answer to one", one.ID)
	if got := b.DrainAnswers("alice", one.ID, false, 0); len(got) != 1 {
		t.Fatalf("the threaded reply should answer its own ask, got %+v", got)
	}

	// The survivor must not inherit the suppressed mention.
	if b.HasPendingAnswer("alice", two.ID) {
		t.Fatal("resolving one ask promoted a previously-refused mention into the other's answer")
	}
	// ...but a genuinely new mention, now unambiguous, still answers it.
	b.Send("bob", "alice", "@alice yes to the remaining one")
	if !b.HasPendingAnswer("alice", two.ID) {
		t.Fatal("a fresh mention with only one ask open should answer it")
	}
}

// --- invitations ---
//
// Instead of DMing a peer to go subscribe themselves, you invite them and they
// accept. The invitation IS an ordinary message, so it wakes/replays/threads
// like any other and a recipient who has never heard of invitations still gets
// a sentence telling them what to run. Joining stays THEIR action throughout:
// an invite that subscribed someone on their behalf would be one identity
// acting as another, which is the hazard rooms and ownership exist to prevent.

func TestInviteIsAMessageAndAcceptJoins(t *testing.T) {
	b := newTestBroker()
	b.Register("trail")
	b.Register("fable")
	b.Sub("trail", topicKey("", "peri"))

	m, err := b.Invite("trail", "fable", "peri", "", "come look")
	if err != nil {
		t.Fatal(err)
	}
	// The recipient must be able to tell it apart from any other message.
	if m.Invite != "#peri" {
		t.Fatalf("the delivered message must carry what it invites to, got %q", m.Invite)
	}
	if got := b.Drain("fable", true, 0); len(got) != 1 || got[0].Invite != "#peri" {
		t.Fatalf("the invitation should arrive as an ordinary inbox message, got %+v", got)
	}

	inv, err := b.LookupInvite("fable", m.ID)
	if err != nil {
		t.Fatal(err)
	}
	b.Sub("fable", topicKey(inv.room, inv.topic))
	if !b.IsSubscribed("fable", topicKey("", "peri")) {
		t.Fatal("accepting should subscribe the invitee")
	}
}

// The token names one agent's decision. It is not a capability that travels.
func TestOnlyTheInviteeCanAccept(t *testing.T) {
	b := newTestBroker()
	for _, n := range []string{"trail", "fable", "nosy"} {
		b.Register(n)
	}
	b.Sub("trail", topicKey("", "peri"))
	m, err := b.Invite("trail", "fable", "peri", "", "")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := b.LookupInvite("nosy", m.ID); err == nil {
		t.Fatal("a third party must not be able to redeem someone else's invitation")
	}
	if _, err := b.LookupInvite("trail", m.ID); err == nil {
		t.Fatal("not even the sender may accept on the invitee's behalf")
	}
	if _, err := b.LookupInvite("fable", m.ID); err != nil {
		t.Fatalf("the invitee must be able to redeem it: %v", err)
	}
	// Spent once redeemed.
	b.ClearInvite(m.ID)
	if _, err := b.LookupInvite("fable", m.ID); err == nil {
		t.Fatal("a redeemed invitation must not be reusable")
	}
}

// You may only invite into something you are in yourself — otherwise it is not
// an invitation, it is a suggestion about someone else's business.
func TestCannotInviteIntoSomethingYouAreNotIn(t *testing.T) {
	b := newTestBroker()
	b.Register("nosy")
	b.Register("fable")

	if _, err := b.Invite("nosy", "fable", "builds", "", ""); err == nil {
		t.Fatal("inviting into an unfollowed topic should be refused")
	}
	if _, err := b.Invite("nosy", "fable", "", "someroom", ""); err == nil {
		t.Fatal("inviting into a room you are not in should be refused")
	}
}

// Topics are room-scoped, so a topic invitation across a room boundary would
// quietly punch through the isolation. It is refused, with advice that can
// actually be typed — the global room has no name, so it cannot be an invite
// target and saying "invite them to (global)" would be the same untypeable
// suggestion the cross-room error used to make.
func TestTopicInviteAcrossRoomsIsRefusedWithTypeableAdvice(t *testing.T) {
	b := newTestBroker()
	b.Register("trail")
	b.Register(agentKey("coord", "away"))
	b.Sub("trail", topicKey("", "builds"))

	_, err := b.Invite("trail", agentKey("coord", "away"), "builds", "", "")
	if err == nil {
		t.Fatal("a topic invitation across rooms should be refused")
	}
	if strings.Contains(err.Error(), "invite coord/away (global)") {
		t.Fatalf("advice must be typeable, not a display label: %v", err)
	}
	if !strings.Contains(err.Error(), "room leave") {
		t.Fatalf("from the global room the honest advice is the command that works: %v", err)
	}

	// From a NAMED room there is a real command to offer.
	b.Register(agentKey("team", "host"))
	b.Register(agentKey("coord", "other"))
	b.Sub(agentKey("team", "host"), topicKey("team", "work"))
	_, err = b.Invite(agentKey("team", "host"), agentKey("coord", "other"), "work", "", "")
	if err == nil || !strings.Contains(err.Error(), "mess invite coord/other team") {
		t.Fatalf("want a runnable room invitation to offer, got %v", err)
	}
}

// The pending-invitation table must not grow without bound.
func TestPendingInviteTableIsBounded(t *testing.T) {
	b := newTestBroker()
	b.Register("trail")
	b.Register("fable")
	b.Sub("trail", topicKey("", "peri"))
	for range maxPendingInvites + 25 {
		if _, err := b.Invite("trail", "fable", "peri", "", "x"); err != nil {
			t.Fatal(err)
		}
	}
	b.mu.Lock()
	n := len(b.invites)
	b.mu.Unlock()
	if n > maxPendingInvites {
		t.Fatalf("invites table grew to %d, past the %d cap", n, maxPendingInvites)
	}
}
