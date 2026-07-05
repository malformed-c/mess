package main

import "testing"

func TestLastMsgRoundTrip(t *testing.T) {
	t.Setenv("MESS_DIR", t.TempDir())
	clearSessionEnv(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "sess-1")
	p := resolvePaths()

	if _, ok := readLastMsg(p); ok {
		t.Fatal("expected no last message initially")
	}
	writeLastMsg(p, lastMsgInfo{ID: "m1", Kind: KindTopic, Topic: "eng", From: "alice"})
	got, ok := readLastMsg(p)
	if !ok || got.ID != "m1" || got.Kind != KindTopic || got.Topic != "eng" || got.From != "alice" {
		t.Fatalf("round trip failed: %+v ok=%v", got, ok)
	}
	// A later message overwrites the earlier one.
	writeLastMsg(p, lastMsgInfo{ID: "m2", Kind: KindDirect, From: "bob"})
	got, ok = readLastMsg(p)
	if !ok || got.ID != "m2" || got.Kind != KindDirect || got.From != "bob" {
		t.Fatalf("overwrite failed: %+v ok=%v", got, ok)
	}
}

func TestOpenThreadRoundTripAndClear(t *testing.T) {
	t.Setenv("MESS_DIR", t.TempDir())
	clearSessionEnv(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "sess-2")
	p := resolvePaths()

	if _, ok := readOpenThread(p); ok {
		t.Fatal("expected no open thread initially")
	}
	if err := writeOpenThread(p, openThreadInfo{ThreadID: "m1", Kind: KindTopic, Topic: "eng"}); err != nil {
		t.Fatal(err)
	}
	got, ok := readOpenThread(p)
	if !ok || got.ThreadID != "m1" || got.Kind != KindTopic || got.Topic != "eng" {
		t.Fatalf("round trip failed: %+v ok=%v", got, ok)
	}
	if err := clearOpenThread(p); err != nil {
		t.Fatal(err)
	}
	if _, ok := readOpenThread(p); ok {
		t.Fatal("expected no open thread after clear")
	}
	// Clearing an already-clear thread is not an error (mirrors clearIdentity).
	if err := clearOpenThread(p); err != nil {
		t.Fatalf("clearing an absent open thread should be a no-op, got %v", err)
	}
}

func TestOpenThreadRequiresSessionID(t *testing.T) {
	t.Setenv("MESS_DIR", t.TempDir())
	clearSessionEnv(t)
	p := resolvePaths()
	if err := writeOpenThread(p, openThreadInfo{ThreadID: "m1"}); err == nil {
		t.Fatal("expected error when no session id is available")
	}
}

// updateLastMsg picks the newest direct/topic message, skipping broadcasts
// (which have no coherent reply target).
func TestUpdateLastMsgSkipsBroadcastsAndPicksNewest(t *testing.T) {
	t.Setenv("MESS_DIR", t.TempDir())
	clearSessionEnv(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "sess-3")
	p := resolvePaths()

	msgs := []Message{
		{ID: "m1", Kind: KindTopic, Topic: "eng", From: "alice"},
		{ID: "m2", Kind: KindBroadcast, From: "bob"},
		{ID: "m3", Kind: KindDirect, From: "carol"},
	}
	updateLastMsg(p, msgs)
	got, ok := readLastMsg(p)
	if !ok || got.ID != "m3" || got.Kind != KindDirect || got.From != "carol" {
		t.Fatalf("expected the newest non-broadcast message (m3), got %+v ok=%v", got, ok)
	}
}

// If the newest message is itself a threaded reply, updateLastMsg must target
// the thread's root — not the reply's own ID — so a further `mess reply` stays
// flat under the same root instead of spawning a reply-to-a-reply sub-thread.
func TestUpdateLastMsgTargetsThreadRootNotReplyID(t *testing.T) {
	t.Setenv("MESS_DIR", t.TempDir())
	clearSessionEnv(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "sess-5")
	p := resolvePaths()

	msgs := []Message{
		{ID: "m5", Kind: KindTopic, Topic: "eng", From: "alice"},                 // root
		{ID: "m6", Kind: KindTopic, Topic: "eng", From: "alice", ThreadID: "m5"}, // reply
	}
	updateLastMsg(p, msgs)
	got, ok := readLastMsg(p)
	if !ok || got.ID != "m5" {
		t.Fatalf("expected the thread root m5, got %+v ok=%v", got, ok)
	}
}

// If the newest message is a broadcast, updateLastMsg falls back to the
// newest non-broadcast one instead of recording nothing.
func TestUpdateLastMsgFallsBackPastTrailingBroadcast(t *testing.T) {
	t.Setenv("MESS_DIR", t.TempDir())
	clearSessionEnv(t)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "sess-4")
	p := resolvePaths()

	msgs := []Message{
		{ID: "m1", Kind: KindDirect, From: "alice"},
		{ID: "m2", Kind: KindBroadcast, From: "bob"},
	}
	updateLastMsg(p, msgs)
	got, ok := readLastMsg(p)
	if !ok || got.ID != "m1" {
		t.Fatalf("expected fallback to m1, got %+v ok=%v", got, ok)
	}
}
