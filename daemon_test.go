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
}
