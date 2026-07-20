package main

import (
	"fmt"
	"path/filepath"
	"testing"
)

// --- perf pass ---
//
// Broker.changed() calls b.onChange(b.snapshot()) synchronously, while the
// triggering method still holds b.mu — so in the live daemon (where onChange
// is wired to a real saveSnapshot: JSON-marshal the ENTIRE state + write +
// rename), every single send/recv/broadcast/etc. blocks on a full disk
// write of the WHOLE daemon's state before the mutex is released, not just
// an incremental change. These benchmarks quantify how that cost scales
// with total state size, since a live fleet (this session's: ~78 agents,
// some with 300+ pending messages) is exactly the regime where an O(total
// state) cost per message would show up.

// populateBroker seeds nAgents agents, each with msgsPerAgent pending
// messages, to simulate a fleet at realistic backlog levels.
func populateBroker(b *Broker, nAgents, msgsPerAgent int) {
	for a := 0; a < nAgents; a++ {
		name := fmt.Sprintf("agent-%d", a)
		for m := 0; m < msgsPerAgent; m++ {
			b.Send("bench-sender", name, fmt.Sprintf("message body number %d, some realistic-length filler text", m))
		}
	}
}

// BenchmarkSendNoPersist isolates the in-memory cost alone (no onChange
// wired) — the baseline to compare the persisted variants against.
func BenchmarkSendNoPersist(b *testing.B) {
	for _, size := range []struct{ agents, msgs int }{{10, 10}, {100, 50}, {500, 100}} {
		b.Run(fmt.Sprintf("agents=%d_backlog=%d", size.agents, size.msgs), func(b *testing.B) {
			br := NewBroker()
			populateBroker(br, size.agents, size.msgs)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				br.Send("bench-sender", "target", "benchmark message body")
			}
		})
	}
}

// BenchmarkSendWithDiskPersist measures the REAL cost as wired in the live
// daemon: every send synchronously re-serializes and rewrites the entire
// state to disk. This is the number that matters for "does mess degrade as
// the fleet/backlog grows."
func BenchmarkSendWithDiskPersist(b *testing.B) {
	for _, size := range []struct{ agents, msgs int }{{10, 10}, {100, 50}, {500, 100}} {
		b.Run(fmt.Sprintf("agents=%d_backlog=%d", size.agents, size.msgs), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "state.json")
			br := NewBroker()
			br.onChange = func(s snapshot) { _ = saveSnapshot(path, s) }
			populateBroker(br, size.agents, size.msgs)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				br.Send("bench-sender", "target", "benchmark message body")
			}
		})
	}
}

// BenchmarkSnapshotAndMarshal isolates just the snapshot()+JSON-marshal
// cost (no disk write) at increasing state sizes, to separate "cost of
// serialization" from "cost of the write syscall."
func BenchmarkSnapshotAndMarshal(b *testing.B) {
	for _, size := range []struct{ agents, msgs int }{{10, 10}, {100, 50}, {500, 100}, {1000, 100}} {
		b.Run(fmt.Sprintf("agents=%d_backlog=%d", size.agents, size.msgs), func(b *testing.B) {
			br := NewBroker()
			populateBroker(br, size.agents, size.msgs)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				snap := br.snapshot()
				_ = snap
			}
		})
	}
}

// BenchmarkDrainLargeInbox measures drain cost as one agent's own backlog
// grows (independent of fleet size) — the O(inbox size) split-then-copy in
// drainMatchingLocked.
func BenchmarkDrainLargeInbox(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("backlog=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				br := NewBroker()
				for m := 0; m < n; m++ {
					br.Send("bench-sender", "bob", "message body")
				}
				b.StartTimer()
				br.Drain("bob", false, 0)
			}
		})
	}
}
