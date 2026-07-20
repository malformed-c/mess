package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// --- saveSnapshot / loadSnapshotFile: real file I/O, not just in-memory ---
//
// TestPersistenceRoundTrip (broker_test.go) only exercises b.load(b.snapshot())
// in memory; these hit the actual file I/O layer.

func TestSaveSnapshotRoundTripsThroughDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	b := newTestBroker()
	b.Send("alice", "bob", "on disk")
	b.Sub("bob", "builds")

	if err := saveSnapshot(path, b.snapshot()); err != nil {
		t.Fatal(err)
	}
	snap, err := loadSnapshotFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b2 := newTestBroker()
	b2.load(snap)
	if got := b2.Drain("bob", false, 0); len(got) != 1 || got[0].Body != "on disk" {
		t.Fatalf("inbox not restored from disk: %+v", got)
	}
}

// saveSnapshot writes to a temp file and renames over the destination — the
// destination must never be visible in a partially-written state, even if
// the process were killed mid-write (the rename is what makes this atomic
// on POSIX; a partial write only ever lands in the .tmp file, which the
// live path never points at until the whole write completed).
func TestSaveSnapshotIsAtomicNeverLeavesPartialFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	b := newTestBroker()
	b.Send("alice", "bob", "first")
	if err := saveSnapshot(path, b.snapshot()); err != nil {
		t.Fatal(err)
	}
	firstData, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// A second save's .tmp file existing mid-write must never be what
	// loadSnapshotFile(path) sees — simulate by checking the .tmp file is
	// cleaned up (renamed away) after a successful save, not left behind
	// to confuse a later load.
	b.Send("alice", "bob", "second")
	if err := saveSnapshot(path, b.snapshot()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("expected the .tmp file to be gone after rename, stat err = %v", err)
	}
	secondData, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstData) == string(secondData) {
		t.Fatal("second save should have produced different content (two messages, not one)")
	}
}

// --- loadSnapshotWithRecovery: the actual crash-recovery fix ---
//
// Real gap found in the concurrency/crash-recovery audit: a state.json that
// fails to parse used to make the daemon silently start with a completely
// empty broker — every agent's identity, inbox, and subscriptions gone,
// signaled only by an easy-to-miss log line. loadSnapshotWithRecovery
// preserves the corrupt file instead of letting the next save silently
// overwrite/lose it, and always calls warn() so the caller can log loudly.

func TestLoadSnapshotWithRecoveryMissingFileIsNotCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json") // never created
	warned := false
	snap := loadSnapshotWithRecovery(path, time.Now, func(string) { warned = true })
	if warned {
		t.Fatal("a missing state file (first run) must not be treated as corruption")
	}
	if len(snap.Agents) != 0 {
		t.Fatalf("expected a zero snapshot, got %+v", snap)
	}
}

func TestLoadSnapshotWithRecoveryValidFileLoadsNormally(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	b := newTestBroker()
	b.Send("alice", "bob", "hello")
	if err := saveSnapshot(path, b.snapshot()); err != nil {
		t.Fatal(err)
	}
	warned := false
	snap := loadSnapshotWithRecovery(path, time.Now, func(string) { warned = true })
	if warned {
		t.Fatal("a valid state file must not trigger a corruption warning")
	}
	b2 := newTestBroker()
	b2.load(snap)
	if got := b2.Drain("bob", false, 0); len(got) != 1 {
		t.Fatalf("expected the valid snapshot to load normally, got %+v", got)
	}
}

// The actual fix: a corrupted file is preserved (renamed aside), not
// silently discarded, and the daemon still starts (with empty state) rather
// than refusing to serve the whole fleet over one bad file.
func TestLoadSnapshotWithRecoveryPreservesCorruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte("{not valid json"), 0o600); err != nil {
		t.Fatal(err)
	}
	fixedNow := time.Unix(1234567890, 0)
	var warnMsg string
	snap := loadSnapshotWithRecovery(path, func() time.Time { return fixedNow }, func(msg string) { warnMsg = msg })

	if len(snap.Agents) != 0 {
		t.Fatalf("expected empty snapshot after corruption, got %+v", snap)
	}
	if warnMsg == "" {
		t.Fatal("expected a warning to be raised for a corrupt state file")
	}
	backup := path + ".corrupt-1234567890"
	if _, err := os.Stat(backup); err != nil {
		t.Fatalf("expected the corrupt file preserved at %s: %v", backup, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected the original corrupt path to be gone (renamed away), stat err = %v", err)
	}
	data, err := os.ReadFile(backup)
	if err != nil || string(data) != "{not valid json" {
		t.Fatalf("expected the preserved backup to contain the original corrupt content, got %q, err=%v", data, err)
	}
}
