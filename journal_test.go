package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestJournalWriterRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.jsonl")
	jw, err := openJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer jw.close()

	if err := jw.append(journalLine{Message: Message{ID: "m1", From: "alice", To: "bob", Kind: KindDirect, Body: "hi", Time: time.Now()}, Event: "sent"}); err != nil {
		t.Fatal(err)
	}
	if err := jw.close(); err != nil {
		t.Fatal(err)
	}

	msgs, err := searchJournal(path, journalFilter{All: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].ID != "m1" || msgs[0].Body != "hi" {
		t.Fatalf("expected the round-tripped message, got %+v", msgs)
	}
}

func TestSearchJournalFilters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.jsonl")
	jw, err := openJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer jw.close()

	now := time.Now()
	old := now.Add(-48 * time.Hour)
	lines := []journalLine{
		{Message: Message{ID: "m1", From: "alice", Topic: "eng", Kind: KindTopic, Body: "deploy is green", Time: now}, Room: "A", Event: "sent"},
		{Message: Message{ID: "m2", From: "bob", Topic: "eng", Kind: KindTopic, Body: "the license question", Time: now}, Room: "A", Event: "sent"},
		{Message: Message{ID: "m3", From: "alice", Topic: "ops", Kind: KindTopic, Body: "unrelated", Time: old}, Room: "A", Event: "sent"},
		{Message: Message{ID: "m4", From: "carol", Topic: "eng", Kind: KindTopic, Body: "in room B", Time: now}, Room: "B", Event: "sent"},
	}
	for _, l := range lines {
		if err := jw.append(l); err != nil {
			t.Fatal(err)
		}
	}
	if err := jw.close(); err != nil {
		t.Fatal(err)
	}

	// Room-scoped by default (room A only, like Ps/Broadcast).
	got, err := searchJournal(path, journalFilter{Room: "A", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 messages in room A, got %d: %+v", len(got), got)
	}

	// --all crosses rooms.
	got, err = searchJournal(path, journalFilter{All: true, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("expected all 4 messages with --all, got %d", len(got))
	}

	// --from (case-insensitive).
	got, err = searchJournal(path, journalFilter{Room: "A", From: "ALICE", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected alice's 2 messages, got %d: %+v", len(got), got)
	}

	// --grep.
	got, err = searchJournal(path, journalFilter{Room: "A", Grep: "license", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "m2" {
		t.Fatalf("expected only the license message, got %+v", got)
	}

	// --topic.
	got, err = searchJournal(path, journalFilter{Room: "A", Topic: "ops", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "m3" {
		t.Fatalf("expected only the #ops message, got %+v", got)
	}

	// --since excludes the 48h-old message.
	got, err = searchJournal(path, journalFilter{Room: "A", Since: 24 * time.Hour, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 recent messages (excluding the 48h-old one), got %d: %+v", len(got), got)
	}

	// Invalid --grep pattern is a clear error, not a silent empty result.
	if _, err := searchJournal(path, journalFilter{Grep: "(unterminated", Now: now}); err == nil {
		t.Fatal("expected an error for an invalid regexp")
	}
}

func TestParseSince(t *testing.T) {
	cases := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"", 0, false},
		{"90s", 90 * time.Second, false},
		{"15m", 15 * time.Minute, false},
		{"3h", 3 * time.Hour, false},
		{"2d", 48 * time.Hour, false},
		{"1w", 7 * 24 * time.Hour, false},
		{"3D", 72 * time.Hour, false}, // case-insensitive unit
		{"nonsense", 0, true},
		{"d", 0, true}, // no numeric part
	}
	for _, c := range cases {
		got, err := parseSince(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseSince(%q): expected an error, got %v", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSince(%q): unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseSince(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// A truncated/corrupt trailing line (simulating a crash mid-write) is
// skipped, not fatal — matching loadSnapshotFile's defensive posture.
func TestSearchJournalToleratesCorruptTrailingLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.jsonl")
	good := `{"id":"m1","from":"alice","kind":"direct","body":"hi","time":"2026-01-01T00:00:00Z","event":"sent"}` + "\n"
	corrupt := `{"id":"m2","from":"alice","kind":"direct","bo` // truncated mid-object, no trailing newline
	if err := os.WriteFile(path, []byte(good+corrupt), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := searchJournal(path, journalFilter{All: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "m1" {
		t.Fatalf("expected only the valid line, got %+v", got)
	}
}

// Rotation shifts journal.jsonl -> .1 and both the rotated and the fresh
// active file remain greppable together.
func TestJournalRotation(t *testing.T) {
	origSize := journalRotateSize
	journalRotateSize = 1 // rotate after every single write
	t.Cleanup(func() { journalRotateSize = origSize })

	path := filepath.Join(t.TempDir(), "journal.jsonl")
	jw, err := openJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := jw.append(journalLine{Message: Message{ID: "m1", From: "alice", Kind: KindDirect, Body: "first", Time: time.Now()}, Event: "sent"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("expected rotation to have produced %s.1: %v", path, err)
	}
	if err := jw.append(journalLine{Message: Message{ID: "m2", From: "bob", Kind: KindDirect, Body: "second", Time: time.Now()}, Event: "sent"}); err != nil {
		t.Fatal(err)
	}
	if err := jw.close(); err != nil {
		t.Fatal(err)
	}

	got, err := searchJournal(path, journalFilter{All: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected both generations to be greppable together, got %d: %+v", len(got), got)
	}
	if got[0].ID != "m1" || got[1].ID != "m2" {
		t.Fatalf("expected oldest-first order across generations, got %+v", got)
	}
}
