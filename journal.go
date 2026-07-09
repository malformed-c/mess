package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// journalLine is one durable, append-only record of a message as it was sent,
// or later expired unread (see Broker.ExpireInbox). Unlike the bounded
// in-memory agentState.history/topicHistory (capped at maxHistory=50), this
// is the one place a message's full content survives indefinitely (subject
// only to rotation) and is queryable via `mess log`.
type journalLine struct {
	Message
	Room      string    `json:"room,omitempty"`
	Event     string    `json:"event"`              // "sent" | "expired"
	ExpiredAt time.Time `json:"expiredAt,omitzero"` // set only for Event=="expired"
}

// journalRotateSize is a var (not a const) so tests can shrink it to trigger
// rotation deterministically without writing 50MB of fixture data.
var journalRotateSize int64 = 50 * 1024 * 1024 // 50MB

const journalMaxGenerations = 5 // ~250MB ceiling across all generations

// journalWriter appends journalLines to disk, flushing after every write (no
// fsync — matches this codebase's existing durability tier: survives a clean
// restart, not a power loss, same as saveSnapshot's non-fsync'd atomic
// rename) and rotating by size, since nothing in this codebase rotates today
// and the journal is the one file that otherwise grows forever. Guarded by
// its own mutex, independent of Broker.mu — the journal is a write-only
// stream, not broker state, so appending to it never costs time under the
// broker's lock.
type journalWriter struct {
	mu   sync.Mutex
	path string
	f    *os.File
	w    *bufio.Writer
	size int64
}

func openJournal(path string) (*journalWriter, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &journalWriter{path: path, f: f, w: bufio.NewWriter(f), size: info.Size()}, nil
}

func (j *journalWriter) append(line journalLine) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	data, err := json.Marshal(line)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	n, err := j.w.Write(data)
	if err != nil {
		return err
	}
	if err := j.w.Flush(); err != nil {
		return err
	}
	j.size += int64(n)
	if j.size >= journalRotateSize {
		if err := j.rotateLocked(); err != nil {
			// The write that already succeeded isn't lost; just keep appending
			// to the oversized file until rotation can succeed.
			dlog("journal rotate failed: %v", err)
		}
	}
	return nil
}

// rotateLocked shifts journal.jsonl -> .1 -> .2 -> ... -> journalMaxGenerations,
// dropping the oldest generation, then reopens a fresh active file. Caller
// must hold j.mu.
func (j *journalWriter) rotateLocked() error {
	if err := j.w.Flush(); err != nil {
		return err
	}
	if err := j.f.Close(); err != nil {
		return err
	}
	os.Remove(fmt.Sprintf("%s.%d", j.path, journalMaxGenerations))
	for i := journalMaxGenerations - 1; i >= 1; i-- {
		src := fmt.Sprintf("%s.%d", j.path, i)
		if _, err := os.Stat(src); err == nil {
			os.Rename(src, fmt.Sprintf("%s.%d", j.path, i+1))
		}
	}
	if err := os.Rename(j.path, j.path+".1"); err != nil && !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(j.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	j.f, j.w, j.size = f, bufio.NewWriter(f), 0
	return nil
}

func (j *journalWriter) close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.w.Flush(); err != nil {
		return err
	}
	return j.f.Close()
}

// journalFilter narrows a mess log query. Room-scoped by default like Ps and
// Broadcast; All bypasses that. Since is a lower time bound (zero = no bound).
type journalFilter struct {
	Room  string
	All   bool
	From  string
	Topic string
	Grep  string
	Since time.Duration
	Max   int
	Now   time.Time
}

// searchJournal scans every existing generation of path (oldest first, so
// results come back in chronological order) applying filter, skipping any
// line that fails to parse (a truncated trailing line from a crash mid-write
// is tolerated, not fatal — same defensive posture as loadSnapshotFile).
func searchJournal(path string, filter journalFilter) ([]Message, error) {
	var grepRe *regexp.Regexp
	if filter.Grep != "" {
		re, err := regexp.Compile(filter.Grep)
		if err != nil {
			return nil, fmt.Errorf("invalid --grep pattern: %w", err)
		}
		grepRe = re
	}
	var cutoff time.Time
	if filter.Since > 0 {
		now := filter.Now
		if now.IsZero() {
			now = time.Now()
		}
		cutoff = now.Add(-filter.Since)
	}

	var files []string
	for i := journalMaxGenerations; i >= 1; i-- { // oldest generation first
		files = append(files, fmt.Sprintf("%s.%d", path, i))
	}
	files = append(files, path) // current, newest, active file last

	var out []Message
	for _, fp := range files {
		f, err := os.Open(fp)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024) // allow lines well past a giant message body
		for sc.Scan() {
			var line journalLine
			if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
				continue // truncated/corrupt line; skip, don't fail the whole query
			}
			if !filter.All && line.Room != filter.Room {
				continue
			}
			if filter.From != "" && !strings.EqualFold(line.From, filter.From) {
				continue
			}
			if filter.Topic != "" && line.Topic != filter.Topic {
				continue
			}
			if !cutoff.IsZero() && line.Time.Before(cutoff) {
				continue
			}
			if grepRe != nil && !grepRe.MatchString(line.Body) {
				continue
			}
			out = append(out, line.Message)
		}
		if err := sc.Err(); err != nil {
			dlog("journal scan of %s stopped early: %v", fp, err) // e.g. a line past the buffer cap; best-effort
		}
		f.Close()
	}
	if filter.Max > 0 && filter.Max < len(out) {
		out = out[len(out)-filter.Max:]
	}
	return out, nil
}

// parseSince parses a duration that additionally supports day/week suffixes
// ("3d", "2w") that time.ParseDuration doesn't, for `mess log --since 3d`.
func parseSince(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	if n := len(s); n > 1 {
		unit := s[n-1]
		if num, err := strconv.Atoi(s[:n-1]); err == nil {
			switch unit {
			case 'd', 'D':
				return time.Duration(num) * 24 * time.Hour, nil
			case 'w', 'W':
				return time.Duration(num) * 7 * 24 * time.Hour, nil
			}
		}
	}
	return 0, fmt.Errorf("invalid duration %q (try 90s, 15m, 3h, 2d, 1w)", s)
}
