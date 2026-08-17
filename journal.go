package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"mess/wire"
	"os"
	"sync"
)

// journalLine is one durable, append-only record of a message as it was sent,
// or later expired unread (see Broker.ExpireInbox). Unlike the bounded
// in-memory agentState.history/topicHistory (capped at maxHistory=50), this
// is the one place a message's full content survives indefinitely (subject
// only to rotation) and is queryable via `mess log`.
type journalLine = wire.JournalLine

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

func init() { wire.Debugf = dlog } // the journal reader's diagnostics go where ours do

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

// The reader moved to package wire so a separate UI module can follow the
// journal directly. These aliases keep the daemon's own call sites unchanged.
type journalFilter = wire.Filter

var (
	searchJournal = wire.SearchJournal
	parseSince    = wire.ParseSince
)
