// Journal reading. The writer stays in mess itself — only the daemon appends —
// but the reader is here because it is the honest source for a UI: it is
// append-only, so any number of viewers can follow it continuously without
// consuming anything, unlike an inbox, where every read is destructive.
package wire

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Debugf, when set by the host program, receives best-effort diagnostics from
// the journal reader. nil (the default) means silence — a library has no
// business choosing where a program's logs go.
var Debugf func(format string, args ...any)

// is the one place a message's full content survives indefinitely (subject
// only to rotation) and is queryable via `mess log`.
type JournalLine struct {
	Message
	Room      string    `json:"room,omitempty"`
	Event     string    `json:"event"`              // "sent" | "expired"
	ExpiredAt time.Time `json:"expiredAt,omitzero"` // set only for Event=="expired"
}

const maxGenerations = 5 // ~250MB ceiling across all generations

// Filter narrows a mess log query. Room-scoped by default like Ps and
// Broadcast; All bypasses that. Since is a lower time bound (zero = no bound).
type Filter struct {
	Room  string
	All   bool
	From  string
	Topic string
	Grep  string
	Since time.Duration
	Max   int
	Now   time.Time
}

// SearchJournal scans every existing generation of path (oldest first, so
// results come back in chronological order) applying filter, skipping any
// line that fails to parse (a truncated trailing line from a crash mid-write
// is tolerated, not fatal — same defensive posture as loadSnapshotFile).
func SearchJournal(path string, filter Filter) ([]Message, error) {
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
	for i := maxGenerations; i >= 1; i-- { // oldest generation first
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
			var line JournalLine
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
			// e.g. a line past the buffer cap. Best-effort: report it if the
			// host wired up a logger, otherwise carry on with what parsed.
			if Debugf != nil {
				Debugf("journal scan of %s stopped early: %v", fp, err)
			}
		}
		f.Close()
	}
	if filter.Max > 0 && filter.Max < len(out) {
		out = out[len(out)-filter.Max:]
	}
	return out, nil
}

// ParseSince parses a duration that additionally supports day/week suffixes
// ("3d", "2w") that time.ParseDuration doesn't, for `mess log --since 3d`.
func ParseSince(s string) (time.Duration, error) {
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
