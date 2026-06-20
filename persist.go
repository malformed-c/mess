package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

// snapshot is the serializable form of broker state. Waiters are transient and
// deliberately not persisted.
type snapshot struct {
	Seq    int                 `json:"seq"`
	Agents []agentSnap         `json:"agents"`
	Topics map[string][]string `json:"topics"`
}

type agentSnap struct {
	Name   string    `json:"name"`
	Inbox  []Message `json:"inbox,omitempty"`
	Topics []string  `json:"topics,omitempty"`
}

// snapshot captures broker state. Caller must hold the lock.
func (b *Broker) snapshot() snapshot {
	s := snapshot{Seq: b.seq, Topics: map[string][]string{}}
	for _, a := range b.agents {
		topics := make([]string, 0, len(a.topics))
		for t := range a.topics {
			topics = append(topics, t)
		}
		sort.Strings(topics)
		s.Agents = append(s.Agents, agentSnap{Name: a.name, Inbox: a.inbox, Topics: topics})
	}
	sort.Slice(s.Agents, func(i, j int) bool { return s.Agents[i].Name < s.Agents[j].Name })
	for t, subs := range b.topics {
		names := make([]string, 0, len(subs))
		for n := range subs {
			names = append(names, n)
		}
		sort.Strings(names)
		s.Topics[t] = names
	}
	return s
}

// load replaces broker state from a snapshot.
func (b *Broker) load(s snapshot) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq = s.Seq
	b.agents = map[string]*agentState{}
	b.topics = map[string]map[string]bool{}
	for _, as := range s.Agents {
		a := b.ensure(as.Name)
		a.inbox = as.Inbox
		for _, t := range as.Topics {
			a.topics[t] = true
		}
	}
	for t, names := range s.Topics {
		if b.topics[t] == nil {
			b.topics[t] = map[string]bool{}
		}
		for _, n := range names {
			b.topics[t][n] = true
		}
	}
}

// saveSnapshot writes state atomically (temp file + rename).
func saveSnapshot(path string, s snapshot) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// loadSnapshotFile reads a snapshot, returning a zero snapshot if absent.
func loadSnapshotFile(path string) (snapshot, error) {
	var s snapshot
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	if len(data) == 0 {
		return s, nil
	}
	err = json.Unmarshal(data, &s)
	return s, err
}

// paths resolves the runtime directory and the files within it. The directory
// can be overridden with MESS_DIR (useful for tests and isolation).
type paths struct{ dir, sock, state, log string }

func resolvePaths() paths {
	dir := os.Getenv("MESS_DIR")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			home = os.TempDir()
		}
		dir = filepath.Join(home, ".mess")
	}
	return paths{
		dir:   dir,
		sock:  filepath.Join(dir, "mess.sock"),
		state: filepath.Join(dir, "state.json"),
		log:   filepath.Join(dir, "daemon.log"),
	}
}
