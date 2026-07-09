package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// snapshot is the serializable form of broker state. Waiters are transient and
// deliberately not persisted.
type snapshot struct {
	Seq     int          `json:"seq"`
	Agents  []agentSnap  `json:"agents"`
	Topics  []topicSnap  `json:"topics,omitempty"`
	Bridges []bridgeSnap `json:"bridges,omitempty"`
}

type agentSnap struct {
	Room     string    `json:"room,omitempty"` // "" (missing on old snapshots) = global room
	Name     string    `json:"name"`
	Inbox    []Message `json:"inbox,omitempty"`
	Topics   []string  `json:"topics,omitempty"`
	State    string    `json:"state,omitempty"`
	LastSeen time.Time `json:"lastSeen,omitzero"`
}

type topicSnap struct {
	Room        string    `json:"room,omitempty"`
	Name        string    `json:"name"`
	Subscribers []string  `json:"subscribers"`
	History     []Message `json:"history,omitempty"` // the topic's own bounded log; see Broker.topicHistory
}

type bridgeSnap struct {
	ID        string    `json:"id"`
	ARoom     string    `json:"aRoom"`
	ATopic    string    `json:"aTopic"`
	BRoom     string    `json:"bRoom"`
	BTopic    string    `json:"bTopic"`
	Direction string    `json:"direction"` // "both" | "out" | "in" (relative to a->b)
	Creator   string    `json:"creator,omitempty"`
	CreatedAt time.Time `json:"createdAt,omitzero"`
	ExpiresAt time.Time `json:"expiresAt,omitzero"` // zero = never
}

// UnmarshalJSON accepts both the current room-aware topic array and the
// legacy (pre-rooms) `"topics": {"name": ["sub1","sub2"]}` object shape, so an
// on-disk snapshot written by an older daemon loads unchanged — every legacy
// topic becomes Room=="" (global).
func (s *snapshot) UnmarshalJSON(data []byte) error {
	var raw struct {
		Seq     int             `json:"seq"`
		Agents  []agentSnap     `json:"agents"`
		Topics  json.RawMessage `json:"topics"`
		Bridges []bridgeSnap    `json:"bridges,omitempty"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	s.Seq, s.Agents, s.Bridges = raw.Seq, raw.Agents, raw.Bridges
	trimmed := bytes.TrimLeft(raw.Topics, " \t\r\n")
	switch {
	case len(trimmed) == 0:
		// absent/empty -- zero topics
	case trimmed[0] == '[': // current shape
		return json.Unmarshal(raw.Topics, &s.Topics)
	default: // legacy shape: map[string][]string
		var legacy map[string][]string
		if err := json.Unmarshal(raw.Topics, &legacy); err != nil {
			return err
		}
		for name, subs := range legacy {
			s.Topics = append(s.Topics, topicSnap{Name: name, Subscribers: subs})
		}
		sort.Slice(s.Topics, func(i, j int) bool { return s.Topics[i].Name < s.Topics[j].Name })
	}
	return nil
}

// snapshot captures broker state. Caller must hold the lock.
func (b *Broker) snapshot() snapshot {
	s := snapshot{Seq: b.seq}
	for key, a := range b.agents {
		topics := make([]string, 0, len(a.topics))
		for t := range a.topics {
			topics = append(topics, t)
		}
		sort.Strings(topics)
		s.Agents = append(s.Agents, agentSnap{Room: a.room, Name: a.name, Inbox: a.inbox, Topics: topics, State: a.state, LastSeen: b.lastSeen[key]})
	}
	sort.Slice(s.Agents, func(i, j int) bool {
		return roomThenNameLess(s.Agents[i].Room, s.Agents[i].Name, s.Agents[j].Room, s.Agents[j].Name)
	})
	// Union b.topics (current subscribers) with b.topicHistory (a topic's own
	// log can outlive its last subscriber unsubscribing) so history isn't
	// silently dropped for a topic nobody's currently subscribed to.
	topicKeys := make(map[string]bool, len(b.topics)+len(b.topicHistory))
	for tk := range b.topics {
		topicKeys[tk] = true
	}
	for tk := range b.topicHistory {
		topicKeys[tk] = true
	}
	for tk := range topicKeys {
		tRoom, tName := splitTopicKey(tk)
		subs := b.topics[tk]
		names := make([]string, 0, len(subs))
		for key := range subs {
			_, n := splitAgentKey(key)
			names = append(names, n)
		}
		sort.Strings(names)
		s.Topics = append(s.Topics, topicSnap{Room: tRoom, Name: tName, Subscribers: names, History: b.topicHistory[tk]})
	}
	sort.Slice(s.Topics, func(i, j int) bool {
		return roomThenNameLess(s.Topics[i].Room, s.Topics[i].Name, s.Topics[j].Room, s.Topics[j].Name)
	})
	for _, br := range b.bridges {
		s.Bridges = append(s.Bridges, bridgeSnap{
			ID: br.id, ARoom: br.aRoom, ATopic: br.aTopic, BRoom: br.bRoom, BTopic: br.bTopic,
			Direction: br.dir.String(), Creator: br.creator, CreatedAt: br.createdAt, ExpiresAt: br.expiresAt,
		})
	}
	sort.Slice(s.Bridges, func(i, j int) bool { return s.Bridges[i].ID < s.Bridges[j].ID })
	return s
}

// load replaces broker state from a snapshot.
func (b *Broker) load(s snapshot) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq = s.Seq
	b.agents = map[string]*agentState{}
	b.topics = map[string]map[string]bool{}
	b.lastSeen = map[string]time.Time{}
	b.bridges = map[string]*bridge{}
	b.bridgesByTopic = map[string][]*bridge{}
	b.topicHistory = map[string][]Message{}
	for _, as := range s.Agents {
		key := agentKey(as.Room, as.Name)
		a := b.ensure(key)
		a.inbox = as.Inbox
		a.state = as.State
		for _, t := range as.Topics {
			a.topics[t] = true
		}
		// Default a missing timestamp (legacy snapshot) to load time, so an
		// upgrade gives every existing agent a full grace period before cleanup
		// could prune it — rather than treating them all as "never seen".
		if as.LastSeen.IsZero() {
			b.lastSeen[key] = b.now()
		} else {
			b.lastSeen[key] = as.LastSeen
		}
	}
	for _, ts := range s.Topics {
		tk := topicKey(ts.Room, ts.Name)
		if len(ts.Subscribers) > 0 {
			subs := make(map[string]bool, len(ts.Subscribers))
			for _, n := range ts.Subscribers {
				subs[agentKey(ts.Room, n)] = true
			}
			b.topics[tk] = subs
		}
		if len(ts.History) > 0 {
			b.topicHistory[tk] = ts.History
		}
	}
	for _, bs := range s.Bridges {
		dir := bridgeBoth
		switch bs.Direction {
		case "out":
			dir = bridgeAToB
		case "in":
			dir = bridgeBToA
		}
		br := &bridge{
			id: bs.ID, aRoom: bs.ARoom, aTopic: bs.ATopic, bRoom: bs.BRoom, bTopic: bs.BTopic,
			a: topicKey(bs.ARoom, bs.ATopic), b: topicKey(bs.BRoom, bs.BTopic),
			dir: dir, creator: bs.Creator, createdAt: bs.CreatedAt, expiresAt: bs.ExpiresAt,
		}
		b.bridges[br.id] = br
		b.bridgesByTopic[br.a] = append(b.bridgesByTopic[br.a], br)
		b.bridgesByTopic[br.b] = append(b.bridgesByTopic[br.b], br)
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
type paths struct{ dir, sock, state, log, journal string }

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
		dir:     dir,
		sock:    filepath.Join(dir, "mess.sock"),
		state:   filepath.Join(dir, "state.json"),
		log:     filepath.Join(dir, "daemon.log"),
		journal: filepath.Join(dir, "journal.jsonl"),
	}
}
