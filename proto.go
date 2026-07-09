package main

import "time"

// Message kinds.
const (
	KindDirect    = "direct"
	KindBroadcast = "broadcast"
	KindTopic     = "topic"
)

// Message is a single delivered item in an agent's inbox.
type Message struct {
	ID    string    `json:"id"`
	From  string    `json:"from"`
	To    string    `json:"to,omitempty"`    // recipient for direct messages
	Topic string    `json:"topic,omitempty"` // channel for topic messages
	Kind  string    `json:"kind"`            // direct | broadcast | topic
	Body  string    `json:"body"`
	Time  time.Time `json:"time"`

	// AckRequested marks a direct message whose sender is blocking for a read
	// receipt. Set on send; the sender is notified when the message is consumed.
	AckRequested bool `json:"ack,omitempty"`

	// Quiet marks this recipient's copy as delivered-without-notifying — a topic
	// message that @-mentioned other subscribers, not this one. It's still received
	// (returned by recv), but it doesn't wake a parked recv or count toward the
	// mid-turn steer notice.
	Quiet bool `json:"quiet,omitempty"`

	// Loud marks a message that must wake its recipient even if their parked
	// wake hook is filtering out this Kind (the standard auto-wake hook parks
	// with --no-broadcast, so a plain broadcast never actually wakes an idle
	// agent through it — only a mid-turn steer notice or a manual recv sees
	// it). Set via `mess broadcast --loud`. wakes() checks this before the
	// kind filter, so it's a deliberate override for "this must be seen now."
	Loud bool `json:"loud,omitempty"`

	// ThreadID, when set, marks this message as a reply within a thread — its
	// value is the thread root message's own ID (flat: replying to a reply still
	// attaches to the same root). Empty means this message isn't part of a thread.
	ThreadID string `json:"threadId,omitempty"`

	// Bridge provenance, set only on a message that arrived via a topic bridge
	// (see Bridge/Unbridge). BridgeID is the specific hop that delivered *this*
	// copy; OriginRoom/OriginTopic are the room/topic of the original publish,
	// stamped once at the first hop so it survives an arbitrary-length bridge
	// chain. From/Topic stay the original publisher's bare name/topic.
	BridgeID    string `json:"bridgeId,omitempty"`
	OriginRoom  string `json:"originRoom,omitempty"`
	OriginTopic string `json:"originTopic,omitempty"`

	// Attachment fields (mess send/pub --attach), all set together or not at
	// all. AttachPath is absolute (meaningful regardless of the reader's cwd).
	// AttachHash is "sha256:<hex>", computed client-side before the request is
	// sent — this is a single-machine tool, so the daemon has no reason to
	// ever assume a different filesystem view than the CLI that sent it.
	AttachPath  string    `json:"attachPath,omitempty"`
	AttachHash  string    `json:"attachHash,omitempty"`
	AttachSize  int64     `json:"attachSize,omitempty"`
	AttachMTime time.Time `json:"attachMtime,omitzero"`
}

// Request is one command sent from a client to the daemon.
type Request struct {
	Op       string   `json:"op"`
	As       string   `json:"as,omitempty"`       // identity of the calling agent
	To       string   `json:"to,omitempty"`       // direct recipient
	Topic    string   `json:"topic,omitempty"`    // topic for pub/sub
	Body     string   `json:"body,omitempty"`     // message body
	Ack      bool     `json:"ack,omitempty"`      // (send) block until recipient reads it
	Wait     bool     `json:"wait,omitempty"`     // block until a message arrives
	Timeout  string   `json:"timeout,omitempty"`  // optional wait timeout (duration); also (room bridge) the TTL duration
	Peek     bool     `json:"peek,omitempty"`     // recv without consuming
	IfIdle   bool     `json:"ifIdle,omitempty"`   // (recv, non-blocking only) drain only if not currently busy, checked atomically with the drain
	Max      int      `json:"max,omitempty"`      // recv at most N messages (0 = all)
	Kinds    []string `json:"kinds,omitempty"`    // recv only these kinds (nil = all)
	Batch    string   `json:"batch,omitempty"`    // (recv --wait) coalesce a burst within this window
	Session  string   `json:"session,omitempty"`  // host session id (stamped on every request), binds a name to its owning session
	Force    bool     `json:"force,omitempty"`    // (register/rename/room join/bridge) take over a name/collision held by another live session
	Loud     bool     `json:"loud,omitempty"`     // (broadcast) force a desktop notification to the human operator, regardless of @mention
	HostWide bool     `json:"hostWide,omitempty"` // (broadcast --loud) skip room scoping, reach every room on the host

	Room string `json:"room,omitempty"` // room to act in ("" = global/default room)
	All  bool   `json:"all,omitempty"`  // (ps) ignore Room, show every room

	ThreadID string `json:"threadId,omitempty"` // (send/pub) reply within this thread; (recv) filter to this thread

	Direction   string `json:"direction,omitempty"`   // (room bridge) "both" | "out" | "in"
	RemoteRoom  string `json:"remoteRoom,omitempty"`  // (room bridge) far side's room
	RemoteTopic string `json:"remoteTopic,omitempty"` // (room bridge) far side's topic
	LocalRoom   string `json:"localRoom,omitempty"`   // (room bridge) override for the local side; requires Force if != caller's current room
	BridgeID    string `json:"bridgeId,omitempty"`    // (room unbridge) which bridge to tear down

	From  string `json:"from,omitempty"`  // (log) only messages from this sender
	Grep  string `json:"grep,omitempty"`  // (log) only messages whose body matches this regexp
	Since string `json:"since,omitempty"` // (log) only messages newer than this duration (supports "3d"/"2w")

	// (send/pub --attach) mirrors Message's attachment fields — see there.
	AttachPath  string    `json:"attachPath,omitempty"`
	AttachHash  string    `json:"attachHash,omitempty"`
	AttachSize  int64     `json:"attachSize,omitempty"`
	AttachMTime time.Time `json:"attachMtime,omitzero"`
}

// AgentInfo is reported by the `ps` op.
type AgentInfo struct {
	Name      string    `json:"name"`
	Room      string    `json:"room,omitempty"` // "" = global/default room
	Pending   int       `json:"pending"`
	Topics    []string  `json:"topics,omitempty"`
	Listening bool      `json:"listening,omitempty"` // has an active streaming listener
	Working   bool      `json:"working,omitempty"`   // currently in a turn (busy)
	Online    bool      `json:"online,omitempty"`    // session looks alive (listening/working/recent)
	State     string    `json:"state,omitempty"`     // self-reported working state
	Warning   string    `json:"warning,omitempty"`   // transient status warning (auto-clears)
	Oldest    time.Time `json:"oldest,omitzero"`     // arrival time of the oldest pending message
}

// TopicInfo is reported by the `ps` op.
type TopicInfo struct {
	Name        string   `json:"name"`
	Room        string   `json:"room,omitempty"`
	Subscribers []string `json:"subscribers"`
	Bridged     []string `json:"bridged,omitempty"` // e.g. "periapsis/#deploy (out)", one per bridge touching this topic
}

// BridgeInfo is reported by `room bridge`/`room bridges`.
type BridgeInfo struct {
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

// ThreadInfo summarizes one thread from a calling agent's own received view
// (see ListThreads) — the root message plus whatever replies it has seen.
type ThreadInfo struct {
	ID           string    `json:"id"`
	Kind         string    `json:"kind"`            // direct | topic
	Topic        string    `json:"topic,omitempty"` // set for a topic thread
	Peer         string    `json:"peer,omitempty"`  // set for a direct thread: the other party
	RootBody     string    `json:"rootBody,omitempty"`
	Replies      int       `json:"replies"`      // replies seen, not counting the root
	Participants int       `json:"participants"` // total known posters, server-wide
	LastActivity time.Time `json:"lastActivity"`
}

// Response is the daemon's reply to a Request.
type Response struct {
	OK       bool         `json:"ok"`
	Error    string       `json:"error,omitempty"`
	Acked    bool         `json:"acked,omitempty"` // (send --ack) recipient read the message
	Messages []Message    `json:"messages,omitempty"`
	Agents   []AgentInfo  `json:"agents,omitempty"`
	Topics   []TopicInfo  `json:"topics,omitempty"`
	Bridges  []BridgeInfo `json:"bridges,omitempty"`
	Threads  []ThreadInfo `json:"threads,omitempty"`
	Count    int          `json:"count,omitempty"`
	Removed  []string     `json:"removed,omitempty"` // (cleanup) agents pruned (or, with dry-run, eligible)
	Busy     bool         `json:"busy,omitempty"`    // (recv --if-idle) the agent was busy, so nothing was drained
	ID       string       `json:"id,omitempty"`      // (ask) the created message's own id — the await token
	Expired  int          `json:"expired,omitempty"` // (expire) unread messages dropped (or, with dry-run, eligible)
}
