package main

import (
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"
)

// Broker is the in-memory message store. It is transport-agnostic and safe for
// concurrent use, which keeps it directly unit-testable without a socket.
type Broker struct {
	mu     sync.Mutex
	agents map[string]*agentState
	topics map[string]map[string]bool // topic -> set of subscriber names
	seq    int
	now    func() time.Time // injectable clock for tests

	// pendingAcks maps a message ID to a channel signaled when that message is
	// read (consumed) by its recipient. Transient; never persisted.
	pendingAcks map[string]chan struct{}

	// listeners counts active streaming `listen` connections per agent, so we
	// can tell whether an agent is actively reachable. Transient.
	listeners map[string]int

	// busyUntil marks an agent as actively in a turn until the given time (set by
	// turn-activity hooks, cleared on Stop; the time is a crash backstop).
	busyUntil map[string]time.Time

	// lastSeen records the last time an agent did anything (registered, sent,
	// recv'd, parked, ...). Drives `cleanup`, which prunes long-idle agents.
	lastSeen map[string]time.Time

	// owners records which host session currently owns each name, so register and
	// the runtime identity gate can refuse a *different* live session acting under
	// a name already in use.
	owners map[string]ownerInfo

	// warnings holds a transient status warning per agent (e.g. an API error set
	// by a lifecycle hook). It auto-clears when the agent is next active and
	// self-expires, so stale warnings don't linger in `ps`.
	warnings map[string]warnInfo

	// evicts holds channels, per agent name, that are closed when the agent is
	// removed or renamed — so a parked recv waiting on that name stops instead of
	// lingering as a ghost listener (and being resurrected on a daemon restart).
	evicts map[string][]chan struct{}

	// bridges/bridgesByTopic implement cross-room topic relay (see Bridge/
	// Unbridge/relayLocked). bridgesByTopic indexes by composite topic key
	// (either side) for Pub's fan-out and Ps's audit display.
	bridges        map[string]*bridge
	bridgesByTopic map[string][]*bridge

	// threadParticipants maps a thread's root message ID to the set of composite
	// agent keys who've posted in it — a threaded reply wakes a participant (or
	// an @mention) but is quiet-delivered to everyone else, the same way an
	// unmentioned topic subscriber is.
	threadParticipants map[string]map[string]bool

	// topicHistory is a topic's own bounded append-only log (composite topic
	// key -> recent messages), independent of any individual subscriber's own
	// inbox/history lifecycle — unlike agentState.history (per-*recipient*,
	// only what that agent actually consumed), this exists even for a topic
	// nobody was subscribed to at the time, so `mess export --topic` has
	// something to show.
	topicHistory map[string][]Message

	// onChange is invoked (while holding the lock) after every mutation so the
	// caller can persist state. It receives a snapshot to serialize.
	onChange func(snapshot)
}

type agentState struct {
	name    string // bare display name, derived from the map key
	room    string // "" = global/default room, derived from the map key
	inbox   []Message
	history []Message // bounded ring of recently-consumed messages, for `replay`
	topics  map[string]bool
	state   string          // self-reported "what I'm working on"
	waiters []chan struct{} // signaled (then dropped) on next delivery
}

// maxHistory bounds the per-agent replay history (recently-consumed messages).
const maxHistory = 50

// ownerInfo identifies the host session that owns a name. Ownership binds a name
// to the (stable, per-session) host session id that first claimed it, so a
// different live session can't act under it.
type ownerInfo struct {
	session string
}

// warnInfo is a transient status warning and its expiry.
type warnInfo struct {
	text  string
	until time.Time
}

// bridgeDirection controls which way a bridge relays a publish.
type bridgeDirection int

const (
	bridgeBoth bridgeDirection = iota // relay both ways (default)
	bridgeAToB                        // relay only a -> b
	bridgeBToA                        // relay only b -> a
)

func (d bridgeDirection) String() string {
	switch d {
	case bridgeAToB:
		return "out"
	case bridgeBToA:
		return "in"
	default:
		return "both"
	}
}

// bridge links two topics (possibly in different rooms) so a publish to
// either side also relays to subscribers on the other — the explicit escape
// hatch for cross-room coordination now that topics are room-scoped.
type bridge struct {
	id                   string
	a, b                 string // composite topic keys: topicKey(room, topic)
	aRoom, aTopic        string // decomposed, cached for display/persistence
	bRoom, bTopic        string
	dir                  bridgeDirection
	creator              string
	createdAt, expiresAt time.Time // expiresAt zero = never
}

func (br *bridge) expired(now time.Time) bool {
	return !br.expiresAt.IsZero() && now.After(br.expiresAt)
}

// otherSide returns the far composite topic key from key, and whether br's
// direction allows relaying from key at all.
func (br *bridge) otherSide(key string) (string, bool) {
	switch key {
	case br.a:
		return br.b, br.dir == bridgeBoth || br.dir == bridgeAToB
	case br.b:
		return br.a, br.dir == bridgeBoth || br.dir == bridgeBToA
	}
	return "", false
}

// describeFrom renders br for display in ps, from the perspective of key (one
// of its two topic sides) — e.g. "periapsis/#deploy (out)".
func (br *bridge) describeFrom(key string) string {
	other, _ := br.otherSide(key)
	oRoom, oTopic := splitTopicKey(other)
	dir := br.dir.String()
	if key == br.b {
		// Flip the label's sense so it always reads relative to the caller's side.
		switch br.dir {
		case bridgeAToB:
			dir = "in"
		case bridgeBToA:
			dir = "out"
		}
	}
	return fmt.Sprintf("%s (%s)", displayName(oRoom, "#"+oTopic), dir)
}

// NewBroker returns an empty broker.
func NewBroker() *Broker {
	return &Broker{
		agents:             map[string]*agentState{},
		topics:             map[string]map[string]bool{},
		pendingAcks:        map[string]chan struct{}{},
		listeners:          map[string]int{},
		busyUntil:          map[string]time.Time{},
		lastSeen:           map[string]time.Time{},
		owners:             map[string]ownerInfo{},
		warnings:           map[string]warnInfo{},
		evicts:             map[string][]chan struct{}{},
		bridges:            map[string]*bridge{},
		bridgesByTopic:     map[string][]*bridge{},
		threadParticipants: map[string]map[string]bool{},
		topicHistory:       map[string][]Message{},
		now:                time.Now,
	}
}

// WatchEvict returns a channel that is closed when name is removed or renamed, so
// a parked recv can stop waiting instead of becoming a ghost listener.
func (b *Broker) WatchEvict(name string) chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan struct{})
	b.evicts[name] = append(b.evicts[name], ch)
	return ch
}

// UnwatchEvict deregisters an eviction channel (on recv return).
func (b *Broker) UnwatchEvict(name string, ch chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	chans := b.evicts[name]
	for i, c := range chans {
		if c == ch {
			b.evicts[name] = append(chans[:i], chans[i+1:]...)
			break
		}
	}
	if len(b.evicts[name]) == 0 {
		delete(b.evicts, name)
	}
}

// SetWarn sets (or, with empty text, clears) an agent's transient status warning,
// auto-clearing after ttl.
func (b *Broker) SetWarn(name, text string, ttl time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ensure(name)
	if text == "" {
		delete(b.warnings, name)
	} else {
		b.warnings[name] = warnInfo{text: text, until: b.now().Add(ttl)}
		b.touch(name)
	}
	b.changed()
}

// clearWarnLocked drops an agent's warning (called when it becomes active again,
// so a recovered agent's stale warning disappears). Holds the lock.
func (b *Broker) clearWarnLocked(name string) {
	delete(b.warnings, name)
}

// touch records that an agent was just active (for cleanup). Call with the lock
// held; an empty name (e.g. a recv with no identity) is ignored.
func (b *Broker) touch(name string) {
	if name != "" {
		b.lastSeen[name] = b.now()
	}
}

// ensure gets-or-creates the agentState for key, a composite agentKey(room,
// name) (or a bare name, for the global room). name/room are derived from the
// key itself, so no caller needs to set them separately.
func (b *Broker) ensure(key string) *agentState {
	a := b.agents[key]
	if a == nil {
		room, name := splitAgentKey(key)
		a = &agentState{name: name, room: room, topics: map[string]bool{}}
		b.agents[key] = a
	}
	return a
}

func (b *Broker) nextID() string {
	b.seq++
	return fmt.Sprintf("m%d", b.seq)
}

// deliver appends a message to an agent's inbox and wakes any waiters.
func (a *agentState) deliver(m Message) {
	a.inbox = append(a.inbox, m)
	for _, w := range a.waiters {
		select {
		case w <- struct{}{}:
		default:
		}
	}
	a.waiters = nil
}

// deliverQuiet appends a message flagged Quiet (no waiter signal, and skipped by
// the wake trigger and the steer notice), so an unmentioned subscriber still
// receives a topic message but isn't woken or notified by it.
func (a *agentState) deliverQuiet(m Message) {
	m.Quiet = true
	a.inbox = append(a.inbox, m)
}

// changed builds a snapshot and fires the persistence hook. Call with lock held.
func (b *Broker) changed() {
	if b.onChange != nil {
		b.onChange(b.snapshot())
	}
}

// Register makes an agent known so it can receive broadcasts. It does not track
// ownership (used internally and in tests); RegisterOwned is the guarded path.
func (b *Broker) Register(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ensure(name)
	b.touch(name)
	b.changed()
}

// RegisterOwned registers name on behalf of a host session, guarding against a
// *different, still-live* session claiming a name already in use. It returns
// ok=false and a message on such a collision, unless force is set. A takeover of
// a name whose owner is no longer live is allowed (reclaiming a dead name). The
// host session id is stable for a session's whole life, so a different id under
// the same name is always a distinct session — never a rotation.
func (b *Broker) RegisterOwned(name, session string, force bool) (bool, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !force && b.foreignLiveOwnerLocked(name, session) {
		return false, ownershipMsg(name)
	}
	b.ensure(name)
	b.touch(name)
	b.clearWarnLocked(name) // re-registering (fresh/resumed session) clears a stale warning
	b.owners[name] = ownerInfo{session: session}
	b.changed()
	return true, ""
}

// IsRegistered reports whether name has ever been claimed via
// register/room-join/rename — as opposed to merely having a pending inbox
// because someone sent it a message. ensure() auto-creates an agentState for
// any recipient of a plain send/pub, registered or not (by design — a
// fire-and-forget message can wait for a name that hasn't started yet), so
// presence in b.agents alone isn't a reliable "is this a real identity"
// signal; b.owners, populated only by an actual identity claim, is.
func (b *Broker) IsRegistered(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.owners[name]
	return ok
}

// IsOnline reports whether name currently looks alive — listening, working,
// or recently active — the same signal `mess ps`'s Online column already
// uses (aliveLocked). Exported so a caller like ask can fail fast against an
// offline recipient instead of blocking for a reply that may never come.
func (b *Broker) IsOnline(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.aliveLocked(name)
}

// foreignLiveOwnerLocked reports whether name is currently owned by a *different*
// still-live session than the caller's — i.e. claiming it would be an identity
// takeover. A "" session can't be distinguished from any other, so it is never
// treated as an owner or as the caller (no enforcement without a session id).
func (b *Broker) foreignLiveOwnerLocked(name, session string) bool {
	cur, ok := b.owners[name]
	return ok && cur.session != "" && session != "" && cur.session != session && b.aliveLocked(name)
}

func ownershipMsg(name string) string {
	return fmt.Sprintf("name %q is in use by another live session; choose a different name (mess register <name>) or pass --force to take it over", name)
}

// ClaimIdentity binds name to the caller's session for any identity-asserting
// operation (send, recv, sub, busy, ...), rejecting a *different live session*
// acting under a name it doesn't own. This is defense in depth: even if identity
// resolution ever handed a session the wrong name, the daemon refuses to let it
// speak or receive as another live agent. First live user of a free/dead name
// takes ownership. No session id (e.g. a bare MESS_AGENT run) means no
// enforcement — the check is skipped.
func (b *Broker) ClaimIdentity(name, session string) (bool, string) {
	if name == "" || session == "" || isUserHandle(name) {
		return true, "" // no id, or the shared human mailbox — never single-session-owned
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.foreignLiveOwnerLocked(name, session) {
		return false, ownershipMsg(name)
	}
	b.ensure(name)
	b.touch(name) // acting under a name is activity — keeps ownership live-and-enforced
	b.owners[name] = ownerInfo{session: session}
	return true, ""
}

// Rename moves an agent from old to new, migrating its inbox, topic
// subscriptions, state, and busy/last-seen bookkeeping, then removing old. It
// honors the same collision guard as RegisterOwned on the destination name
// (refuses a different live session's name unless force). Returns ok=false and a
// message on collision.
func (b *Broker) Rename(old, newName, session string, force bool) (bool, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if newName == "" {
		return false, "new name required"
	}
	if old == newName {
		b.ensure(newName)
		b.touch(newName)
		b.owners[newName] = ownerInfo{session: session}
		b.changed()
		return true, ""
	}
	if !force && b.foreignLiveOwnerLocked(newName, session) {
		return false, ownershipMsg(newName)
	}

	dst := b.ensure(newName)
	room, _ := splitAgentKey(newName)
	if src := b.agents[old]; src != nil {
		dst.inbox = append(dst.inbox, src.inbox...)
		if dst.state == "" {
			dst.state = src.state
		}
		for topicName := range src.topics {
			dst.topics[topicName] = true
			tk := topicKey(room, topicName)
			if b.topics[tk] == nil {
				b.topics[tk] = map[string]bool{}
			}
			b.topics[tk][newName] = true
		}
	}
	// Carry over activity/turn markers (keep the fresher of the two).
	if t, ok := b.lastSeen[old]; ok && t.After(b.lastSeen[newName]) {
		b.lastSeen[newName] = t
	}
	if t, ok := b.busyUntil[old]; ok && t.After(b.busyUntil[newName]) {
		b.busyUntil[newName] = t
	}
	b.removeAgentLocked(old) // also unsubscribes old from topics and clears its maps
	b.touch(newName)
	b.owners[newName] = ownerInfo{session: session}
	b.changed()
	return true, ""
}

// aliveLocked reports whether an agent looks currently reachable — parked
// (listening), in a turn (busy), or active in the last couple of minutes. Held
// lock. Used by the register collision guard to tell a live owner from a dead
// one whose name may be reclaimed.
func (b *Broker) aliveLocked(name string) bool {
	if b.listeners[name] > 0 {
		return true
	}
	if b.busyUntil[name].After(b.now()) {
		return true
	}
	if t, ok := b.lastSeen[name]; ok && b.now().Sub(t) < 2*time.Minute {
		return true
	}
	return false
}

// Send delivers a direct message to a single recipient.
func (b *Broker) Send(from, to, body string) (Message, error) {
	m, _, err := b.send(from, to, body, "", false, nil, false)
	return m, err
}

// SendAck delivers a direct message and returns a channel that fires once the
// recipient reads (consumes) it. The caller can block on the channel, with its
// own timeout, to implement a read receipt.
func (b *Broker) SendAck(from, to, body string) (Message, <-chan struct{}, error) {
	return b.send(from, to, body, "", true, nil, false)
}

// SendThreaded is Send, tagging the message as a reply within threadID (the
// thread root's own message ID; see PubThreaded). Direct messages have only
// one recipient, so — unlike Pub — a thread tag never changes wake behavior
// here, only participant bookkeeping (so the same person showing up in a
// topic thread later is already recognized as a participant).
func (b *Broker) SendThreaded(from, to, body, threadID string) (Message, error) {
	m, _, err := b.send(from, to, body, threadID, false, nil, false)
	return m, err
}

// SendAckThreaded is SendAck with a thread tag (see SendThreaded).
func (b *Broker) SendAckThreaded(from, to, body, threadID string) (Message, <-chan struct{}, error) {
	return b.send(from, to, body, threadID, true, nil, false)
}

// SendThreadedAttach is SendThreaded with a file attachment (mess send --attach).
func (b *Broker) SendThreadedAttach(from, to, body, threadID string, attach *Attachment) (Message, error) {
	m, _, err := b.send(from, to, body, threadID, false, attach, false)
	return m, err
}

// SendAsk is Send, flagging the message as a `mess ask` root (see
// Message.Ask) so recv/log rendering and the auto-wake injection can tell the
// recipient it expects a threaded reply (`mess reply`/`--thread <id>`), not a
// plain send back — the asker's `mess ask`/`mess await` wait only detects an
// answer threaded to this message's own ID.
func (b *Broker) SendAsk(from, to, body string) (Message, error) {
	m, _, err := b.send(from, to, body, "", false, nil, true)
	return m, err
}

// Attachment is a file reference recorded alongside a message (mess send/pub
// --attach): a path + content hash (computed client-side, before the request
// is sent — this is a single-machine tool, so the daemon never assumes a
// different filesystem view than the CLI that sent it), plus size/mtime.
type Attachment struct {
	Path  string
	Hash  string // "sha256:<hex>"
	Size  int64
	MTime time.Time
}

// send's from/to are composite keys (agentKey(room, name)) except when to is
// the bare human mailbox handle (never room-scoped — see dispatch). The
// delivered Message always carries bare names.
func (b *Broker) send(from, to, body, threadID string, ack bool, attach *Attachment, isAsk bool) (Message, <-chan struct{}, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if to == "" {
		return Message{}, nil, fmt.Errorf("recipient required")
	}
	_, fromName := splitAgentKey(from)
	_, toName := splitAgentKey(to)
	m := Message{ID: b.nextID(), From: fromName, To: toName, Kind: KindDirect, Body: body, Time: b.now(), AckRequested: ack, ThreadID: threadID, Ask: isAsk}
	if attach != nil {
		m.AttachPath, m.AttachHash, m.AttachSize, m.AttachMTime = attach.Path, attach.Hash, attach.Size, attach.MTime
	}
	b.touch(from)
	if threadID != "" {
		b.trackThreadParticipantLocked(threadID, from)
	}
	b.ensure(to).deliver(m)
	var ackCh chan struct{}
	if ack {
		ackCh = make(chan struct{}, 1)
		b.pendingAcks[m.ID] = ackCh
	}
	b.changed()
	return m, ackCh, nil
}

// trackThreadParticipantLocked records that key has posted in threadID, so a
// later reply in the same thread wakes them (like an @mention) even without
// explicitly naming them — see publishLocalLocked. Caller must hold b.mu.
func (b *Broker) trackThreadParticipantLocked(threadID, key string) {
	if b.threadParticipants[threadID] == nil {
		b.threadParticipants[threadID] = map[string]bool{}
	}
	b.threadParticipants[threadID][key] = true
}

// CancelAck drops a pending read receipt (e.g. when the sender times out).
func (b *Broker) CancelAck(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.pendingAcks, id)
}

// Broadcast delivers to every known agent except the sender itself. from is a
// composite key (agentKey(room, name)); by default the room it decomposes to
// is the scope — an agent in the global room broadcasts to the rest of the
// global room, an agent in a joined room broadcasts only to that room's other
// members. loud (mess broadcast --loud) marks the message so wakes() wakes
// recipients even if their parked wake hook filters out KindBroadcast (the
// standard auto-wake hook parks with --no-broadcast). hostWide (plain --loud,
// as opposed to --loud-room) skips the room filter entirely, reaching every
// room on the host — for host-wide events like a daemon restart, where a
// room boundary would silently leave other rooms unwarned; hostWide is only
// ever true alongside loud (a non-loud broadcast is always room-scoped).
func (b *Broker) Broadcast(from, body string, loud, hostWide bool) (Message, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	room, fromName := splitAgentKey(from)
	m := Message{ID: b.nextID(), From: fromName, Kind: KindBroadcast, Body: body, Time: b.now(), Loud: loud}
	b.touch(from)
	n := 0
	for key, a := range b.agents {
		if key == from {
			continue
		}
		if !hostWide {
			if r, _ := splitAgentKey(key); r != room {
				continue
			}
		}
		a.deliver(m)
		n++
	}
	b.changed()
	return m, n
}

// mentionRe matches an @name mention at a word boundary (so it doesn't fire on
// things like an email's "user@host"). Names are letters/digits/_/- (matching
// agent names like "peri-sonnet-5").
var mentionRe = regexp.MustCompile(`(?:^|\s)@([A-Za-z0-9][A-Za-z0-9_-]*)`)

// mentionsIn returns the set of @-mentioned names in body, or nil if none.
func mentionsIn(body string) map[string]bool {
	matches := mentionRe.FindAllStringSubmatch(body, -1)
	if len(matches) == 0 {
		return nil
	}
	set := make(map[string]bool, len(matches))
	for _, g := range matches {
		set[g[1]] = true
	}
	return set
}

// publishLocalLocked delivers m to every subscriber of the composite topic key
// key except skip (the original sender; pass "" for a relay hop, where nobody
// is skipped). isRelay forces every recipient to be quiet-delivered (no wake)
// unless individually @mentioned — used for bridge relay hops, so a bridge
// between two busy rooms can't become a wake-storm amplifier. A threaded
// message (m.ThreadID != "") is quiet-delivered to everyone except an
// @mention or an existing participant in that thread (someone who has
// already posted in it) — the same noise fix as @mention, but for "a reply
// shouldn't wake everyone the way a fresh topic message does." A direct
// (non-relay, non-threaded) publish keeps today's behavior (no mention ->
// wake everyone). Returns the delivery/wake counts. Caller must hold b.mu.
func (b *Broker) publishLocalLocked(key string, m Message, skip string, isRelay bool) (delivered, woke int) {
	b.appendTopicHistoryLocked(key, m)
	mentions := mentionsIn(m.Body)
	participants := b.threadParticipants[m.ThreadID] // nil if ThreadID=="" or nobody's posted yet
	for subKey := range b.topics[key] {
		if subKey == skip {
			continue
		}
		_, name := splitAgentKey(subKey)
		a := b.ensure(subKey)
		mentioned := len(mentions) > 0 && mentions[name]
		participant := m.ThreadID != "" && participants[subKey]
		switch {
		case mentioned || participant:
			a.deliver(m) // wake: explicitly mentioned, or already in this thread
			woke++
		case isRelay || len(mentions) > 0 || m.ThreadID != "":
			a.deliverQuiet(m) // relay hop, an unmentioned subscriber of a mentioning publish, or an uninvolved subscriber of a threaded reply
		default:
			a.deliver(m) // no mentions, no thread: wake everyone, as before
			woke++
		}
		delivered++
	}
	return delivered, woke
}

// appendTopicHistoryLocked records m in key's own bounded log, independent of
// current subscribers (see topicHistory's field comment). Caller must hold b.mu.
func (b *Broker) appendTopicHistoryLocked(key string, m Message) {
	h := append(b.topicHistory[key], m)
	if len(h) > maxHistory {
		h = h[len(h)-maxHistory:]
	}
	b.topicHistory[key] = h
}

// maxBridgeHops hard-caps how far a single publish can relay across a chain of
// bridges, even if the visited-set cycle guard below ever has a bug.
const maxBridgeHops = 8

// relayLocked walks every bridge touching the composite topic key, delivering
// m to the far side (stamping bridge provenance) and recursing so a publish
// can cross a chain of bridges (A<->B<->C), not just one hop. visited prevents
// re-entering a topic already hit by this publish, so a cycle (A<->B<->A)
// can't ping-pong forever. Every relay hop is quiet-delivered (see
// publishLocalLocked's isRelay) unless individually @mentioned. Caller must
// hold b.mu.
func (b *Broker) relayLocked(key string, m Message, visited map[string]bool, depth int) {
	if depth >= maxBridgeHops {
		elog("bridge relay: hop cap reached for message %s at %s, stopping (possible cycle)", m.ID, key)
		return
	}
	for _, br := range b.bridgesByTopic[key] {
		if br.expired(b.now()) {
			continue
		}
		other, ok := br.otherSide(key)
		if !ok || visited[other] {
			continue
		}
		visited[other] = true
		rm := m
		rm.BridgeID = br.id
		if rm.OriginTopic == "" { // stamp true origin only once, at the first hop
			rm.OriginRoom, rm.OriginTopic = splitTopicKey(key)
		}
		b.publishLocalLocked(other, rm, "", true)
		b.relayLocked(other, rm, visited, depth+1)
	}
}

// Pub delivers to every subscriber of a topic except the sender, and returns the
// delivery count and how many were *woken*. If the body @-mentions subscribers,
// only the mentioned ones are woken (the rest still receive it, read on their
// next recv); with no mentions, everyone is woken as before. from/topic are
// composite keys (agentKey/topicKey(room, ...)); b.topics[topic]'s subscriber
// set holds composite agent keys too, since every subscriber of a room-scoped
// topic is necessarily a member of that same room. Also relays to any bridged
// topic (possibly in another room) — see relayLocked; the returned
// delivered/woke counts cover only the direct local audience, not relay hops.
func (b *Broker) Pub(from, topic, body string) (m Message, delivered, woke int) {
	return b.PubThreaded(from, topic, body, "")
}

// PubThreaded is Pub, tagging the message as a reply within threadID (the
// thread root's own message ID) — see publishLocalLocked for the resulting
// wake-quieting behavior and trackThreadParticipantLocked for how a later
// reply recognizes today's poster as a participant.
func (b *Broker) PubThreaded(from, topic, body, threadID string) (m Message, delivered, woke int) {
	return b.pub(from, topic, body, threadID, nil)
}

// PubThreadedAttach is PubThreaded with a file attachment (mess pub --attach).
func (b *Broker) PubThreadedAttach(from, topic, body, threadID string, attach *Attachment) (m Message, delivered, woke int) {
	return b.pub(from, topic, body, threadID, attach)
}

func (b *Broker) pub(from, topic, body, threadID string, attach *Attachment) (m Message, delivered, woke int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, fromName := splitAgentKey(from)
	_, topicName := splitAgentKey(topic)
	m = Message{ID: b.nextID(), From: fromName, Topic: topicName, Kind: KindTopic, Body: body, Time: b.now(), ThreadID: threadID}
	if attach != nil {
		m.AttachPath, m.AttachHash, m.AttachSize, m.AttachMTime = attach.Path, attach.Hash, attach.Size, attach.MTime
	}
	b.touch(from)
	if threadID != "" {
		b.trackThreadParticipantLocked(threadID, from)
	}
	delivered, woke = b.publishLocalLocked(topic, m, from, false)
	b.relayLocked(topic, m, map[string]bool{topic: true}, 0)
	b.changed()
	return m, delivered, woke
}

// Sub subscribes an agent to a topic. name/topic are composite keys.
func (b *Broker) Sub(name, topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, topicName := splitAgentKey(topic)
	b.ensure(name).topics[topicName] = true
	b.touch(name)
	if b.topics[topic] == nil {
		b.topics[topic] = map[string]bool{}
	}
	b.topics[topic][name] = true
	b.changed()
}

// Unsub removes a topic subscription. name/topic are composite keys.
func (b *Broker) Unsub(name, topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.touch(name)
	_, topicName := splitAgentKey(topic)
	if a := b.agents[name]; a != nil {
		delete(a.topics, topicName)
	}
	if subs := b.topics[topic]; subs != nil {
		delete(subs, name)
		if len(subs) == 0 {
			delete(b.topics, topic)
		}
	}
	b.changed()
}

// maxBridges caps the number of live bridges, guarding against runaway
// creation; --force bypasses it for the rare legitimate case.
const maxBridges = 200

func (b *Broker) nextBridgeID() string {
	b.seq++
	return fmt.Sprintf("br%d", b.seq)
}

// findBridgeLocked returns an existing bridge between composite topic keys a
// and b with the given direction, if any (creation is idempotent unless
// force). Caller must hold b.mu.
func (b *Broker) findBridgeLocked(a, bKey string, dir bridgeDirection) *bridge {
	for _, br := range b.bridges {
		if br.dir == dir && ((br.a == a && br.b == bKey) || (br.a == bKey && br.b == a)) {
			return br
		}
	}
	return nil
}

// Bridge links localRoom/localTopic to remoteRoom/remoteTopic so a publish to
// either side also relays to subscribers on the other (see relayLocked) — the
// explicit escape hatch for cross-room coordination now that topics are
// room-scoped. Creation is idempotent (returns the existing bridge) unless
// force, which also bypasses the maxBridges cap. Every creation is logged
// loudly (elog, never hidden by MESS_DEBUG) since a bridge crosses an
// otherwise-hard isolation boundary and there's no consent from the far room
// to gate on — audit visibility (mess room bridges, ps's Bridged field) is the
// mitigation instead.
func (b *Broker) Bridge(localRoom, localTopic, remoteRoom, remoteTopic string, dir bridgeDirection, creator string, ttl time.Duration, force bool) (*bridge, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ak, bk := topicKey(localRoom, localTopic), topicKey(remoteRoom, remoteTopic)
	if ak == bk {
		return nil, fmt.Errorf("cannot bridge a topic to itself")
	}
	if !force {
		if existing := b.findBridgeLocked(ak, bk, dir); existing != nil {
			return existing, nil // idempotent
		}
		if len(b.bridges) >= maxBridges {
			return nil, fmt.Errorf("bridge limit reached (%d); pass --force or unbridge an unused one", maxBridges)
		}
	}
	id := b.nextBridgeID()
	br := &bridge{
		id: id, a: ak, b: bk,
		aRoom: localRoom, aTopic: localTopic, bRoom: remoteRoom, bTopic: remoteTopic,
		dir: dir, creator: creator, createdAt: b.now(),
	}
	if ttl > 0 {
		br.expiresAt = b.now().Add(ttl)
	}
	b.bridges[id] = br
	b.bridgesByTopic[ak] = append(b.bridgesByTopic[ak], br)
	b.bridgesByTopic[bk] = append(b.bridgesByTopic[bk], br)
	b.changed()
	elog("BRIDGE created: id=%s creator=%s %s <-%s-> %s", id, creator, displayName(localRoom, "#"+localTopic), br.dir.String(), displayName(remoteRoom, "#"+remoteTopic))
	return br, nil
}

// Unbridge tears down a bridge by ID. Unilateral and idempotent, like
// unregister/rm — knowing a bridge's ID already means it was learned about
// through the audit surface (mess room bridges / ps). Returns false (no
// error) for an unknown ID.
func (b *Broker) Unbridge(id string) (bool, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	br, ok := b.bridges[id]
	if !ok {
		return false, ""
	}
	delete(b.bridges, id)
	b.bridgesByTopic[br.a] = removeBridge(b.bridgesByTopic[br.a], br)
	b.bridgesByTopic[br.b] = removeBridge(b.bridgesByTopic[br.b], br)
	if len(b.bridgesByTopic[br.a]) == 0 {
		delete(b.bridgesByTopic, br.a)
	}
	if len(b.bridgesByTopic[br.b]) == 0 {
		delete(b.bridgesByTopic, br.b)
	}
	desc := fmt.Sprintf("%s <-%s-> %s", displayName(br.aRoom, "#"+br.aTopic), br.dir.String(), displayName(br.bRoom, "#"+br.bTopic))
	b.changed()
	return true, desc
}

func removeBridge(list []*bridge, target *bridge) []*bridge {
	out := list[:0]
	for _, br := range list {
		if br != target {
			out = append(out, br)
		}
	}
	return out
}

// bridgeToInfo converts a *bridge to its wire form.
func bridgeToInfo(br *bridge) BridgeInfo {
	return BridgeInfo{
		ID: br.id, ARoom: br.aRoom, ATopic: br.aTopic, BRoom: br.bRoom, BTopic: br.bTopic,
		Direction: br.dir.String(), Creator: br.creator, CreatedAt: br.createdAt, ExpiresAt: br.expiresAt,
	}
}

// ListBridges reports every live (non-expired) bridge, sorted by ID.
func (b *Broker) ListBridges() []BridgeInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	var out []BridgeInfo
	for _, br := range b.bridges {
		if br.expired(now) {
			continue
		}
		out = append(out, bridgeToInfo(br))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Drain returns queued messages for an agent. With peek, messages are left in
// place. max limits the count (0 = all).
func (b *Broker) Drain(name string, peek bool, max int) []Message {
	return b.DrainKinds(name, peek, max, nil)
}

// drainMatchingLocked partitions a's inbox by match, consuming the matched
// messages (firing any pending acks and appending them to history) unless
// peek, and leaving non-matching messages in the inbox in order. Shared by
// DrainKinds and DrainThread, which differ only in what match tests. Caller
// must hold b.mu.
func (b *Broker) drainMatchingLocked(a *agentState, peek bool, max int, match func(Message) bool) []Message {
	var out, keep []Message
	for _, m := range a.inbox {
		if match(m) && (max <= 0 || len(out) < max) {
			out = append(out, m)
		} else {
			keep = append(keep, m)
		}
	}
	if !peek && len(out) > 0 {
		for _, m := range out {
			if m.AckRequested {
				if ch := b.pendingAcks[m.ID]; ch != nil {
					ch <- struct{}{} // buffered(1); never blocks
					delete(b.pendingAcks, m.ID)
				}
			}
		}
		a.inbox = keep
		a.history = append(a.history, out...) // keep for `replay` (recovers a lost wake)
		if len(a.history) > maxHistory {
			a.history = a.history[len(a.history)-maxHistory:]
		}
		b.changed()
	}
	return out
}

// DrainKinds is Drain restricted to the given message kinds (nil = all kinds).
// Non-matching messages are left in the inbox in order, so a filtered waiter
// (e.g. recv --wait --kind direct) ignores broadcast noise without losing it.
func (b *Broker) DrainKinds(name string, peek bool, max int, kinds map[string]bool) []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.ensure(name)
	b.touch(name)
	return b.drainMatchingLocked(a, peek, max, func(m Message) bool {
		return kinds == nil || kinds[m.Kind]
	})
}

// DrainIfIdle drains like DrainKinds, but only if the agent isn't currently
// busy (busyUntil in the future) — checked and acted on under the same lock
// acquisition, so there's no gap between "is this agent busy" and "drain its
// inbox" for a concurrent `mess busy` (a new turn starting) to land in.
//
// This exists for the auto-wake hook specifically: it used to check busy
// status via a separate `mess ps` call and only then issue a separate drain,
// two independent round trips with a real window in between — if a new turn
// started (mess busy) right after the ps check but before the drain, the wake
// hook could steal a message out from under an agent that had just become
// active, leaving its own subsequent `mess recv` to find nothing (the message
// having already been silently drained and handed to the wake hook's own,
// possibly-dropped, stderr injection instead). idle reports whether the drain
// actually ran (false means busy — msgs is always nil in that case).
func (b *Broker) DrainIfIdle(name string, max int, kinds map[string]bool) (msgs []Message, idle bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.busyUntil[name].After(b.now()) {
		return nil, false
	}
	a := b.ensure(name)
	b.touch(name)
	return b.drainMatchingLocked(a, false, max, func(m Message) bool {
		return kinds == nil || kinds[m.Kind]
	}), true
}

// DrainThread is Drain restricted to one thread: a message whose ThreadID
// matches, plus the thread's root message itself (whose own ID equals
// threadID, and which never carries a ThreadID — it isn't a reply to
// anything). Non-matching messages are left in the inbox in order, same as
// DrainKinds — reading a thread doesn't consume the rest of the inbox.
func (b *Broker) DrainThread(name, threadID string, peek bool, max int) []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.ensure(name)
	b.touch(name)
	return b.drainMatchingLocked(a, peek, max, func(m Message) bool {
		return m.ThreadID == threadID || m.ID == threadID
	})
}

// DrainQuiet consumes and returns an agent's whole inbox WITHOUT marking it
// active or firing read receipts — for an operator clearing another agent's
// stuck backlog (`mess drain`). Unlike a real recv it doesn't touch the agent
// (so it stays offline / eligible for cleanup) and doesn't ack (the operator
// read it, not the agent). Returns nil for an unknown agent.
func (b *Broker) DrainQuiet(name string, max int) []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.agents[name]
	if a == nil {
		return nil
	}
	var out, keep []Message
	for _, m := range a.inbox {
		if max <= 0 || len(out) < max {
			out = append(out, m)
		} else {
			keep = append(keep, m)
		}
	}
	if len(out) > 0 {
		a.inbox = keep
		b.changed()
	}
	return out
}

// capAndCopy returns a defensive copy of the most recent n messages in h
// (n<=0 means no cap, i.e. everything). The copy matters because h aliases a
// slice still owned by the broker (an agent's history or a topic's log),
// which can keep growing via append — without a copy, a later append that
// reuses spare capacity in place could silently mutate what looks like an
// already-returned, independent snapshot.
func capAndCopy(h []Message, n int) []Message {
	if n > 0 && n < len(h) {
		h = h[len(h)-n:]
	}
	out := make([]Message, len(h))
	copy(out, h)
	return out
}

// Replay returns the last n messages the agent has already consumed (from its
// bounded history), so a message lost to a dropped wake injection can still be
// recovered. n<=0 returns the whole history (oldest first).
func (b *Broker) Replay(name string, n int) []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.agents[name]
	if a == nil {
		return nil
	}
	return capAndCopy(a.history, n)
}

// ExportTopic returns a topic's own bounded history (topicKey composite key),
// most-recent max messages (0 = all), oldest first. Unlike Replay, this
// exists even if nobody was subscribed at the time a message went by — it's
// the topic's own log, not a recipient's.
func (b *Broker) ExportTopic(topic string, max int) []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	return capAndCopy(b.topicHistory[topic], max)
}

// ExportThread returns name's own view (already-consumed history plus
// whatever's still queued, time-ordered) of one thread: the root message plus
// every reply name has *received*. Peek-only; consumes nothing.
//
// Known gap: a message name sent itself never appears — Pub/Send never add a
// sender's own message to its own inbox (the same "you don't receive your own
// broadcast/topic post" rule recv already follows), so an active participant's
// own replies are invisible to their own export. `mess export --topic`
// doesn't have this gap, since a topic's history is logged once at publish
// time regardless of sender — prefer it when completeness matters more than
// "just my view."
func (b *Broker) ExportThread(name, threadID string, max int) []Message {
	return b.exportOwn(name, max, func(m Message) bool {
		return m.ThreadID == threadID || m.ID == threadID
	})
}

// ExportDirect returns name's own direct-message history with peer
// (bare name), time-ordered. Peek-only; consumes nothing. Same "own received
// view" gap as ExportThread: a message name sent to peer won't appear.
func (b *Broker) ExportDirect(name, peer string, max int) []Message {
	_, peerName := splitAgentKey(peer)
	return b.exportOwn(name, max, func(m Message) bool {
		return m.Kind == KindDirect && (m.From == peerName || m.To == peerName)
	})
}

// exportOwn scans name's own consumed history and current inbox (never
// mutating either) for messages matching, merges and time-sorts them, and
// caps to the most recent max (0 = all).
func (b *Broker) exportOwn(name string, max int, match func(Message) bool) []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.agents[name]
	if a == nil {
		return nil
	}
	var out []Message
	for _, m := range a.history {
		if match(m) {
			out = append(out, m)
		}
	}
	for _, m := range a.inbox {
		if match(m) {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time.Before(out[j].Time) })
	if max > 0 && max < len(out) {
		out = out[len(out)-max:]
	}
	return out
}

// ListThreads summarizes every thread name has seen activity in — from their
// own received view (same history+inbox scan as exportOwn), most recently
// active first. A thread is only discovered once at least one reply (a
// message with ThreadID set) has passed through name's view; the root itself
// (ThreadID=="", ID==threadID) fills in Topic/Peer/RootBody when it's also
// been seen, but its absence doesn't hide the thread — only the two Kind
// classifications below actually differ from the root and never appear until
// the root is present, RootBody/Peer stay best-effort. Participants counts
// server-wide, not just who name has personally seen.
func (b *Broker) ListThreads(name string) []ThreadInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.agents[name]
	if a == nil {
		return nil
	}
	_, myName := splitAgentKey(name)

	all := make([]Message, 0, len(a.history)+len(a.inbox))
	all = append(all, a.history...)
	all = append(all, a.inbox...)

	ids := map[string]bool{}
	for _, m := range all {
		if m.ThreadID != "" {
			ids[m.ThreadID] = true
		}
	}
	if len(ids) == 0 {
		return nil
	}

	infos := make(map[string]*ThreadInfo, len(ids))
	for id := range ids {
		infos[id] = &ThreadInfo{ID: id}
	}
	for _, m := range all {
		id := m.ThreadID
		isRoot := false
		if id == "" {
			if !ids[m.ID] {
				continue // an ordinary message, not part of any thread we're tracking
			}
			id, isRoot = m.ID, true
		}
		info := infos[id]
		if info.Kind == "" {
			info.Kind, info.Topic = m.Kind, m.Topic
		}
		if isRoot {
			info.RootBody = m.Body
		} else {
			info.Replies++
		}
		if info.Kind == KindDirect && info.Peer == "" {
			if m.From != myName {
				info.Peer = m.From
			} else {
				info.Peer = m.To
			}
		}
		if m.Time.After(info.LastActivity) {
			info.LastActivity = m.Time
		}
	}

	out := make([]ThreadInfo, 0, len(infos))
	for id, info := range infos {
		info.Participants = len(b.threadParticipants[id])
		out = append(out, *info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastActivity.After(out[j].LastActivity) })
	return out
}

func matchKind(m Message, kinds map[string]bool) bool {
	return kinds == nil || kinds[m.Kind]
}

// wakes reports whether a message should wake/notify its recipient: either
// it's flagged Loud (an explicit override — see Message.Loud), or it matches
// the kind filter and isn't a Quiet (non-mention) delivery.
func wakes(m Message, kinds map[string]bool) bool {
	return m.Loud || (matchKind(m, kinds) && !m.Quiet)
}

// HasPending reports whether the agent has a queued message that should wake it
// (matching kinds, not a quiet delivery). Quiet messages are received but never
// trigger a wake.
func (b *Broker) HasPending(name string, kinds map[string]bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.agents[name]
	if a == nil {
		return false
	}
	for _, m := range a.inbox {
		if wakes(m, kinds) {
			return true
		}
	}
	return false
}

// waitChan registers a one-shot waiter and returns a channel signaled on the
// next wake-worthy delivery to the agent. It fires immediately only if a matching
// non-quiet message is already queued, so an ignored broadcast or a quiet topic
// message doesn't busy-loop the waiter.
func (b *Broker) waitChan(name string, kinds map[string]bool) <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.ensure(name)
	ch := make(chan struct{}, 1)
	for _, m := range a.inbox {
		if wakes(m, kinds) {
			ch <- struct{}{} // already has a wake-worthy message; fire immediately
			return ch
		}
	}
	a.waiters = append(a.waiters, ch)
	return ch
}

// HasPendingThread is HasPending restricted to one thread (a message whose
// ThreadID matches, or whose own ID equals threadID — the root itself), used
// by `mess await` to check whether an ask has already been answered. Unlike
// HasPending's wake filter, there's no Quiet exemption: any matching message
// counts, since an ask's reply is always meant for the asker specifically.
func (b *Broker) HasPendingThread(name, threadID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.agents[name]
	if a == nil {
		return false
	}
	for _, m := range a.inbox {
		if m.ThreadID == threadID || m.ID == threadID {
			return true
		}
	}
	return false
}

// waitChanThread is waitChan restricted to one thread — see HasPendingThread.
// Reuses the same a.waiters wake mechanism as the kind-based waitChan; a
// reply's deliver() already wakes every waiter unconditionally, so the only
// new part here is the predicate re-checked on wake (see HasPendingThread).
func (b *Broker) waitChanThread(name, threadID string) <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.ensure(name)
	ch := make(chan struct{}, 1)
	for _, m := range a.inbox {
		if m.ThreadID == threadID || m.ID == threadID {
			ch <- struct{}{} // already answered; fire immediately
			return ch
		}
	}
	a.waiters = append(a.waiters, ch)
	return ch
}

// AddListener marks the start of an active streaming listener for an agent.
func (b *Broker) AddListener(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ensure(name)
	b.touch(name)
	b.listeners[name]++
}

// RemoveListener marks the end of a streaming listener.
func (b *Broker) RemoveListener(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.listeners[name] <= 1 {
		delete(b.listeners, name)
	} else {
		b.listeners[name]--
	}
}

// IsListening reports whether an agent has at least one active listener.
func (b *Broker) IsListening(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.listeners[name] > 0
}

// Stat returns an agent's queued-message count and whether it currently has an
// active listener — a cheap one-lock snapshot for diagnostic logging.
func (b *Broker) Stat(name string) (pending int, listening bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if a := b.agents[name]; a != nil {
		pending = len(a.inbox)
	}
	return pending, b.listeners[name] > 0
}

// SetBusy marks an agent as actively in a turn for the given duration (a crash
// backstop; turn-activity hooks refresh it, and Stop clears it).
func (b *Broker) SetBusy(name string, dur time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ensure(name)
	b.touch(name)
	b.clearWarnLocked(name) // becoming active means any prior warning is stale
	b.busyUntil[name] = b.now().Add(dur)
}

// ClearBusy marks an agent as no longer in a turn (called on Stop).
func (b *Broker) ClearBusy(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.busyUntil, name)
}

// SetState records an agent's self-reported working state (empty clears it).
func (b *Broker) SetState(name, state string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ensure(name).state = state
	b.touch(name)
	b.changed()
}

// RemoveAgent forgets an agent entirely — its inbox, topic subscriptions, and
// listener count — e.g. to clear out a dead session. Returns false if unknown.
func (b *Broker) RemoveAgent(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.removeAgentLocked(name) {
		return false
	}
	b.changed()
	return true
}

// removeAgentLocked forgets an agent's presence, inbox, subscriptions, listener
// count, busy/last-seen bookkeeping. Returns false if unknown. Holds the lock;
// the caller fires changed() (so a batch removal persists once).
func (b *Broker) removeAgentLocked(name string) bool {
	if _, ok := b.agents[name]; !ok {
		return false
	}
	delete(b.agents, name)
	delete(b.listeners, name)
	delete(b.busyUntil, name)
	delete(b.lastSeen, name)
	delete(b.owners, name)
	for _, ch := range b.evicts[name] {
		close(ch) // wake any parked recv on this name so it stops instead of ghosting
	}
	delete(b.evicts, name)
	for topic, subs := range b.topics {
		delete(subs, name)
		if len(subs) == 0 {
			delete(b.topics, topic)
		}
	}
	return true
}

// Cleanup prunes agents that look dead: not currently alive (not listening,
// working, or active in the last couple of minutes) AND stale — either no
// activity for longer than maxAge, or mail sitting undrained longer than maxAge.
// The undrained-mail signal is restart-proof (message timestamps persist and
// aren't reset), so it catches long-dead agents even when lastSeen was reset by
// the load-time grace. With dryRun it only reports eligibility. Returns the
// affected agent names, sorted.
func (b *Broker) Cleanup(maxAge time.Duration, dryRun bool) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	var keys, names []string
	for key, a := range b.agents {
		if b.aliveLocked(key) {
			continue // online (listening / working / recently active) — keep
		}
		if isUserHandle(a.name) {
			continue // the human's mailbox — never prune unread pings, in any room
		}
		stale := false
		if seen, ok := b.lastSeen[key]; ok && now.Sub(seen) > maxAge {
			stale = true // no activity for too long
		}
		if len(a.inbox) > 0 && now.Sub(a.inbox[0].Time) > maxAge {
			stale = true // mail undrained for too long — a dead session accumulating
		}
		if stale {
			keys = append(keys, key)
			names = append(names, displayName(a.room, a.name))
		}
	}
	sort.Strings(names)
	sort.Strings(keys)
	if !dryRun && len(keys) > 0 {
		for _, key := range keys {
			b.removeAgentLocked(key)
		}
		b.changed()
	}

	// Opportunistically sweep expired bridges (opt-in via --ttl; most never
	// expire). relayLocked already skips an expired bridge lazily, so this is
	// housekeeping, not a correctness fix — just avoids unbounded growth of the
	// bridges map from a long-running daemon accumulating short-lived bridges.
	if !dryRun {
		var expiredIDs []string
		for id, br := range b.bridges {
			if br.expired(now) {
				expiredIDs = append(expiredIDs, id)
			}
		}
		if len(expiredIDs) > 0 {
			sort.Strings(expiredIDs)
			for _, id := range expiredIDs {
				br := b.bridges[id]
				delete(b.bridges, id)
				b.bridgesByTopic[br.a] = removeBridge(b.bridgesByTopic[br.a], br)
				b.bridgesByTopic[br.b] = removeBridge(b.bridgesByTopic[br.b], br)
			}
			elog("cleanup swept %d expired bridge(s): %v", len(expiredIDs), expiredIDs)
			b.changed()
		}
	}
	return names
}

// ExpireInbox drops unread messages older than maxAge from every agent's
// inbox — regardless of aliveness. This is deliberately a different
// granularity than Cleanup: Cleanup skips any currently-alive agent entirely
// (it only prunes whole *dead* registrations), but a live-yet-sporadic agent
// can still be sitting on 30-day-old unread mail that never gets read, which
// Cleanup would never touch. isUserHandle is exempt unconditionally, same
// carve-out Cleanup makes (the human's own unread pings must never
// auto-vanish). Returns every dropped message with full content — the caller
// is expected to durably record them (see daemon.go's "expire" handling, which
// calls this once with dryRun=true to preview+journal before committing with
// dryRun=false, so a message is never removed without first being logged).
func (b *Broker) ExpireInbox(maxAge time.Duration, dryRun bool) []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	var expired []Message
	for _, a := range b.agents {
		if isUserHandle(a.name) || len(a.inbox) == 0 {
			continue
		}
		var keep []Message
		for _, m := range a.inbox {
			if now.Sub(m.Time) > maxAge {
				expired = append(expired, m)
			} else {
				keep = append(keep, m)
			}
		}
		if !dryRun && len(keep) != len(a.inbox) {
			a.inbox = keep
		}
	}
	if !dryRun && len(expired) > 0 {
		b.changed()
	}
	return expired
}

// Ps reports current agents and topics, sorted for stable output. With
// all==false, only the given room's agents/topics are returned (room=="" is
// the global/default room — the scope everyone is in until they `mess room
// join`); with all==true, room is ignored and everything is returned across
// every room. The human operator's mailbox (isUserHandle) is always included
// regardless of room — there's one human per machine, not one per room (the
// same exception dispatch() and Cleanup already make), so a room-scoped `ps`
// still surfaces them as reachable instead of hiding the one recipient every
// agent can always fall back to.
func (b *Broker) Ps(room string, all bool) ([]AgentInfo, []TopicInfo) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var agents []AgentInfo
	for key, a := range b.agents {
		if !all && a.room != room && !isUserHandle(a.name) {
			continue
		}
		topics := make([]string, 0, len(a.topics))
		for t := range a.topics {
			topics = append(topics, t)
		}
		sort.Strings(topics)
		var oldest time.Time
		if len(a.inbox) > 0 {
			oldest = a.inbox[0].Time // inbox is in arrival order; [0] is oldest
		}
		warning := ""
		if w, ok := b.warnings[key]; ok && w.until.After(b.now()) {
			warning = w.text // expired warnings are simply not reported
		}
		agents = append(agents, AgentInfo{Name: a.name, Room: a.room, Pending: len(a.inbox), Topics: topics, Listening: b.listeners[key] > 0, Working: b.busyUntil[key].After(b.now()), Online: b.aliveLocked(key), State: a.state, Warning: warning, Oldest: oldest})
	}
	sort.Slice(agents, func(i, j int) bool {
		return roomThenNameLess(agents[i].Room, agents[i].Name, agents[j].Room, agents[j].Name)
	})

	var topics []TopicInfo
	for tk, subs := range b.topics {
		tRoom, tName := splitTopicKey(tk)
		if !all && tRoom != room {
			continue
		}
		names := make([]string, 0, len(subs))
		for key := range subs {
			_, n := splitAgentKey(key)
			names = append(names, n)
		}
		sort.Strings(names)
		var bridged []string
		for _, br := range b.bridgesByTopic[tk] {
			bridged = append(bridged, br.describeFrom(tk))
		}
		sort.Strings(bridged)
		topics = append(topics, TopicInfo{Name: tName, Room: tRoom, Subscribers: names, Bridged: bridged})
	}
	sort.Slice(topics, func(i, j int) bool {
		return roomThenNameLess(topics[i].Room, topics[i].Name, topics[j].Room, topics[j].Name)
	})
	return agents, topics
}
