package main

import (
	"fmt"
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

	// owners records which host session (and terminal anchor) currently owns each
	// name, so register can guard against two live sessions claiming one name
	// while still allowing a rotated session (same anchor) to reclaim its own.
	owners map[string]ownerInfo

	// warnings holds a transient status warning per agent (e.g. an API error set
	// by a lifecycle hook). It auto-clears when the agent is next active and
	// self-expires, so stale warnings don't linger in `ps`.
	warnings map[string]warnInfo

	// onChange is invoked (while holding the lock) after every mutation so the
	// caller can persist state. It receives a snapshot to serialize.
	onChange func(snapshot)
}

type agentState struct {
	name    string
	inbox   []Message
	topics  map[string]bool
	state   string          // self-reported "what I'm working on"
	waiters []chan struct{} // signaled (then dropped) on next delivery
}

// ownerInfo identifies the host session that registered a name, and its stable
// terminal anchor (empty when unavailable).
type ownerInfo struct {
	session string
	anchor  string
}

// warnInfo is a transient status warning and its expiry.
type warnInfo struct {
	text  string
	until time.Time
}

// NewBroker returns an empty broker.
func NewBroker() *Broker {
	return &Broker{
		agents:      map[string]*agentState{},
		topics:      map[string]map[string]bool{},
		pendingAcks: map[string]chan struct{}{},
		listeners:   map[string]int{},
		busyUntil:   map[string]time.Time{},
		lastSeen:    map[string]time.Time{},
		owners:      map[string]ownerInfo{},
		warnings:    map[string]warnInfo{},
		now:         time.Now,
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

func (b *Broker) ensure(name string) *agentState {
	a := b.agents[name]
	if a == nil {
		a = &agentState{name: name, topics: map[string]bool{}}
		b.agents[name] = a
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

// RegisterOwned registers name on behalf of a host session (and terminal
// anchor), guarding against a *different, still-live* session in a *different*
// terminal claiming a name already in use. It returns ok=false and a message on
// such a collision, unless force is set. A rotated session that shares the
// current owner's anchor, or a takeover of a name whose owner is no longer live,
// is allowed — that's identity recovery, not a collision.
func (b *Broker) RegisterOwned(name, session, anchor string, force bool) (bool, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cur, ok := b.owners[name]; ok && !force && cur.session != session {
		sameTerminal := anchor != "" && anchor == cur.anchor
		if !sameTerminal && b.aliveLocked(name) {
			return false, fmt.Sprintf("name %q is in use by another live session; choose a different name or pass --force to take it over", name)
		}
	}
	b.ensure(name)
	b.touch(name)
	b.clearWarnLocked(name) // re-registering (fresh/resumed session) clears a stale warning
	b.owners[name] = ownerInfo{session: session, anchor: anchor}
	b.changed()
	return true, ""
}

// Rename moves an agent from old to new, migrating its inbox, topic
// subscriptions, state, and busy/last-seen bookkeeping, then removing old. It
// honors the same collision guard as RegisterOwned on the destination name
// (refuses a different live session's name unless force). Returns ok=false and a
// message on collision.
func (b *Broker) Rename(old, newName, session, anchor string, force bool) (bool, string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if newName == "" {
		return false, "new name required"
	}
	if old == newName {
		b.ensure(newName)
		b.touch(newName)
		b.owners[newName] = ownerInfo{session: session, anchor: anchor}
		b.changed()
		return true, ""
	}
	if cur, ok := b.owners[newName]; ok && !force && cur.session != session {
		sameTerminal := anchor != "" && anchor == cur.anchor
		if !sameTerminal && b.aliveLocked(newName) {
			return false, fmt.Sprintf("name %q is in use by another live session; choose a different name or pass --force to take it over", newName)
		}
	}

	dst := b.ensure(newName)
	if src := b.agents[old]; src != nil {
		dst.inbox = append(dst.inbox, src.inbox...)
		if dst.state == "" {
			dst.state = src.state
		}
		for topic := range src.topics {
			dst.topics[topic] = true
			if b.topics[topic] == nil {
				b.topics[topic] = map[string]bool{}
			}
			b.topics[topic][newName] = true
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
	b.owners[newName] = ownerInfo{session: session, anchor: anchor}
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
	m, _, err := b.send(from, to, body, false)
	return m, err
}

// SendAck delivers a direct message and returns a channel that fires once the
// recipient reads (consumes) it. The caller can block on the channel, with its
// own timeout, to implement a read receipt.
func (b *Broker) SendAck(from, to, body string) (Message, <-chan struct{}, error) {
	return b.send(from, to, body, true)
}

func (b *Broker) send(from, to, body string, ack bool) (Message, <-chan struct{}, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if to == "" {
		return Message{}, nil, fmt.Errorf("recipient required")
	}
	m := Message{ID: b.nextID(), From: from, To: to, Kind: KindDirect, Body: body, Time: b.now(), AckRequested: ack}
	b.touch(from)
	b.ensure(to).deliver(m)
	var ackCh chan struct{}
	if ack {
		ackCh = make(chan struct{}, 1)
		b.pendingAcks[m.ID] = ackCh
	}
	b.changed()
	return m, ackCh, nil
}

// CancelAck drops a pending read receipt (e.g. when the sender times out).
func (b *Broker) CancelAck(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.pendingAcks, id)
}

// Broadcast delivers to every known agent except the sender.
func (b *Broker) Broadcast(from, body string) (Message, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	m := Message{ID: b.nextID(), From: from, Kind: KindBroadcast, Body: body, Time: b.now()}
	b.touch(from)
	n := 0
	for name, a := range b.agents {
		if name == from {
			continue
		}
		a.deliver(m)
		n++
	}
	b.changed()
	return m, n
}

// Pub delivers to every subscriber of a topic except the sender.
func (b *Broker) Pub(from, topic, body string) (Message, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	m := Message{ID: b.nextID(), From: from, Topic: topic, Kind: KindTopic, Body: body, Time: b.now()}
	b.touch(from)
	n := 0
	for name := range b.topics[topic] {
		if name == from {
			continue
		}
		b.ensure(name).deliver(m)
		n++
	}
	b.changed()
	return m, n
}

// Sub subscribes an agent to a topic.
func (b *Broker) Sub(name, topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ensure(name).topics[topic] = true
	b.touch(name)
	if b.topics[topic] == nil {
		b.topics[topic] = map[string]bool{}
	}
	b.topics[topic][name] = true
	b.changed()
}

// Unsub removes a topic subscription.
func (b *Broker) Unsub(name, topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.touch(name)
	if a := b.agents[name]; a != nil {
		delete(a.topics, topic)
	}
	if subs := b.topics[topic]; subs != nil {
		delete(subs, name)
		if len(subs) == 0 {
			delete(b.topics, topic)
		}
	}
	b.changed()
}

// Drain returns queued messages for an agent. With peek, messages are left in
// place. max limits the count (0 = all).
func (b *Broker) Drain(name string, peek bool, max int) []Message {
	return b.DrainKinds(name, peek, max, nil)
}

// DrainKinds is Drain restricted to the given message kinds (nil = all kinds).
// Non-matching messages are left in the inbox in order, so a filtered waiter
// (e.g. recv --wait --kind direct) ignores broadcast noise without losing it.
func (b *Broker) DrainKinds(name string, peek bool, max int, kinds map[string]bool) []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.ensure(name)
	b.touch(name)
	var out, keep []Message
	for _, m := range a.inbox {
		if (kinds == nil || kinds[m.Kind]) && (max <= 0 || len(out) < max) {
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
		b.changed()
	}
	return out
}

func matchKind(m Message, kinds map[string]bool) bool {
	return kinds == nil || kinds[m.Kind]
}

// HasPending reports whether the agent has a queued message matching kinds
// (nil = any kind).
func (b *Broker) HasPending(name string, kinds map[string]bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.agents[name]
	if a == nil {
		return false
	}
	for _, m := range a.inbox {
		if matchKind(m, kinds) {
			return true
		}
	}
	return false
}

// waitChan registers a one-shot waiter and returns a channel signaled on the
// next delivery to the agent. It fires immediately only if a message matching
// kinds is already queued, so a non-matching leftover (e.g. an ignored
// broadcast) doesn't busy-loop a filtered waiter.
func (b *Broker) waitChan(name string, kinds map[string]bool) <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.ensure(name)
	ch := make(chan struct{}, 1)
	for _, m := range a.inbox {
		if matchKind(m, kinds) {
			ch <- struct{}{} // already has a matching message; fire immediately
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
	for topic, subs := range b.topics {
		delete(subs, name)
		if len(subs) == 0 {
			delete(b.topics, topic)
		}
	}
	return true
}

// Cleanup prunes agents idle longer than maxAge — last active over maxAge ago
// (or never seen) and not currently listening, so live/parked sessions are kept.
// With dryRun it only reports who is eligible without removing anything. Returns
// the affected agent names, sorted.
func (b *Broker) Cleanup(maxAge time.Duration, dryRun bool) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	var names []string
	for name := range b.agents {
		if b.listeners[name] > 0 {
			continue // reachable right now — never prune
		}
		seen, ok := b.lastSeen[name]
		if ok && now.Sub(seen) <= maxAge {
			continue // active recently enough
		}
		names = append(names, name)
	}
	sort.Strings(names)
	if !dryRun && len(names) > 0 {
		for _, name := range names {
			b.removeAgentLocked(name)
		}
		b.changed()
	}
	return names
}

// Ps reports current agents and topics, sorted for stable output.
func (b *Broker) Ps() ([]AgentInfo, []TopicInfo) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var agents []AgentInfo
	for _, a := range b.agents {
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
		if w, ok := b.warnings[a.name]; ok && w.until.After(b.now()) {
			warning = w.text // expired warnings are simply not reported
		}
		agents = append(agents, AgentInfo{Name: a.name, Pending: len(a.inbox), Topics: topics, Listening: b.listeners[a.name] > 0, Working: b.busyUntil[a.name].After(b.now()), Online: b.aliveLocked(a.name), State: a.state, Warning: warning, Oldest: oldest})
	}
	sort.Slice(agents, func(i, j int) bool { return agents[i].Name < agents[j].Name })

	var topics []TopicInfo
	for t, subs := range b.topics {
		names := make([]string, 0, len(subs))
		for n := range subs {
			names = append(names, n)
		}
		sort.Strings(names)
		topics = append(topics, TopicInfo{Name: t, Subscribers: names})
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	return agents, topics
}
