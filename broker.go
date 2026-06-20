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

// NewBroker returns an empty broker.
func NewBroker() *Broker {
	return &Broker{
		agents:      map[string]*agentState{},
		topics:      map[string]map[string]bool{},
		pendingAcks: map[string]chan struct{}{},
		listeners:   map[string]int{},
		now:         time.Now,
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

// Register makes an agent known so it can receive broadcasts.
func (b *Broker) Register(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ensure(name)
	b.changed()
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

// SetState records an agent's self-reported working state (empty clears it).
func (b *Broker) SetState(name, state string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ensure(name).state = state
	b.changed()
}

// RemoveAgent forgets an agent entirely — its inbox, topic subscriptions, and
// listener count — e.g. to clear out a dead session. Returns false if unknown.
func (b *Broker) RemoveAgent(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.agents[name]; !ok {
		return false
	}
	delete(b.agents, name)
	delete(b.listeners, name)
	for topic, subs := range b.topics {
		delete(subs, name)
		if len(subs) == 0 {
			delete(b.topics, topic)
		}
	}
	b.changed()
	return true
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
		agents = append(agents, AgentInfo{Name: a.name, Pending: len(a.inbox), Topics: topics, Listening: b.listeners[a.name] > 0, State: a.state})
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
