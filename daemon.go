package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// debugEnabled gates low-level/noisy logging (benign client-disconnect write
// errors, raw drains). Set MESS_DEBUG=1 to see them. The default, "info" level,
// logs the useful lifecycle events: sends, wakes, parks, and listener churn.
var debugEnabled = os.Getenv("MESS_DEBUG") != ""

// eventLog collapses consecutive identical log messages into a single line
// annotated with a repeat count ("<msg> (×N)"), so a burst of the same event
// (e.g. repeated parks or drains) doesn't flood the log. A message is held until
// the next distinct message arrives or a periodic flush fires, so the count is
// known before the line is written.
type eventLog struct {
	mu    sync.Mutex
	last  string
	count int
}

// events is the daemon-wide deduplicating logger; flushed periodically by a
// ticker started in runDaemon.
var events = &eventLog{}

// log records a message, suppressing it if it equals the pending one (just
// bumping the count) and otherwise flushing the pending one first.
func (e *eventLog) log(msg string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.count > 0 && msg == e.last {
		e.count++
		return
	}
	e.flushLocked()
	e.last, e.count = msg, 1
}

// flushLocked writes the pending message (with its count) and resets. Holds lock.
func (e *eventLog) flushLocked() {
	switch e.count {
	case 0:
		return
	case 1:
		log.Print(e.last)
	default:
		log.Printf("%s (×%d)", e.last, e.count)
	}
	e.count = 0
}

// flush writes any pending message; called on a timer and at shutdown so a
// trailing line (e.g. a lone "parked") isn't held indefinitely.
func (e *eventLog) flush() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.flushLocked()
}

// elog logs a lifecycle event through the deduplicating logger.
func elog(format string, args ...any) {
	events.log(fmt.Sprintf(format, args...))
}

// dlog logs only when MESS_DEBUG is set (through the same dedup path for
// consistent ordering with elog).
func dlog(format string, args ...any) {
	if debugEnabled {
		events.log(fmt.Sprintf(format, args...))
	}
}

// daemon owns the broker, the listener, and the persistence file.
type daemon struct {
	broker *Broker
	paths  paths
	ln     net.Listener

	saveMu sync.Mutex
	stop   chan struct{}
}

// runDaemon starts the server in the foreground. It exits cleanly if another
// daemon already holds the socket.
func runDaemon(p paths) error {
	if err := os.MkdirAll(p.dir, 0o700); err != nil {
		return err
	}
	// If an existing socket answers, another daemon is live; do nothing.
	if c, err := net.DialTimeout("unix", p.sock, 200*time.Millisecond); err == nil {
		c.Close()
		return nil
	}
	// Remove a stale socket left by a crashed daemon.
	_ = os.Remove(p.sock)

	ln, err := net.Listen("unix", p.sock)
	if err != nil {
		return err
	}

	d := &daemon{broker: NewBroker(), paths: p, ln: ln, stop: make(chan struct{})}

	snap, err := loadSnapshotFile(p.state)
	if err != nil {
		elog("warning: could not load state: %v", err)
	} else {
		d.broker.load(snap)
	}
	d.broker.onChange = d.persist

	// Periodically flush the dedup logger so a trailing collapsed line lands.
	flushTicker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-flushTicker.C:
				events.flush()
			case <-d.stop:
				return
			}
		}
	}()

	elog("mess daemon listening on %s", p.sock)
	go d.acceptLoop()
	<-d.stop
	flushTicker.Stop()
	_ = ln.Close()
	_ = os.Remove(p.sock)
	elog("mess daemon stopped")
	events.flush()
	return nil
}

// persist serializes a snapshot to disk. Invoked from broker mutations.
func (d *daemon) persist(s snapshot) {
	d.saveMu.Lock()
	defer d.saveMu.Unlock()
	if err := saveSnapshot(d.paths.state, s); err != nil {
		elog("warning: could not save state: %v", err)
	}
}

func (d *daemon) acceptLoop() {
	for {
		conn, err := d.ln.Accept()
		if err != nil {
			select {
			case <-d.stop:
				return // shutting down
			default:
				elog("accept error: %v", err)
				return
			}
		}
		go d.handle(conn)
	}
}

func (d *daemon) handle(conn net.Conn) {
	defer conn.Close()
	var req Request
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		writeResp(conn, Response{Error: "bad request: " + err.Error()})
		return
	}
	if req.Op == "listen" { // streaming: many responses over one held connection
		d.handleListen(conn, req)
		return
	}
	if req.Op == "recv" { // may block (recv --wait); needs disconnect detection
		writeResp(conn, d.recv(conn, req))
		return
	}
	writeResp(conn, d.dispatch(req))
}

// handleListen streams messages to a long-lived listener connection until the
// client disconnects, an idle timeout elapses, or the daemon shuts down. The
// connection's lifetime is the agent's "is listening" signal.
func (d *daemon) handleListen(conn net.Conn, req Request) {
	if req.As == "" {
		writeResp(conn, Response{Error: "no identity for listen"})
		return
	}
	var idle time.Duration
	if req.Timeout != "" {
		dur, err := time.ParseDuration(req.Timeout)
		if err != nil {
			writeResp(conn, Response{Error: "invalid timeout: " + err.Error()})
			return
		}
		idle = dur
	}

	d.broker.AddListener(req.As)
	defer d.broker.RemoveListener(req.As)
	elog("listen %s start (waiting on %s)", req.As, kindsLabel(req.Kinds))
	defer elog("listen %s end", req.As)

	// Detect client disconnect by watching for EOF on the connection.
	gone := make(chan struct{})
	go func() { io.Copy(io.Discard, conn); close(gone) }()

	enc := json.NewEncoder(conn)
	var timer *time.Timer
	var idleCh <-chan time.Time
	if idle > 0 {
		timer = time.NewTimer(idle)
		idleCh = timer.C
		defer timer.Stop()
	}
	resetIdle := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(idle)
	}

	kinds := kindSet(req.Kinds)
	batch, _ := time.ParseDuration(req.Batch) // 0 on empty/invalid = no batching
	for {
		if msgs := d.broker.DrainKinds(req.As, false, req.Max, kinds); len(msgs) > 0 {
			if err := enc.Encode(Response{OK: true, Messages: msgs, Count: len(msgs)}); err != nil {
				return
			}
			resetIdle()
			continue
		}
		ch := d.broker.waitChan(req.As, kinds)
		select {
		case <-ch:
			// new delivery; pause briefly to coalesce a burst, then loop to drain
			if batch > 0 {
				select {
				case <-time.After(batch):
				case <-gone:
					return
				case <-d.stop:
					return
				}
			}
		case <-idleCh:
			return
		case <-gone:
			return
		case <-d.stop:
			return
		}
	}
}

func writeResp(conn net.Conn, r Response) {
	enc := json.NewEncoder(conn)
	if err := enc.Encode(r); err != nil {
		// A broken pipe just means the client disconnected before reading the
		// reply (the common case for a parked recv whose hook was reaped). Benign.
		dlog("write error: %v", err)
	}
}

func (d *daemon) dispatch(req Request) Response {
	b := d.broker
	switch req.Op {
	case "ping":
		return Response{OK: true}
	case "register":
		if ok, msg := b.RegisterOwned(req.As, req.Session, req.Anchor, req.Force); !ok {
			elog("register %s refused: %s", req.As, msg)
			return Response{Error: msg}
		}
		elog("register %s", req.As)
		return Response{OK: true}
	case "send":
		return d.send(req)
	case "broadcast":
		_, n := b.Broadcast(req.As, req.Body)
		elog("broadcast %s -> %d agent(s)", req.As, n)
		return Response{OK: true, Count: n}
	case "pub":
		_, delivered, woke := b.Pub(req.As, req.Topic, req.Body)
		if woke < delivered {
			elog("pub %s #%s -> %d sub(s), woke %d (@mention)", req.As, req.Topic, delivered, woke)
		} else {
			elog("pub %s #%s -> %d sub(s)", req.As, req.Topic, delivered)
		}
		return Response{OK: true, Count: delivered}
	case "sub":
		b.Sub(req.As, req.Topic)
		return Response{OK: true}
	case "unsub":
		b.Unsub(req.As, req.Topic)
		return Response{OK: true}
	case "state":
		b.SetState(req.As, req.Body)
		return Response{OK: true}
	case "warn":
		ttl := 15 * time.Minute // default; auto-clears even if the agent never recovers
		if req.Timeout != "" {
			if d, err := time.ParseDuration(req.Timeout); err == nil {
				ttl = d
			}
		}
		b.SetWarn(req.As, req.Body, ttl)
		return Response{OK: true}
	case "busy":
		dur := time.Hour // generous crash backstop; turn hooks refresh, Stop clears
		if req.Timeout != "" {
			if d, err := time.ParseDuration(req.Timeout); err == nil {
				dur = d
			}
		}
		b.SetBusy(req.As, dur)
		return Response{OK: true}
	case "unbusy":
		b.ClearBusy(req.As)
		return Response{OK: true}
	case "rm":
		if b.RemoveAgent(req.To) {
			elog("rm %s", req.To)
			return Response{OK: true, Count: 1}
		}
		return Response{OK: true, Count: 0} // idempotent: unknown agent is not an error
	case "drain":
		msgs := b.DrainQuiet(req.As, req.Max) // clear a backlog without touching/acking
		if len(msgs) > 0 {
			elog("drain %s -> %d", req.As, len(msgs))
		}
		return Response{OK: true, Messages: msgs, Count: len(msgs)}
	case "unregister":
		if b.RemoveAgent(req.As) {
			elog("unregister %s", req.As)
			return Response{OK: true, Count: 1}
		}
		return Response{OK: true, Count: 0} // idempotent
	case "rename":
		if ok, msg := b.Rename(req.As, req.To, req.Session, req.Anchor, req.Force); !ok {
			elog("rename %s -> %s refused: %s", req.As, req.To, msg)
			return Response{Error: msg}
		}
		elog("rename %s -> %s", req.As, req.To)
		return Response{OK: true}
	case "cleanup":
		maxAge := 24 * time.Hour
		if req.Timeout != "" {
			if d, err := time.ParseDuration(req.Timeout); err == nil {
				maxAge = d
			} else {
				return Response{Error: "invalid duration: " + req.Timeout}
			}
		}
		names := b.Cleanup(maxAge, req.Peek) // Peek == dry-run
		if len(names) > 0 && !req.Peek {
			elog("cleanup removed %d agent(s): %v", len(names), names)
		}
		return Response{OK: true, Count: len(names), Removed: names}
	case "ps":
		agents, topics := b.Ps()
		return Response{OK: true, Agents: agents, Topics: topics}
	case "stop":
		close(d.stop)
		return Response{OK: true}
	default:
		return Response{Error: fmt.Sprintf("unknown op %q", req.Op)}
	}
}

// kindSet turns a kinds list into a lookup set (nil = no filter / all kinds).
func kindSet(list []string) map[string]bool {
	if len(list) == 0 {
		return nil
	}
	m := make(map[string]bool, len(list))
	for _, k := range list {
		m[k] = true
	}
	return m
}

// timerFor returns a timeout channel (nil = never) and a stop func for an
// optional duration string.
func timerFor(spec string) (<-chan time.Time, func(), error) {
	if spec == "" {
		return nil, func() {}, nil
	}
	dur, err := time.ParseDuration(spec)
	if err != nil {
		return nil, func() {}, fmt.Errorf("invalid timeout: %w", err)
	}
	t := time.NewTimer(dur)
	return t.C, func() { t.Stop() }, nil
}

func (d *daemon) send(req Request) Response {
	b := d.broker
	if !req.Ack {
		if _, err := b.Send(req.As, req.To, req.Body); err != nil {
			return Response{Error: err.Error()}
		}
		pending, listening := b.Stat(req.To)
		elog("send %s -> %s | recipient pending=%d listening=%v", req.As, req.To, pending, listening)
		return Response{OK: true, Count: 1}
	}

	// Blocking send: wait for a read receipt, honoring an optional timeout.
	m, ackCh, err := b.SendAck(req.As, req.To, req.Body)
	if err != nil {
		return Response{Error: err.Error()}
	}
	pending, listening := b.Stat(req.To)
	elog("send %s -> %s (ack) | recipient pending=%d listening=%v", req.As, req.To, pending, listening)
	timeout, stop, err := timerFor(req.Timeout)
	if err != nil {
		b.CancelAck(m.ID)
		return Response{Error: err.Error()}
	}
	defer stop()
	select {
	case <-ackCh:
		return Response{OK: true, Count: 1, Acked: true}
	case <-timeout:
		// Leave the pending receipt registered: a later read still fires it
		// (and self-cleans). We just stop waiting.
		return Response{OK: true, Count: 1, Acked: false}
	case <-d.stop:
		return Response{Error: "daemon shutting down"}
	}
}

func (d *daemon) recv(conn net.Conn, req Request) Response {
	b := d.broker
	trigger := kindSet(req.Kinds)

	// Non-blocking drain: the kind filter acts as a result filter.
	if !req.Wait {
		msgs := b.DrainKinds(req.As, req.Peek, req.Max, trigger)
		if len(msgs) > 0 {
			elog("recv %s drained %d%s", req.As, len(msgs), peekNote(req.Peek))
		} else {
			dlog("recv %s drained 0", req.As)
		}
		return Response{OK: true, Messages: msgs, Count: len(msgs)}
	}

	// Blocking receive: the kind filter is the WAKE TRIGGER (e.g. --no-broadcast
	// means broadcasts don't wake you), but once woken we drain EVERYTHING so no
	// queued message (broadcasts included) is left behind.
	batch, _ := time.ParseDuration(req.Batch) // 0 on empty/invalid = no batching

	// Watch for client disconnect, so a parked waiter whose client dies releases
	// its listener count instead of leaking it (and showing a false "listening").
	gone := make(chan struct{})
	go func() { io.Copy(io.Discard, conn); close(gone) }()

	// finish drains everything and returns. With --batch it first waits a short
	// window so a burst of messages coalesces into a single wake instead of one
	// wake per message.
	finish := func() Response {
		if batch > 0 {
			select {
			case <-time.After(batch):
			case <-gone:
				return Response{Error: "client gone"}
			case <-d.stop:
				return Response{Error: "daemon shutting down"}
			}
		}
		msgs := b.DrainKinds(req.As, req.Peek, req.Max, nil)
		elog("recv %s woke -> drained %d%s", req.As, len(msgs), peekNote(req.Peek))
		return Response{OK: true, Messages: msgs, Count: len(msgs)}
	}

	if b.HasPending(req.As, trigger) {
		return finish()
	}

	timeout, stop, err := timerFor(req.Timeout)
	if err != nil {
		return Response{Error: err.Error()}
	}
	defer stop()
	// A parked recv --wait is the wake primitive: mark the agent reachable for
	// the duration of the wait so it shows as listening and is not flagged idle.
	b.AddListener(req.As)
	defer b.RemoveListener(req.As)
	// Stop waiting if this name is removed/renamed, so the hook exits cleanly
	// instead of lingering as a ghost listener (and being resurrected on restart).
	evicted := b.WatchEvict(req.As)
	defer b.UnwatchEvict(req.As, evicted)
	elog("recv %s parked (waiting on %s)", req.As, kindsLabel(req.Kinds))
	for {
		ch := b.waitChan(req.As, trigger)
		select {
		case <-ch:
			if b.HasPending(req.As, trigger) {
				return finish()
			}
		case <-evicted:
			elog("recv %s evicted (removed/renamed)", req.As)
			return Response{OK: true, Messages: nil, Count: 0} // empty -> hook won't wake or re-park
		case <-timeout:
			elog("recv %s wait timed out (unparked)", req.As)
			return Response{OK: true, Messages: nil, Count: 0}
		case <-gone:
			elog("recv %s client gone (unparked)", req.As) // defer RemoveListener fixes presence
			return Response{Error: "client gone"}
		case <-d.stop:
			return Response{Error: "daemon shutting down"}
		}
	}
}

// peekNote annotates a drain log line when messages were left in place.
func peekNote(peek bool) string {
	if peek {
		return " (peek; left queued)"
	}
	return ""
}

// kindsLabel renders a recv kind filter for logs ("all" when unfiltered).
func kindsLabel(kinds []string) string {
	if len(kinds) == 0 {
		return "all"
	}
	return strings.Join(kinds, ",")
}

var errNoDaemon = errors.New("no daemon running")
