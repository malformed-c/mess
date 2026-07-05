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
	// A raw request could otherwise forge a composite-key collision by smuggling
	// the room separator inside a name/topic (e.g. As: "x\x00admin" impersonating
	// room "x"'s "admin" while claiming Room ""). No legitimate CLI input can
	// contain this control byte, so reject it outright.
	if strings.ContainsRune(req.Room, 0) || strings.ContainsRune(req.As, 0) ||
		strings.ContainsRune(req.To, 0) || strings.ContainsRune(req.Topic, 0) {
		writeResp(conn, Response{Error: "invalid request: control byte not allowed in room/as/to/topic"})
		return
	}
	// Defense in depth: for any op where req.As is the caller's *own* identity,
	// refuse to let a different live session act under a name it doesn't own —
	// even if the client's identity resolution ever produced the wrong name.
	// The identity checked/claimed is room-scoped (agentKey(req.Room, req.As)),
	// so "admin" in one room never collides with "admin" in another.
	if actsAsSelf(req.Op) {
		who := agentKey(req.Room, req.As)
		if ok, msg := d.broker.ClaimIdentity(who, req.Session); !ok {
			elog("%s as %s refused: %s", req.Op, req.As, msg)
			writeResp(conn, Response{Error: msg})
			return
		}
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
	who := agentKey(req.Room, req.As)
	var idle time.Duration
	if req.Timeout != "" {
		dur, err := time.ParseDuration(req.Timeout)
		if err != nil {
			writeResp(conn, Response{Error: "invalid timeout: " + err.Error()})
			return
		}
		idle = dur
	}

	d.broker.AddListener(who)
	defer d.broker.RemoveListener(who)
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
		if msgs := d.broker.DrainKinds(who, false, req.Max, kinds); len(msgs) > 0 {
			if err := enc.Encode(Response{OK: true, Messages: msgs, Count: len(msgs)}); err != nil {
				return
			}
			resetIdle()
			continue
		}
		ch := d.broker.waitChan(who, kinds)
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

// actsAsSelf reports whether an op's req.As is the caller's *own* identity (so it
// should pass the session-ownership gate). Excludes register/rename (their own
// guard honors --force) and target-addressed ops (rm/drain use a name argument,
// not the caller's identity).
func actsAsSelf(op string) bool {
	switch op {
	case "send", "broadcast", "pub", "sub", "unsub", "state", "warn",
		"busy", "unbusy", "recv", "listen", "replay", "unregister", "room-leave",
		"bridge", "unbridge", "export":
		return true
	}
	return false
}

// parseBridgeDirection turns the wire "out"/"in"/"" into a bridgeDirection
// (default bridgeBoth for an empty/unrecognized value).
func parseBridgeDirection(s string) bridgeDirection {
	switch s {
	case "out":
		return bridgeAToB
	case "in":
		return bridgeBToA
	default:
		return bridgeBoth
	}
}

func (d *daemon) dispatch(req Request) Response {
	b := d.broker
	// Strip an accidental leading "#" from a topic argument: topics are always
	// displayed as #name (ps, README examples), so `mess sub #trail` is a natural
	// typo for `mess sub trail` — without this, it silently creates/targets a
	// distinct "#trail" topic instead of colliding with the intended "trail" one.
	req.Topic = strings.TrimPrefix(req.Topic, "#")

	// Room-scope the caller's identity, the addressed recipient/target, and the
	// topic — except the human mailbox (`user`/login name), which is a single
	// global handle regardless of the sender's room: there's one human operator
	// per machine, not one per room, so `mess send user`/`@user` must always
	// reach it no matter who sends it.
	who := agentKey(req.Room, req.As)
	to := agentKey(req.Room, req.To)
	if isUserHandle(req.To) {
		to = req.To
	}
	topic := topicKey(req.Room, req.Topic)

	switch req.Op {
	case "ping":
		return Response{OK: true}
	case "register":
		if ok, msg := b.RegisterOwned(who, req.Session, req.Force); !ok {
			elog("register %s refused: %s", req.As, msg)
			return Response{Error: msg}
		}
		elog("register %s", req.As)
		return Response{OK: true}
	case "room-join":
		if ok, msg := b.RegisterOwned(who, req.Session, req.Force); !ok {
			elog("room-join %s in %q refused: %s", req.As, req.Room, msg)
			return Response{Error: msg}
		}
		elog("room-join %s in %q", req.As, req.Room)
		return Response{OK: true}
	case "room-leave":
		if b.RemoveAgent(who) {
			elog("room-leave %s from %q", req.As, req.Room)
			return Response{OK: true, Count: 1}
		}
		return Response{OK: true, Count: 0} // idempotent
	case "send":
		resp := d.send(req, who, to)
		if resp.Error == "" {
			notifyUser(req.As, req.To, req.Body) // ping the human on a direct-to-mailbox or @mention
		}
		return resp
	case "broadcast":
		_, n := b.Broadcast(who, req.Body, req.Loud)
		if req.Loud {
			notifyUserLoud(req.As, req.Body)
			elog("broadcast %s -> %d agent(s) (loud)", req.As, n)
		} else {
			notifyUser(req.As, "", req.Body)
			elog("broadcast %s -> %d agent(s)", req.As, n)
		}
		return Response{OK: true, Count: n}
	case "pub":
		_, delivered, woke := b.PubThreaded(who, topic, req.Body, req.ThreadID)
		notifyUser(req.As, "", req.Body)
		if woke < delivered {
			elog("pub %s #%s -> %d sub(s), woke %d (@mention/thread)", req.As, req.Topic, delivered, woke)
		} else {
			elog("pub %s #%s -> %d sub(s)", req.As, req.Topic, delivered)
		}
		return Response{OK: true, Count: delivered}
	case "sub":
		b.Sub(who, topic)
		return Response{OK: true}
	case "unsub":
		b.Unsub(who, topic)
		return Response{OK: true}
	case "bridge":
		// The local side defaults to the caller's ambient room (req.Room, already
		// filled by client.go's withRoom); --local-room overrides it but requires
		// --force unless it matches, since otherwise any caller could bridge on
		// behalf of a room it isn't even in.
		localRoom := req.Room
		if req.LocalRoom != "" {
			if req.LocalRoom != req.Room && !req.Force {
				return Response{Error: fmt.Sprintf("not currently in room %q; pass --force to bridge on its behalf", req.LocalRoom)}
			}
			localRoom = req.LocalRoom
		}
		remoteTopic := strings.TrimPrefix(req.RemoteTopic, "#")
		dir := parseBridgeDirection(req.Direction)
		var ttl time.Duration
		if req.Timeout != "" {
			if d, err := time.ParseDuration(req.Timeout); err == nil {
				ttl = d
			}
		}
		br, err := b.Bridge(localRoom, req.Topic, req.RemoteRoom, remoteTopic, dir, req.As, ttl, req.Force)
		if err != nil {
			elog("bridge %s -> %s refused: %s", displayName(localRoom, req.Topic), displayName(req.RemoteRoom, remoteTopic), err)
			return Response{Error: err.Error()}
		}
		return Response{OK: true, Bridges: []BridgeInfo{bridgeToInfo(br)}}
	case "unbridge":
		if ok, desc := b.Unbridge(req.BridgeID); ok {
			elog("BRIDGE removed: id=%s by=%s (%s)", req.BridgeID, req.As, desc)
			return Response{OK: true, Count: 1}
		}
		return Response{OK: true, Count: 0} // idempotent
	case "bridges":
		return Response{OK: true, Bridges: b.ListBridges()}
	case "export":
		var msgs []Message
		switch {
		case req.Topic != "":
			msgs = b.ExportTopic(topic, req.Max)
		case req.ThreadID != "":
			msgs = b.ExportThread(who, req.ThreadID, req.Max)
		case req.To != "":
			msgs = b.ExportDirect(who, to, req.Max)
		default:
			return Response{Error: "export requires --topic, --thread, or --to"}
		}
		return Response{OK: true, Messages: msgs, Count: len(msgs)}
	case "state":
		b.SetState(who, req.Body)
		return Response{OK: true}
	case "warn":
		ttl := 15 * time.Minute // default; auto-clears even if the agent never recovers
		if req.Timeout != "" {
			if d, err := time.ParseDuration(req.Timeout); err == nil {
				ttl = d
			}
		}
		b.SetWarn(who, req.Body, ttl)
		return Response{OK: true}
	case "busy":
		dur := time.Hour // generous crash backstop; turn hooks refresh, Stop clears
		if req.Timeout != "" {
			if d, err := time.ParseDuration(req.Timeout); err == nil {
				dur = d
			}
		}
		b.SetBusy(who, dur)
		return Response{OK: true}
	case "unbusy":
		b.ClearBusy(who)
		return Response{OK: true}
	case "rm":
		if b.RemoveAgent(to) {
			elog("rm %s", req.To)
			return Response{OK: true, Count: 1}
		}
		return Response{OK: true, Count: 0} // idempotent: unknown agent is not an error
	case "drain":
		msgs := b.DrainQuiet(who, req.Max) // clear a backlog without touching/acking
		if len(msgs) > 0 {
			elog("drain %s -> %d", req.As, len(msgs))
		}
		return Response{OK: true, Messages: msgs, Count: len(msgs)}
	case "replay":
		msgs := b.Replay(who, req.Max) // recently-consumed history (recover a lost wake)
		return Response{OK: true, Messages: msgs, Count: len(msgs)}
	case "unregister":
		if b.RemoveAgent(who) {
			elog("unregister %s", req.As)
			return Response{OK: true, Count: 1}
		}
		return Response{OK: true, Count: 0} // idempotent
	case "rename":
		// Rename stays within one room: old and new are composited with the same
		// req.Room. Moving to a *different* room is `mess room join` instead.
		if ok, msg := b.Rename(who, to, req.Session, req.Force); !ok {
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
		agents, topics := b.Ps(req.Room, req.All)
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

func (d *daemon) send(req Request, who, to string) Response {
	b := d.broker
	if !req.Ack {
		if _, err := b.SendThreaded(who, to, req.Body, req.ThreadID); err != nil {
			return Response{Error: err.Error()}
		}
		pending, listening := b.Stat(to)
		elog("send %s -> %s | recipient pending=%d listening=%v", req.As, req.To, pending, listening)
		return Response{OK: true, Count: 1}
	}

	// Blocking send: wait for a read receipt, honoring an optional timeout.
	m, ackCh, err := b.SendAckThreaded(who, to, req.Body, req.ThreadID)
	if err != nil {
		return Response{Error: err.Error()}
	}
	pending, listening := b.Stat(to)
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
	who := agentKey(req.Room, req.As)
	trigger := kindSet(req.Kinds)

	// Non-blocking drain: the kind filter acts as a result filter. A ThreadID
	// filter takes over entirely (mess recv --thread is a "show me this
	// conversation" query, not a wake-worthiness filter) — not combined with
	// --wait, which stays kind-filtered as today.
	if !req.Wait && req.ThreadID != "" {
		msgs := b.DrainThread(who, req.ThreadID, req.Peek, req.Max)
		if len(msgs) > 0 {
			elog("recv %s drained %d (thread %s)%s", req.As, len(msgs), req.ThreadID, peekNote(req.Peek))
		} else {
			dlog("recv %s drained 0 (thread %s)", req.As, req.ThreadID)
		}
		return Response{OK: true, Messages: msgs, Count: len(msgs)}
	}
	if !req.Wait {
		msgs := b.DrainKinds(who, req.Peek, req.Max, trigger)
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
		msgs := b.DrainKinds(who, req.Peek, req.Max, nil)
		elog("recv %s woke -> drained %d%s", req.As, len(msgs), peekNote(req.Peek))
		return Response{OK: true, Messages: msgs, Count: len(msgs)}
	}

	if b.HasPending(who, trigger) {
		return finish()
	}

	timeout, stop, err := timerFor(req.Timeout)
	if err != nil {
		return Response{Error: err.Error()}
	}
	defer stop()
	// A parked recv --wait is the wake primitive: mark the agent reachable for
	// the duration of the wait so it shows as listening and is not flagged idle.
	b.AddListener(who)
	defer b.RemoveListener(who)
	// Stop waiting if this name is removed/renamed, so the hook exits cleanly
	// instead of lingering as a ghost listener (and being resurrected on restart).
	evicted := b.WatchEvict(who)
	defer b.UnwatchEvict(who, evicted)
	elog("recv %s parked (waiting on %s)", req.As, kindsLabel(req.Kinds))
	for {
		ch := b.waitChan(who, trigger)
		select {
		case <-ch:
			if b.HasPending(who, trigger) {
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
