package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

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
		log.Printf("warning: could not load state: %v", err)
	} else {
		d.broker.load(snap)
	}
	d.broker.onChange = d.persist

	log.Printf("mess daemon listening on %s", p.sock)
	go d.acceptLoop()
	<-d.stop
	_ = ln.Close()
	_ = os.Remove(p.sock)
	log.Printf("mess daemon stopped")
	return nil
}

// persist serializes a snapshot to disk. Invoked from broker mutations.
func (d *daemon) persist(s snapshot) {
	d.saveMu.Lock()
	defer d.saveMu.Unlock()
	if err := saveSnapshot(d.paths.state, s); err != nil {
		log.Printf("warning: could not save state: %v", err)
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
				log.Printf("accept error: %v", err)
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
			// new delivery; loop to drain
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
		log.Printf("write error: %v", err)
	}
}

func (d *daemon) dispatch(req Request) Response {
	b := d.broker
	switch req.Op {
	case "ping":
		return Response{OK: true}
	case "register":
		b.Register(req.As)
		return Response{OK: true}
	case "send":
		return d.send(req)
	case "broadcast":
		_, n := b.Broadcast(req.As, req.Body)
		return Response{OK: true, Count: n}
	case "pub":
		_, n := b.Pub(req.As, req.Topic, req.Body)
		return Response{OK: true, Count: n}
	case "sub":
		b.Sub(req.As, req.Topic)
		return Response{OK: true}
	case "unsub":
		b.Unsub(req.As, req.Topic)
		return Response{OK: true}
	case "recv":
		return d.recv(req)
	case "state":
		b.SetState(req.As, req.Body)
		return Response{OK: true}
	case "rm":
		if b.RemoveAgent(req.To) {
			return Response{OK: true, Count: 1}
		}
		return Response{OK: true, Count: 0} // idempotent: unknown agent is not an error
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
		return Response{OK: true, Count: 1}
	}

	// Blocking send: wait for a read receipt, honoring an optional timeout.
	m, ackCh, err := b.SendAck(req.As, req.To, req.Body)
	if err != nil {
		return Response{Error: err.Error()}
	}
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

func (d *daemon) recv(req Request) Response {
	b := d.broker
	trigger := kindSet(req.Kinds)

	// Non-blocking drain: the kind filter acts as a result filter.
	if !req.Wait {
		msgs := b.DrainKinds(req.As, req.Peek, req.Max, trigger)
		return Response{OK: true, Messages: msgs, Count: len(msgs)}
	}

	// Blocking receive: the kind filter is the WAKE TRIGGER (e.g. --no-broadcast
	// means broadcasts don't wake you), but once woken we drain EVERYTHING so no
	// queued message (broadcasts included) is left behind.
	drainAll := func() []Message { return b.DrainKinds(req.As, req.Peek, req.Max, nil) }
	if b.HasPending(req.As, trigger) {
		msgs := drainAll()
		return Response{OK: true, Messages: msgs, Count: len(msgs)}
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
	for {
		ch := b.waitChan(req.As, trigger)
		select {
		case <-ch:
			if b.HasPending(req.As, trigger) {
				if msgs := drainAll(); len(msgs) > 0 {
					return Response{OK: true, Messages: msgs, Count: len(msgs)}
				}
			}
		case <-timeout:
			return Response{OK: true, Messages: nil, Count: 0}
		case <-d.stop:
			return Response{Error: "daemon shutting down"}
		}
	}
}

var errNoDaemon = errors.New("no daemon running")
