package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// withSession stamps the caller's host session id onto a request (unless already
// set), so the daemon can bind names to sessions and reject a different live
// session acting under a name it doesn't own. Empty when run outside a supported
// host agent — the daemon then skips the ownership check.
func withSession(req Request) Request {
	if req.Session == "" {
		req.Session = sessionID()
	}
	return req
}

// withRoom stamps the caller's currently-joined room onto a request (unless
// already set), the same way withSession stamps the host session id. This is
// what makes rooms ambient: send/broadcast/pub/sub/recv/etc. all transparently
// inherit the caller's joined room with no per-command flag, since every call
// site already routes through call/callWait/callStream.
//
// req.Global skips the auto-fill, leaving Room=="" as an explicit choice
// rather than "unset" — without it, an empty Room is ambiguous: it could
// mean "target the global room" or "no override, fall back to ambient",
// and there was no way to ask for the former. Set by send/ask's --global
// flag.
func withRoom(p paths, req Request) Request {
	if req.Room == "" && !req.Global {
		req.Room = resolveRoom(p, "")
	}
	return req
}

// call sends one request to the daemon, auto-starting it if needed.
func call(p paths, req Request) (Response, error) {
	req = withSession(req)
	req = withRoom(p, req)
	conn, err := dialOrStart(p)
	if err != nil {
		return Response{}, err
	}
	defer conn.Close()
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return Response{}, err
	}
	var resp Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return Response{}, err
	}
	if !resp.OK && resp.Error != "" {
		return resp, daemonError(resp.Error)
	}
	return resp, nil
}

// daemonError clarifies the cryptic "unknown op" the daemon returns when it is
// older than this CLI (a stale daemon still running while the binary upgraded).
func daemonError(msg string) error {
	if strings.Contains(msg, "unknown op") {
		return fmt.Errorf("%s — the running mess daemon is older than this CLI; run `mess stop` and retry (it auto-restarts on the current binary)", msg)
	}
	return fmt.Errorf("%s", msg)
}

// callWait runs a blocking recv that survives a daemon restart: if the
// connection drops (or the daemon reports it is shutting down) before a real
// response, it waits for the daemon to come back and re-issues, so a parked
// receiver (e.g. the auto-wake hook) stays armed across a `mess stop`+restart.
// It does not force-start the daemon on reconnect, so a genuine stop still ends
// the wait once the daemon stays down past the reconnect window.
func callWait(p paths, req Request) (Response, error) {
	req = withSession(req)
	req = withRoom(p, req)
	first := true
	for {
		var conn net.Conn
		var err error
		if first {
			conn, err = dialOrStart(p) // initial attempt may start the daemon
			first = false
		} else {
			conn, err = net.Dial("unix", p.sock) // reconnect: don't resurrect a stopped daemon
		}
		if err != nil {
			if waitForDaemon(p) {
				continue
			}
			return Response{}, err
		}
		encErr := json.NewEncoder(conn).Encode(req)
		var resp Response
		var decErr error
		if encErr == nil {
			decErr = json.NewDecoder(conn).Decode(&resp)
		}
		conn.Close()
		if encErr != nil || decErr != nil { // connection dropped mid-wait (daemon bounced)
			if waitForDaemon(p) {
				continue
			}
			return Response{}, fmt.Errorf("lost connection to daemon")
		}
		if resp.Error == "daemon shutting down" { // restart in progress
			if waitForDaemon(p) {
				continue
			}
			return Response{}, fmt.Errorf("daemon stopped")
		}
		if !resp.OK && resp.Error != "" {
			return resp, daemonError(resp.Error)
		}
		return resp, nil
	}
}

// waitForDaemon polls (without starting one) for the daemon to become reachable
// again, e.g. while a restart is in flight. Returns false if it stays down.
func waitForDaemon(p paths) bool {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("unix", p.sock); err == nil {
			c.Close()
			return true
		}
		time.Sleep(150 * time.Millisecond)
	}
	return false
}

// callStream sends one request and invokes onResp for each response streamed
// back over the held connection, until the daemon closes it (EOF).
func callStream(p paths, req Request, onResp func(Response) error) error {
	req = withSession(req)
	req = withRoom(p, req)
	conn, err := dialOrStart(p)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return err
	}
	dec := json.NewDecoder(conn)
	for {
		var resp Response
		if err := dec.Decode(&resp); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if !resp.OK && resp.Error != "" {
			return daemonError(resp.Error)
		}
		if err := onResp(resp); err != nil {
			return err
		}
	}
}

// dialOrStart connects to the daemon, spawning one if the socket is dead.
func dialOrStart(p paths) (net.Conn, error) {
	if conn, err := net.Dial("unix", p.sock); err == nil {
		return conn, nil
	}
	if err := startDaemon(p); err != nil {
		return nil, err
	}
	// Poll for the socket to come up.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.Dial("unix", p.sock); err == nil {
			return conn, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil, fmt.Errorf("daemon did not start (see %s)", p.log)
}

// startDaemon launches a detached background daemon process.
func startDaemon(p paths) error {
	if err := os.MkdirAll(p.dir, 0o700); err != nil {
		return err
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	logf, err := os.OpenFile(p.log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer logf.Close()

	cmd := exec.Command(self, "daemon")
	cmd.Env = os.Environ()
	cmd.Stdout = logf
	cmd.Stderr = logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // detach from this process group
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}
