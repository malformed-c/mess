package wire

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Dir resolves the mess state directory the same way mess itself does, so a
// separate module talks to the same daemon without being told where it is.
func Dir() string {
	if d := os.Getenv("MESS_DIR"); d != "" {
		return d
	}
	home, err := os.UserHomeDir()
	if err != nil {
		home = os.TempDir()
	}
	return filepath.Join(home, ".mess")
}

func Socket(dir string) string  { return filepath.Join(dir, "mess.sock") }
func Journal(dir string) string { return filepath.Join(dir, "journal.jsonl") }

// ErrNoDaemon means nothing is listening. A UI should say so and keep running
// rather than exit: the daemon comes and goes, and the journal it already read
// is still worth showing.
var ErrNoDaemon = errors.New("no mess daemon is listening")

// Call sends one request and returns the reply. It deliberately does NOT start
// a daemon: mess's own client may, because running a mess command implies
// wanting mess, but a viewer opening a window does not.
func Call(sock string, req Request) (Response, error) {
	conn, err := net.DialTimeout("unix", sock, 2*time.Second)
	if err != nil {
		return Response{}, ErrNoDaemon
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return Response{}, err
	}
	var resp Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return Response{}, err
	}
	if !resp.OK && resp.Error != "" {
		return resp, errors.New(resp.Error)
	}
	return resp, nil
}
