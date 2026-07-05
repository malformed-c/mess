package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// lastMsgInfo is the most recent message this session has seen via `mess
// recv` — the implicit default root for `mess reply` when no thread is
// already open. Persisted per session id, mirroring identity.go's pattern.
type lastMsgInfo struct {
	ID    string `json:"id"`
	Kind  string `json:"kind"`
	Topic string `json:"topic,omitempty"` // set for KindTopic
	From  string `json:"from"`            // set for KindDirect (who to reply to)
}

// openThreadInfo is the thread `mess reply` is currently continuing, if any.
// Set the first time `mess reply` starts a thread from lastMsgInfo; cleared
// by `mess thread close`.
type openThreadInfo struct {
	ThreadID string `json:"threadId"`
	Kind     string `json:"kind"`
	Topic    string `json:"topic,omitempty"`
	To       string `json:"to,omitempty"`
}

func lastMsgPath(p paths) string {
	sid := sessionID()
	if sid == "" {
		return ""
	}
	return filepath.Join(p.dir, "lastmsg", filepath.Base(sid))
}

func readLastMsg(p paths) (lastMsgInfo, bool) {
	var info lastMsgInfo
	path := lastMsgPath(p)
	if path == "" {
		return info, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return info, false
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return info, false
	}
	return info, true
}

// writeLastMsg records the most recently seen message, best-effort — a
// session with no id (e.g. run outside a supported host agent) just can't use
// the implicit `mess reply` default, so silently skip rather than error on
// every `mess recv`.
func writeLastMsg(p paths, info lastMsgInfo) {
	path := lastMsgPath(p)
	if path == "" {
		return
	}
	data, err := json.Marshal(info)
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return
	}
	_ = os.WriteFile(path, data, 0o600)
}

func openThreadPath(p paths) string {
	sid := sessionID()
	if sid == "" {
		return ""
	}
	return filepath.Join(p.dir, "openthread", filepath.Base(sid))
}

func readOpenThread(p paths) (openThreadInfo, bool) {
	var info openThreadInfo
	path := openThreadPath(p)
	if path == "" {
		return info, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return info, false
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return info, false
	}
	return info, true
}

func writeOpenThread(p paths, info openThreadInfo) error {
	path := openThreadPath(p)
	if path == "" {
		return fmt.Errorf("no session id (CLAUDE_CODE_SESSION_ID / CODEX_THREAD_ID / MESS_SESSION_ID); cannot track an open thread")
	}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// clearOpenThread removes the tracked open thread (mess thread close). An
// absent file is not an error.
func clearOpenThread(p paths) error {
	path := openThreadPath(p)
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
