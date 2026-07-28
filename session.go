package main

import (
	"os"
	"strconv"
	"strings"
)

// Liveness of a host session, by probe rather than by heuristic.
//
// Presence used to be inferred from three proxies — a listener count, a busy
// flag, and a last-seen timestamp — none of which can tell "this session is
// working" from "this session died without cleaning up". A parked wake hook
// that outlives its session keeps its listener; `mess busy` carries a one-hour
// crash backstop. Either one makes a dead session read online (and `working`)
// with no process behind it, which in turn makes the ownership guard reject the
// *replacement* session under the same name.
//
// So: record which process the identity actually belongs to, and ask the OS.

// hostSessionComms are the process names a host agent runs as. Identity belongs
// to that long-lived process, not to the shell mess is invoked from — every
// tool call gets a fresh shell, so a shell pid would expire constantly.
var hostSessionComms = map[string]bool{"claude": true, "node": true, "grok": true}

// maxAncestorWalk bounds the climb, so a pathological or looping ancestry can
// never hang a command.
const maxAncestorWalk = 32

// hostSessionPID walks up from this process to the nearest host-agent ancestor
// and returns its pid, or 0 if there is none — an unrecognized harness, or no
// procfs. Callers must read 0 as "can't tell", never as "dead".
func hostSessionPID() int {
	pid := os.Getppid()
	for range maxAncestorWalk {
		if pid <= 1 {
			return 0
		}
		comm := processComm(pid)
		if comm == "" {
			return 0
		}
		if hostSessionComms[comm] {
			return pid
		}
		pid = parentPID(pid)
	}
	return 0
}

// processComm reads a pid's executable name, or "" if it isn't running.
func processComm(pid int) string {
	if pid <= 0 {
		return ""
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/comm")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// parentPID reads a pid's parent, or 0 if it can't be determined.
func parentPID(pid int) int {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		if after, ok := strings.CutPrefix(line, "PPid:"); ok {
			p, err := strconv.Atoi(strings.TrimSpace(after))
			if err != nil {
				return 0
			}
			return p
		}
	}
	return 0
}
