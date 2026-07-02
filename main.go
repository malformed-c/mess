package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const usage = `mess - a local messenger for Claude agents

Usage:
  mess send <to> [body...]        send a direct message to an agent
                                  (--ack blocks until it's read; --timeout DUR)
  mess broadcast [body...]        send to every known agent
  mess pub <topic> [body...]      publish to a topic (@mention wakes only the
                                  tagged subscribers; the rest still receive it)
  mess sub <topic>                subscribe to a topic
  mess unsub <topic>              unsubscribe from a topic
  mess register [name]            join the network; with a name, set this
                                  session's identity (persists across turns)
  mess unregister                 leave the network and clear this session's
                                  identity (inverse of register)
  mess rename [--force] <name>    rename yourself, migrating your inbox and
                                  subscriptions to the new name
  mess cleanup [maxage]           prune agents idle longer than maxage (default
                                  24h) and not listening; --dry-run to preview
  mess state [text...]            set your working state (shown in ps); --clear to clear
  mess warn [text...]             set a transient status warning (auto-clears when
                                  you're next active; --ttl DUR, --clear)
  mess busy / mess unbusy         mark/clear "in a turn" (drives ps working status; for hooks)
  mess rm <agent>                 remove an agent (e.g. a dead session) from the network
  mess drain <agent>              clear another agent's inbox (prints what was queued;
                                  leaves the agent registered — for a stuck backlog)
  mess whoami                     print your resolved identity (empty if none)
  mess islistening                exit 0 if you have an active listener, else 1
  mess recv [duration]            receive queued messages
  mess replay [N]                 reprint the last N messages you already consumed
                                  (recover a message lost to a dropped wake)
  mess listen [idle-timeout]      run continuously (bg): print messages as they
                                  arrive until interrupted (alias: recv --follow)
  mess ps                         list agents and topics (online/offline +
                                  working/listening/idle status)
  mess ping                       check the daemon
  mess daemon                     run the daemon in the foreground
  mess stop                       shut the daemon down

Identity (resolved in this order):
  1. --as NAME on the command
  2. a mid-session name set via "mess register <name>" — persisted per host
     session (keyed on the first of $MESS_SESSION_ID, $CLAUDE_CODE_SESSION_ID,
     or $CODEX_THREAD_ID), so it survives across turns, compaction, and resume
  3. the MESS_AGENT environment variable (set at launch)

If no body args are given, the body is read from stdin.

recv flags:
  --wait            block until a message arrives (also implied by a
                    trailing duration, e.g. "mess recv 30s")
  --follow          keep receiving until interrupted (see "mess listen")
  --peek            return messages without consuming them
  --max N           return at most N messages
  --kind LIST       only these kinds (comma-list: direct,broadcast,topic)
  --no-broadcast    ignore broadcasts (= --kind direct,topic); useful for a
                    parked waiter that should wake only on actionable messages
  --batch DUR       with --wait: coalesce a burst arriving within DUR into one
                    wake (fewer back-to-back wakes for rapid messages)
  --json            print messages as JSON lines

Common flags:
  --as NAME         identity of the calling agent
  --json            machine-readable output (recv, ps)
`

func main() {
	log.SetFlags(log.LstdFlags)
	log.SetPrefix("mess: ")
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	p := resolvePaths()
	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "daemon":
		err = runDaemon(p)
	case "send":
		err = cmdSend(p, args)
	case "broadcast":
		err = cmdBroadcast(p, args)
	case "pub":
		err = cmdPub(p, args)
	case "sub", "unsub":
		err = cmdSubUnsub(p, cmd, args)
	case "register":
		err = cmdRegister(p, args)
	case "unregister":
		err = cmdUnregister(p, args)
	case "rename":
		err = cmdRename(p, args)
	case "cleanup":
		err = cmdCleanup(p, args)
	case "state":
		err = cmdState(p, args)
	case "warn":
		err = cmdWarn(p, args)
	case "busy", "unbusy":
		err = cmdBusy(p, cmd, args)
	case "rm":
		err = cmdRm(p, args)
	case "drain":
		err = cmdDrain(p, args)
	case "whoami":
		err = cmdWhoami(p)
	case "islistening":
		err = cmdIsListening(p, args)
	case "recv":
		err = cmdRecv(p, args)
	case "replay":
		err = cmdReplay(p, args)
	case "listen":
		// listen == recv --follow: a continuous background listener.
		err = cmdRecv(p, append([]string{"--follow"}, args...))
	case "ps":
		err = cmdPs(p, args)
	case "ping":
		err = cmdPing(p)
	case "stop":
		err = cmdStop(p)
	case "-h", "--help", "help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "mess: unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

// agentName resolves identity: --as flag, then a mid-session registered name,
// then MESS_AGENT. See identity.go for the rationale.
func agentName(p paths, flagVal string) (string, error) {
	if flagVal != "" {
		return flagVal, nil
	}
	if id := readIdentity(p); id != "" {
		return id, nil
	}
	if env := os.Getenv("MESS_AGENT"); env != "" {
		return env, nil
	}
	return "", fmt.Errorf("no identity: run `mess register <name>`, pass --as NAME, or set MESS_AGENT")
}

// bodyFrom joins remaining args as the body, or reads stdin when none given.
func bodyFrom(args []string) (string, error) {
	if len(args) > 0 {
		return strings.Join(args, " "), nil
	}
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(data), "\n"), nil
}

// newFlags builds a flagset with the shared --as flag registered.
func newFlags(name string) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	as := fs.String("as", "", "identity of the calling agent")
	return fs, as
}

// parseAnywhere parses flags regardless of their position relative to positional
// args. Go's flag package stops at the first positional, so "mess send bob hi
// --as alice" would otherwise swallow "--as alice" into the body. Only flags the
// command actually defines are hoisted; unknown dash-tokens (real message text
// like "-1") are left untouched in the positionals.
func parseAnywhere(fs *flag.FlagSet, args []string) error {
	valueFlags, boolFlags := map[string]bool{}, map[string]bool{}
	fs.VisitAll(func(f *flag.Flag) {
		if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
			boolFlags[f.Name] = true
		} else {
			valueFlags[f.Name] = true
		}
	})
	return fs.Parse(hoistFlags(args, valueFlags, boolFlags))
}

// hoistFlags returns args reordered as: recognized flags, then "--", then
// positionals. The "--" terminator means a dash-leading body token is treated as
// text, not a flag.
func hoistFlags(args []string, valueFlags, boolFlags map[string]bool) []string {
	var flags, pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			pos = append(pos, args[i+1:]...)
			break
		}
		if name, ok := flagToken(a); ok && (valueFlags[name] || boolFlags[name]) {
			flags = append(flags, a)
			if valueFlags[name] && !strings.Contains(a, "=") && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
			continue
		}
		pos = append(pos, a)
	}
	return append(append(flags, "--"), pos...)
}

// flagToken extracts the flag name from a "-x"/"--name"/"--name=v" token.
func flagToken(a string) (string, bool) {
	if len(a) < 2 || a[0] != '-' {
		return "", false
	}
	s := strings.TrimLeft(a, "-")
	if s == "" {
		return "", false
	}
	if i := strings.IndexByte(s, '='); i >= 0 {
		s = s[:i]
	}
	return s, true
}

func cmdSend(p paths, args []string) error {
	fs, as := newFlags("send")
	ack := fs.Bool("ack", false, "block until the recipient reads the message")
	timeout := fs.String("timeout", "", "ack wait timeout (e.g. 30s); default: wait forever")
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: mess send [--ack [--timeout DUR]] <to> [body...]")
	}
	from, err := agentName(p, *as)
	if err != nil {
		return err
	}
	to := rest[0]
	body, err := bodyFrom(rest[1:])
	if err != nil {
		return err
	}
	resp, err := call(p, Request{Op: "send", As: from, To: to, Body: body, Ack: *ack, Timeout: *timeout})
	if err != nil {
		return err
	}
	if *ack {
		if !resp.Acked {
			return fmt.Errorf("not read by %s (ack timeout)", to)
		}
		fmt.Printf("read by %s\n", to)
	}
	return nil
}

func cmdBroadcast(p paths, args []string) error {
	fs, as := newFlags("broadcast")
	parseAnywhere(fs, args)
	from, err := agentName(p, *as)
	if err != nil {
		return err
	}
	body, err := bodyFrom(fs.Args())
	if err != nil {
		return err
	}
	resp, err := call(p, Request{Op: "broadcast", As: from, Body: body})
	if err != nil {
		return err
	}
	fmt.Printf("delivered to %d agent(s)\n", resp.Count)
	return nil
}

func cmdPub(p paths, args []string) error {
	fs, as := newFlags("pub")
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: mess pub <topic> [body...]")
	}
	from, err := agentName(p, *as)
	if err != nil {
		return err
	}
	body, err := bodyFrom(rest[1:])
	if err != nil {
		return err
	}
	resp, err := call(p, Request{Op: "pub", As: from, Topic: rest[0], Body: body})
	if err != nil {
		return err
	}
	fmt.Printf("delivered to %d subscriber(s)\n", resp.Count)
	return nil
}

func cmdSubUnsub(p paths, op string, args []string) error {
	fs, as := newFlags(op)
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: mess %s <topic>", op)
	}
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	_, err = call(p, Request{Op: op, As: name, Topic: rest[0]})
	return err
}

func cmdRegister(p paths, args []string) error {
	fs, as := newFlags("register")
	force := fs.Bool("force", false, "take over a name already held by another live session")
	parseAnywhere(fs, args)
	// `mess register <name>` sets and persists this session's identity. Defer the
	// write until the daemon accepts the name, so a rejected collision doesn't
	// leave a stale identity file behind.
	newName := ""
	if rest := fs.Args(); len(rest) > 0 {
		newName = rest[0]
	}
	name := newName
	if name == "" {
		var err error
		if name, err = agentName(p, *as); err != nil {
			return err
		}
	}
	if _, err := call(p, Request{Op: "register", As: name, Force: *force}); err != nil {
		return err // includes the collision message + the --force hint
	}
	if newName != "" {
		if err := writeIdentity(p, newName); err != nil {
			return err
		}
	}
	fmt.Printf("registered as %s\n", name)
	return nil
}

// cmdUnregister removes the calling agent from the network and clears this
// session's persisted identity — the inverse of register.
func cmdUnregister(p paths, args []string) error {
	fs, as := newFlags("unregister")
	parseAnywhere(fs, args)
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	if _, err := call(p, Request{Op: "unregister", As: name}); err != nil {
		return err
	}
	// Only clear the persisted identity when unregistering our *own* session
	// identity — not when --as targets some other agent.
	if name == readIdentity(p) {
		if err := clearIdentity(p); err != nil {
			return err
		}
	}
	fmt.Printf("unregistered %s\n", name)
	if env := os.Getenv("MESS_AGENT"); env != "" {
		fmt.Printf("note: MESS_AGENT=%s is still set; this session will re-register as %q on its next action\n", env, env)
	}
	return nil
}

// cmdRename renames the calling agent, migrating its inbox and subscriptions to
// the new name and repointing this session's persisted identity.
func cmdRename(p paths, args []string) error {
	fs, as := newFlags("rename")
	force := fs.Bool("force", false, "take over the new name if held by another live session")
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: mess rename [--force] <new-name>")
	}
	newName := rest[0]
	old, err := agentName(p, *as)
	if err != nil {
		return err
	}
	if _, err := call(p, Request{Op: "rename", As: old, To: newName, Force: *force}); err != nil {
		return err // includes the collision message + the --force hint
	}
	if err := writeIdentity(p, newName); err != nil {
		return err
	}
	fmt.Printf("renamed %s -> %s\n", old, newName)
	return nil
}

// cmdCleanup prunes agents idle longer than maxage (default 24h) that aren't
// currently listening. With --dry-run it only reports what would be removed.
func cmdCleanup(p paths, args []string) error {
	fs := flag.NewFlagSet("cleanup", flag.ExitOnError)
	dryRun := fs.Bool("dry-run", false, "list what would be removed without removing")
	parseAnywhere(fs, args)
	maxAge := ""
	if rest := fs.Args(); len(rest) > 0 {
		if _, derr := time.ParseDuration(rest[0]); derr != nil {
			return fmt.Errorf("invalid duration %q", rest[0])
		}
		maxAge = rest[0]
	}
	resp, err := call(p, Request{Op: "cleanup", Timeout: maxAge, Peek: *dryRun})
	if err != nil {
		return err
	}
	if len(resp.Removed) == 0 {
		fmt.Println("nothing to clean up")
		return nil
	}
	verb := "removed"
	if *dryRun {
		verb = "would remove"
	}
	fmt.Printf("%s %d agent(s): %s\n", verb, len(resp.Removed), strings.Join(resp.Removed, ", "))
	return nil
}

// cmdState sets the calling agent's working state (what it's currently doing),
// shown in `mess ps`. With --clear (or empty body) it clears the state.
func cmdState(p paths, args []string) error {
	fs, as := newFlags("state")
	clear := fs.Bool("clear", false, "clear your working state")
	parseAnywhere(fs, args)
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	state := ""
	if !*clear {
		if state, err = bodyFrom(fs.Args()); err != nil {
			return err
		}
	}
	if _, err := call(p, Request{Op: "state", As: name, Body: state}); err != nil {
		return err
	}
	if state == "" {
		fmt.Println("state cleared")
	} else {
		fmt.Printf("state: %s\n", state)
	}
	return nil
}

// cmdWarn sets a transient status warning (shown in ps) that auto-clears when
// the agent is next active and self-expires after --ttl (default 15m). Used by
// the StopFailure hook to flag an API error without it lingering forever.
func cmdWarn(p paths, args []string) error {
	fs, as := newFlags("warn")
	ttl := fs.String("ttl", "", "auto-clear after this long (default 15m)")
	clear := fs.Bool("clear", false, "clear your warning")
	parseAnywhere(fs, args)
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	text := ""
	if !*clear {
		if text, err = bodyFrom(fs.Args()); err != nil {
			return err
		}
	}
	if _, err := call(p, Request{Op: "warn", As: name, Body: text, Timeout: *ttl}); err != nil {
		return err
	}
	if text == "" {
		fmt.Println("warning cleared")
	} else {
		fmt.Printf("⚠ %s\n", text)
	}
	return nil
}

// cmdBusy marks (busy) or clears (unbusy) the calling agent's in-a-turn flag,
// which drives the "working" status in ps. Driven by lifecycle hooks.
func cmdBusy(p paths, op string, args []string) error {
	fs, as := newFlags(op)
	ttl := fs.String("ttl", "", "busy auto-clears after this long (default 1h backstop)")
	parseAnywhere(fs, args)
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	req := Request{Op: op, As: name}
	if op == "busy" {
		req.Timeout = *ttl
	}
	_, err = call(p, req)
	return err
}

// cmdDrain consumes (clears) another agent's inbox and prints what was there —
// an operator tool to clear a stuck/dead agent's backlog. Unlike `rm` it leaves
// the agent registered.
func cmdDrain(p paths, args []string) error {
	fs := flag.NewFlagSet("drain", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "print messages as JSON lines")
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: mess drain <agent>")
	}
	target := rest[0]
	resp, err := call(p, Request{Op: "drain", As: target}) // clear target's inbox (no touch, no ack)
	if err != nil {
		return err
	}
	printMessages(resp.Messages, *asJSON)
	if !*asJSON {
		fmt.Printf("drained %d message(s) from %s\n", resp.Count, target)
	}
	return nil
}

// cmdReplay reprints the last messages this agent already consumed (from a
// bounded history) — a recovery path if a consume-on-wake injection was dropped.
// `mess replay` shows the whole history; `mess replay N` the last N.
func cmdReplay(p paths, args []string) error {
	fs, as := newFlags("replay")
	asJSON := fs.Bool("json", false, "print messages as JSON lines")
	parseAnywhere(fs, args)
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	n := 0
	if rest := fs.Args(); len(rest) > 0 {
		if n, err = strconv.Atoi(rest[0]); err != nil {
			return fmt.Errorf("invalid count %q", rest[0])
		}
	}
	resp, err := call(p, Request{Op: "replay", As: name, Max: n})
	if err != nil {
		return err
	}
	printMessages(resp.Messages, *asJSON)
	return nil
}

// cmdRm removes an agent from the network (its inbox, subscriptions, presence).
func cmdRm(p paths, args []string) error {
	fs, _ := newFlags("rm")
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: mess rm <agent>")
	}
	resp, err := call(p, Request{Op: "rm", To: rest[0]})
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		fmt.Printf("no such agent %q\n", rest[0])
	} else {
		fmt.Printf("removed %s\n", rest[0])
	}
	return nil
}

// cmdWhoami prints the resolved identity, or nothing (exit 0) if none. Designed
// so hooks can do: who=$(mess whoami) && [ -n "$who" ] && ...
func cmdWhoami(p paths) error {
	if name, _ := agentName(p, ""); name != "" {
		fmt.Println(name)
	}
	return nil
}

// cmdIsListening exits 0 if the resolved agent has an active listener, else 1.
// Lets the Stop hook skip the idle broadcast while a `mess listen` is running:
//
//	! mess islistening && mess broadcast "$who idle"
func cmdIsListening(p paths, args []string) error {
	fs, as := newFlags("islistening")
	parseAnywhere(fs, args)
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	resp, err := call(p, Request{Op: "ps"})
	if err != nil {
		return err
	}
	for _, a := range resp.Agents {
		if a.Name == name && a.Listening {
			return nil // exit 0: listening
		}
	}
	os.Exit(1) // not listening (silent, no error message)
	return nil
}

func cmdRecv(p paths, args []string) error {
	fs, as := newFlags("recv")
	wait := fs.Bool("wait", false, "block until a message arrives")
	follow := fs.Bool("follow", false, "keep receiving and printing messages until interrupted (for background use)")
	peek := fs.Bool("peek", false, "do not consume messages")
	asJSON := fs.Bool("json", false, "print messages as JSON lines")
	max := fs.Int("max", 0, "return at most N messages")
	kind := fs.String("kind", "", "only these kinds (comma-list: direct,broadcast,topic)")
	noBroadcast := fs.Bool("no-broadcast", false, "ignore broadcasts (= --kind direct,topic)")
	batch := fs.String("batch", "", "with --wait: coalesce a burst arriving within this window into one return")
	parseAnywhere(fs, args)
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	kinds, err := resolveKinds(*kind, *noBroadcast)
	if err != nil {
		return err
	}

	// A trailing duration is the wait/idle timeout (e.g. "mess recv 30s",
	// "mess listen 5m"). For a one-shot recv it also implies --wait.
	timeout := ""
	if rest := fs.Args(); len(rest) > 0 {
		if d, derr := time.ParseDuration(rest[0]); derr == nil {
			timeout = d.String()
			*wait = true
		} else {
			return fmt.Errorf("invalid duration %q", rest[0])
		}
	}

	// A blocking receiver that joins an agent already being listened on creates
	// a race: a message wakes only one waiter. Warn (stderr, so --json stdout is
	// clean) but proceed. The canonical single receiver is the auto-wake hook.
	if *wait || *follow {
		warnIfAlreadyListening(p, name)
	}

	if *follow {
		return followRecv(p, name, timeout, *max, *asJSON, kinds, *batch)
	}

	req := Request{Op: "recv", As: name, Wait: *wait, Timeout: timeout, Peek: *peek, Max: *max, Kinds: kinds, Batch: *batch}
	// A blocking wait uses the restart-resilient path so it survives a daemon
	// bounce; a non-blocking drain is a plain one-shot call.
	dispatch := call
	if *wait {
		dispatch = callWait
	}
	resp, err := dispatch(p, req)
	if err != nil {
		return err
	}
	printMessages(resp.Messages, *asJSON)
	return nil
}

// resolveKinds turns --kind/--no-broadcast into an explicit kinds list, or nil
// when all kinds are allowed (no filter).
func resolveKinds(kind string, noBroadcast bool) ([]string, error) {
	all := []string{KindDirect, KindBroadcast, KindTopic}
	set := map[string]bool{}
	if kind == "" {
		for _, k := range all {
			set[k] = true
		}
	} else {
		for k := range strings.SplitSeq(kind, ",") {
			k = strings.TrimSpace(k)
			if k == "" {
				continue
			}
			if k != KindDirect && k != KindBroadcast && k != KindTopic {
				return nil, fmt.Errorf("unknown kind %q (want direct, broadcast, or topic)", k)
			}
			set[k] = true
		}
	}
	if noBroadcast {
		delete(set, KindBroadcast)
	}
	if len(set) == len(all) { // all kinds allowed: no filter
		return nil, nil
	}
	var out []string
	for _, k := range all {
		if set[k] {
			out = append(out, k)
		}
	}
	return out, nil
}

// warnIfAlreadyListening prints a stderr warning when the agent already has an
// active listener, so a redundant second waiter doesn't silently create a
// wake race. Best-effort: stays quiet if the daemon can't be queried.
func warnIfAlreadyListening(p paths, name string) {
	resp, err := call(p, Request{Op: "ps"})
	if err != nil {
		return
	}
	for _, a := range resp.Agents {
		if a.Name == name && a.Listening {
			fmt.Fprintf(os.Stderr, "mess: warning: %q already has an active listener; a second one makes each message wake only one of them. Prefer a single receiver (the auto-wake Stop hook).\n", name)
			return
		}
	}
}

// followRecv blocks, printing messages as they arrive, until interrupted (or,
// if an idle timeout was given, until that elapses with no message). Designed to
// run as a long-lived background command so an agent can be woken by peers.
func followRecv(p paths, name, timeout string, max int, asJSON bool, kinds []string, batch string) error {
	return callStream(p, Request{Op: "listen", As: name, Timeout: timeout, Max: max, Kinds: kinds, Batch: batch}, func(resp Response) error {
		printMessages(resp.Messages, asJSON)
		return nil
	})
}

// compactDur renders a duration as a short "45s" / "3m" / "2h" age.
func compactDur(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
}

func cmdPs(p paths, args []string) error {
	fs := flag.NewFlagSet("ps", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "machine-readable output")
	parseAnywhere(fs, args)
	resp, err := call(p, Request{Op: "ps"})
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(resp)
	}
	if len(resp.Agents) == 0 {
		fmt.Println("no agents")
	} else {
		fmt.Println("agents:")
		for _, a := range resp.Agents {
			// "working" = actively in a turn (busy, set by lifecycle hooks);
			// "listening" = idle but parked on recv --wait, reachable now;
			// "idle" = neither (between turns / not parked).
			status := "idle"
			switch {
			case a.Working:
				status = "working"
			case a.Listening:
				status = "listening"
			}
			// "online" = the session looks alive (listening/working/recently active);
			// "offline" = idle with no sign of life (likely a dead/stale session).
			presence := "offline"
			if a.Online {
				presence = "online"
			}
			line := fmt.Sprintf("  %-16s %-7s %-9s %d pending", a.Name, presence, status, a.Pending)
			if a.Pending > 0 && !a.Oldest.IsZero() {
				line += fmt.Sprintf(" (oldest %s)", compactDur(time.Since(a.Oldest)))
			}
			if len(a.Topics) > 0 {
				line += "  [" + strings.Join(a.Topics, ", ") + "]"
			}
			if a.State != "" {
				line += "  — " + a.State
			}
			if a.Warning != "" {
				line += "  ⚠ " + a.Warning
			}
			fmt.Println(line)
		}
	}
	if len(resp.Topics) > 0 {
		fmt.Println("topics:")
		for _, t := range resp.Topics {
			fmt.Printf("  #%-15s %s\n", t.Name, strings.Join(t.Subscribers, ", "))
		}
	}
	return nil
}

func cmdPing(p paths) error {
	if _, err := call(p, Request{Op: "ping"}); err != nil {
		return err
	}
	fmt.Println("ok")
	return nil
}

func cmdStop(p paths) error {
	if _, err := call(p, Request{Op: "stop"}); err != nil {
		return err
	}
	fmt.Println("stopped")
	return nil
}

func printMessages(msgs []Message, asJSON bool) {
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		for _, m := range msgs {
			_ = enc.Encode(m)
		}
		return
	}
	for _, m := range msgs {
		ts := m.Time.Format("15:04:05")
		switch m.Kind {
		case KindTopic:
			fmt.Printf("%s %s #%s: %s\n", ts, m.From, m.Topic, m.Body)
		case KindBroadcast:
			fmt.Printf("%s %s (broadcast): %s\n", ts, m.From, m.Body)
		default:
			fmt.Printf("%s %s: %s\n", ts, m.From, m.Body)
		}
	}
}
