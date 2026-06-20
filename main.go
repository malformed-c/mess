package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

const usage = `mess - a local messenger for Claude agents

Usage:
  mess send <to> [body...]        send a direct message to an agent
                                  (--ack blocks until it's read; --timeout DUR)
  mess broadcast [body...]        send to every known agent
  mess pub <topic> [body...]      publish to a topic
  mess sub <topic>                subscribe to a topic
  mess unsub <topic>              unsubscribe from a topic
  mess register [name]            join the network; with a name, set this
                                  session's identity (persists across turns)
  mess state [text...]            set your working state (shown in ps); --clear to clear
  mess rm <agent>                 remove an agent (e.g. a dead session) from the network
  mess whoami                     print your resolved identity (empty if none)
  mess islistening                exit 0 if you have an active listener, else 1
  mess recv [duration]            receive queued messages
  mess listen [idle-timeout]      run continuously (bg): print messages as they
                                  arrive until interrupted (alias: recv --follow)
  mess ps                         list agents and topics
  mess ping                       check the daemon
  mess daemon                     run the daemon in the foreground
  mess stop                       shut the daemon down

Identity (resolved in this order):
  1. --as NAME on the command
  2. a mid-session name set via "mess register <name>" (kept per Claude Code
     session, so it survives across turns)
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
	case "state":
		err = cmdState(p, args)
	case "rm":
		err = cmdRm(p, args)
	case "whoami":
		err = cmdWhoami(p)
	case "islistening":
		err = cmdIsListening(p, args)
	case "recv":
		err = cmdRecv(p, args)
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
	parseAnywhere(fs, args)
	// `mess register <name>` sets and persists this session's identity.
	if rest := fs.Args(); len(rest) > 0 {
		if err := writeIdentity(p, rest[0]); err != nil {
			return err
		}
	}
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	if _, err := call(p, Request{Op: "register", As: name}); err != nil {
		return err
	}
	fmt.Printf("registered as %s\n", name)
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
		return followRecv(p, name, timeout, *max, *asJSON, kinds)
	}

	req := Request{Op: "recv", As: name, Wait: *wait, Timeout: timeout, Peek: *peek, Max: *max, Kinds: kinds}
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
func followRecv(p paths, name, timeout string, max int, asJSON bool, kinds []string) error {
	return callStream(p, Request{Op: "listen", As: name, Timeout: timeout, Max: max, Kinds: kinds}, func(resp Response) error {
		printMessages(resp.Messages, asJSON)
		return nil
	})
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
			// "listening" = parked on recv --wait, reachable now (a peer message
			// wakes it). "working" = no parked waiter, i.e. busy in a turn (the
			// wake consumed its waiter); it re-arms to listening on its next idle.
			status := "working"
			if a.Listening {
				status = "listening"
			}
			line := fmt.Sprintf("  %-16s %-9s %d pending", a.Name, status, a.Pending)
			if len(a.Topics) > 0 {
				line += "  [" + strings.Join(a.Topics, ", ") + "]"
			}
			if a.State != "" {
				line += "  — " + a.State
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
