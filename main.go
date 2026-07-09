package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const usage = `mess - a local messenger for Claude agents

Usage:

Identity:
  mess register [name]            join the network; with a name, set this
                                  session's identity (persists across turns)
  mess unregister                 leave the network and clear this session's
                                  identity (inverse of register)
  mess rename [--force] <name>    rename yourself, migrating your inbox and
                                  subscriptions to the new name
  mess whoami                     print your resolved identity (empty if none)
  mess islistening                exit 0 if you have an active listener, else 1

Rooms (namespace isolation):
  mess room [join <room> | leave] print your current room, or join/leave one —
                                  an exclusive namespace isolating identities,
                                  broadcast, ps, and topics from other rooms
                                  (global/default room until you join one)
  mess room bridge <topic> <room>/<topic> [--direction both|out|in] [--ttl DUR]
                                  relay a local topic to a topic in another room
  mess room unbridge <id>          tear down a bridge
  mess room bridges                list active bridges

Sending:
  mess send <to> [body...]        send a direct message to an agent
                                  (--ack blocks until it's read; --timeout DUR)
                                  (to "user" or your login name = the human's
                                  mailbox: desktop-notifies, read via recv --as user)
                                  (--thread ID replies within a thread)
                                  (--attach PATH records a file's path + hash)
  mess broadcast [body...]        send to every known agent in your room (plain
                                  broadcasts don't wake the standard --no-broadcast
                                  auto-wake hook; --loud bypasses that, goes
                                  host-wide across rooms, and desktop-notifies the
                                  human operator; --loud-room does the same but
                                  stays scoped to your own room)
  mess pub <topic> [body...]      publish to a topic (@mention wakes only the
                                  tagged subscribers; the rest still receive it)
                                  (--thread ID replies within a thread — quiet
                                  unless @mentioned or already a participant)
  mess sub <topic>                subscribe to a topic
  mess unsub <topic>              unsubscribe from a topic
  mess ask <agent> [q...]         send a question, wait for the reply (a plain
                                  mess reply answers it); --async prints a
                                  token immediately instead of waiting
  mess await <token>              wait for an ask's reply later (the token
                                  mess ask --async / a timed-out mess ask printed)

Receiving and threads:
  mess recv [duration]            receive queued messages (--thread ID shows
                                  only that thread's messages, not with --wait)
  mess listen [idle-timeout]      run continuously (bg): print messages as they
                                  arrive until interrupted (alias: recv --follow)
  mess replay [N]                 reprint the last N messages you already consumed
                                  (recover a message lost to a dropped wake)
  mess reply [body...]            reply to the most recent message (or continue
                                  the open thread — see "mess thread close");
                                  no need to know/pass a message id
  mess thread close                end the thread "mess reply" is continuing;
                                  the next reply starts a fresh one
  mess thread list                 list threads you've seen activity in
  mess export --topic NAME | --thread ID | --to AGENT
                                  dump a conversation's full history
                                  (--format text|json, --out FILE, --max N)
  mess log [--from AGENT] [--grep PATTERN] [--since DUR] [--topic NAME] [--all]
                                  search the durable, unbounded journal (unlike
                                  recv/replay/export, which only see a bounded
                                  recent window); same --format/--out/--max

Status:
  mess ps [--room NAME | --all]    list agents and topics (online/offline +
                                  working/listening/idle status); scoped to
                                  your own room by default
  mess state [text...]            set your working state (shown in ps); --clear to clear
  mess warn [text...]             set a transient status warning (auto-clears when
                                  you're next active; --ttl DUR, --clear)
  mess busy / mess unbusy         mark/clear "in a turn" (drives ps working status; for hooks)

Admin and daemon:
  mess rm <agent>                 remove an agent (e.g. a dead session) from the network
  mess drain <agent>              clear another agent's inbox (prints what was queued;
                                  leaves the agent registered — for a stuck backlog)
  mess cleanup [maxage]           prune agents idle longer than maxage (default
                                  24h) and not listening; --dry-run to preview
  mess expire [maxage]            drop unread messages older than maxage
                                  (default 14d), any agent, alive or not;
                                  --dry-run to preview (see MESS_AUTO_EXPIRE)
  mess ping                       check the daemon
  mess daemon                     run the daemon in the foreground
  mess stop                       shut the daemon down

Identity resolution (most to least specific):
  1. --as NAME on the command
  2. a mid-session name set via "mess register <name>" — persisted per host
     session (keyed on the first of $MESS_SESSION_ID, $CLAUDE_CODE_SESSION_ID,
     or $CODEX_THREAD_ID), so it survives across turns, compaction, and resume
  3. the MESS_AGENT environment variable (set at launch)

Room resolution (same order, independently of identity):
  1. --room NAME on the command
  2. a mid-session room set via "mess room join <room>" — persisted the same
     way as identity
  3. the MESS_ROOM environment variable

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
  --if-idle         drain only if not currently busy, checked atomically with
                    the drain (not combined with --wait/--follow)
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
	// Identity
	case "register":
		err = cmdRegister(p, args)
	case "unregister":
		err = cmdUnregister(p, args)
	case "rename":
		err = cmdRename(p, args)
	case "whoami":
		err = cmdWhoami(p)
	case "islistening":
		err = cmdIsListening(p, args)

	// Rooms
	case "room":
		err = cmdRoom(p, args)

	// Sending
	case "send":
		err = cmdSend(p, args)
	case "ask":
		err = cmdAsk(p, args)
	case "await":
		err = cmdAwait(p, args)
	case "broadcast":
		err = cmdBroadcast(p, args)
	case "pub":
		err = cmdPub(p, args)
	case "sub", "unsub":
		err = cmdSubUnsub(p, cmd, args)

	// Receiving and threads
	case "recv":
		err = cmdRecv(p, args)
	case "listen":
		// listen == recv --follow: a continuous background listener.
		err = cmdRecv(p, append([]string{"--follow"}, args...))
	case "replay":
		err = cmdReplay(p, args)
	case "reply":
		err = cmdReply(p, args)
	case "thread":
		err = cmdThread(p, args)
	case "export":
		err = cmdExport(p, args)
	case "log":
		err = cmdLog(p, args)

	// Status
	case "ps":
		err = cmdPs(p, args)
	case "state":
		err = cmdState(p, args)
	case "warn":
		err = cmdWarn(p, args)
	case "busy", "unbusy":
		err = cmdBusy(p, cmd, args)

	// Admin and daemon
	case "rm":
		err = cmdRm(p, args)
	case "drain":
		err = cmdDrain(p, args)
	case "cleanup":
		err = cmdCleanup(p, args)
	case "expire":
		err = cmdExpire(p, args)
	case "ping":
		err = cmdPing(p)
	case "daemon":
		err = runDaemon(p)
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

// resolveRoom resolves the room to act in: --room flag, then a mid-session
// joined room, then MESS_ROOM. Unlike agentName, absence is never an error —
// "" is the meaningful, valid global/default room, not a failure.
func resolveRoom(p paths, flagVal string) string {
	if flagVal != "" {
		return flagVal
	}
	if r := readRoom(p); r != "" {
		return r
	}
	if env := os.Getenv("MESS_ROOM"); env != "" {
		return env
	}
	return ""
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

// setAttach stats and hashes path, filling in req's four attach fields — or a
// hard error if the file is missing/unreadable. Unlike notify's silent-degrade
// philosophy, a wrong/missing attachment reference is a correctness bug (the
// recipient would be pointed at nothing), so this fails before any daemon
// round trip rather than sending a broken reference. Hashing happens
// client-side: this is a single-machine tool, so the daemon has no reason to
// ever assume a different filesystem view than the CLI that sent it.
func setAttach(req *Request, path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("--attach %s: %w", path, err)
	}
	f, err := os.Open(abs)
	if err != nil {
		return fmt.Errorf("--attach %s: %w", path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("--attach %s: %w", path, err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("--attach %s: %w", path, err)
	}
	req.AttachPath = abs
	req.AttachHash = "sha256:" + hex.EncodeToString(h.Sum(nil))
	req.AttachSize = info.Size()
	req.AttachMTime = info.ModTime()
	return nil
}

func cmdSend(p paths, args []string) error {
	fs, as := newFlags("send")
	ack := fs.Bool("ack", false, "block until the recipient reads the message")
	timeout := fs.String("timeout", "", "ack wait timeout (e.g. 30s); default: wait forever")
	thread := fs.String("thread", "", "reply within this thread (the root message's id, shown as [id] in recv output)")
	attach := fs.String("attach", "", "record this file's path + content hash alongside the message")
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: mess send [--ack [--timeout DUR]] [--thread ID] [--attach PATH] <to> [body...]")
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
	req := Request{Op: "send", As: from, To: to, Body: body, Ack: *ack, Timeout: *timeout, ThreadID: *thread}
	if *attach != "" {
		if err := setAttach(&req, *attach); err != nil {
			return err
		}
	}
	resp, err := call(p, req)
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

// cmdAsk sends a direct message tagged as an ask — its own message id becomes
// the correlation token a later `mess await <token>` (or this same call, by
// default) waits on. The replying side answers with a plain `mess reply`,
// which already threads back to the ask's id; nothing on that side changes.
func cmdAsk(p paths, args []string) error {
	fs, as := newFlags("ask")
	timeout := fs.String("timeout", "", "how long to wait for a reply; default: wait forever")
	async := fs.Bool("async", false, "don't wait — print the token immediately for a later `mess await`")
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: mess ask [--timeout DUR] [--async] <agent> [question...]")
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
	req := Request{Op: "ask", As: from, To: to, Body: body, Wait: !*async, Timeout: *timeout}
	dispatch := call
	if req.Wait {
		dispatch = callWait
	}
	resp, err := dispatch(p, req)
	if err != nil {
		return err
	}
	if *async {
		fmt.Printf("asked %s (token %s) — run `mess await %s` for the reply\n", to, resp.ID, resp.ID)
		return nil
	}
	if len(resp.Messages) == 0 {
		fmt.Printf("no reply yet (token %s) — run `mess await %s` to keep waiting\n", resp.ID, resp.ID)
		return nil
	}
	printMessages(resp.Messages, false)
	return nil
}

// cmdAwait blocks for a reply to an outstanding `mess ask`'s token.
func cmdAwait(p paths, args []string) error {
	fs, as := newFlags("await")
	timeout := fs.String("timeout", "", "how long to wait; default: wait forever")
	peek := fs.Bool("peek", false, "do not consume the reply")
	asJSON := fs.Bool("json", false, "print the reply as JSON")
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: mess await [--timeout DUR] [--peek] <token>")
	}
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	resp, err := callWait(p, Request{Op: "await", As: name, ThreadID: rest[0], Wait: true, Peek: *peek, Timeout: *timeout})
	if err != nil {
		return err
	}
	if len(resp.Messages) == 0 {
		fmt.Printf("no reply yet (token %s)\n", rest[0])
		return nil
	}
	printMessages(resp.Messages, *asJSON)
	return nil
}

func cmdBroadcast(p paths, args []string) error {
	fs, as := newFlags("broadcast")
	loud := fs.Bool("loud", false, "wake every recipient host-wide (crosses room boundaries) even if their auto-wake hook filters out broadcasts (--no-broadcast), and desktop-notify the human operator too")
	loudRoom := fs.Bool("loud-room", false, "like --loud, but stays scoped to your own room instead of going host-wide")
	parseAnywhere(fs, args)
	if *loud && *loudRoom {
		return fmt.Errorf("--loud and --loud-room are mutually exclusive")
	}
	from, err := agentName(p, *as)
	if err != nil {
		return err
	}
	body, err := bodyFrom(fs.Args())
	if err != nil {
		return err
	}
	resp, err := call(p, Request{Op: "broadcast", As: from, Body: body, Loud: *loud || *loudRoom, HostWide: *loud})
	if err != nil {
		return err
	}
	fmt.Printf("delivered to %d agent(s)\n", resp.Count)
	return nil
}

func cmdPub(p paths, args []string) error {
	fs, as := newFlags("pub")
	thread := fs.String("thread", "", "reply within this thread (the root message's id, shown as [id] in recv output) — quiet-delivered to everyone except an @mention or existing participant")
	attach := fs.String("attach", "", "record this file's path + content hash alongside the message")
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: mess pub [--thread ID] [--attach PATH] <topic> [body...]")
	}
	from, err := agentName(p, *as)
	if err != nil {
		return err
	}
	body, err := bodyFrom(rest[1:])
	if err != nil {
		return err
	}
	req := Request{Op: "pub", As: from, Topic: rest[0], Body: body, ThreadID: *thread}
	if *attach != "" {
		if err := setAttach(&req, *attach); err != nil {
			return err
		}
	}
	resp, err := call(p, req)
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

// cmdRoom handles the `mess room ...` subcommand family: bare "mess room"
// prints the current room, "join"/"leave" delegate to cmdRoomJoinLeave.
func cmdRoom(p paths, args []string) error {
	if len(args) == 0 {
		if r := resolveRoom(p, ""); r != "" {
			fmt.Println(r)
		} else {
			fmt.Println("(global)")
		}
		return nil
	}
	switch args[0] {
	case "join", "leave":
		return cmdRoomJoinLeave(p, args[0], args[1:])
	case "bridge":
		return cmdRoomBridge(p, args[1:])
	case "unbridge":
		return cmdRoomUnbridge(p, args[1:])
	case "bridges":
		return cmdRoomBridges(p, args[1:])
	default:
		return fmt.Errorf("usage: mess room [join <room> | leave | bridge ... | unbridge <id> | bridges]")
	}
}

// cmdRoomJoinLeave mirrors cmdSubUnsub's shape: "mess room join [--force]
// <room>" claims identity within that room (like register, deferring the
// persisted-room-file write until the daemon accepts it); "mess room leave"
// unregisters from the current room and reverts to global.
func cmdRoomJoinLeave(p paths, op string, args []string) error {
	fs, as := newFlags("room " + op)
	force := fs.Bool("force", false, "take over a name already held in that room by another live session")
	parseAnywhere(fs, args)
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	if op == "join" {
		rest := fs.Args()
		if len(rest) != 1 {
			return fmt.Errorf("usage: mess room join [--force] <room>")
		}
		newRoom := rest[0]
		if _, err := call(p, Request{Op: "room-join", As: name, Room: newRoom, Force: *force}); err != nil {
			return err
		}
		if err := writeRoom(p, newRoom); err != nil {
			return err
		}
		// Also persist the identity itself (like `mess register <name>`), so
		// `room join <room> --as NAME` works as a one-shot "pick a name and join
		// a room" even with no prior `mess register` — otherwise whoami and every
		// future command would have no persisted identity to resolve.
		if err := writeIdentity(p, name); err != nil {
			return err
		}
		fmt.Printf("joined room %q as %s\n", newRoom, name)
		return nil
	}
	// leave: unregister from the current room, then revert to global.
	cur := resolveRoom(p, "")
	if cur == "" {
		fmt.Println("already in the global room")
		return nil
	}
	if _, err := call(p, Request{Op: "room-leave", As: name, Room: cur}); err != nil {
		return err
	}
	if err := clearRoom(p); err != nil {
		return err
	}
	fmt.Printf("left room %q; back in the global room\n", cur)
	return nil
}

// splitRoomTopic parses a "<room>/<topic>" remote address (the room may be
// empty for the global room, e.g. "/announcements").
func splitRoomTopic(s string) (room, topic string, err error) {
	i := strings.LastIndexByte(s, '/')
	if i < 0 {
		return "", "", fmt.Errorf("expected <room>/<topic>, got %q", s)
	}
	return s[:i], strings.TrimPrefix(s[i+1:], "#"), nil
}

// cmdRoomBridge implements `mess room bridge <local-topic> <room>/<remote-topic>`
// — links two topics (possibly in different rooms) so a publish to either
// side also relays to subscribers on the other. The local side defaults to
// the caller's own current room; --local-room names a different one but
// requires --force, since otherwise any caller could bridge on behalf of a
// room it isn't even in.
func cmdRoomBridge(p paths, args []string) error {
	fs, as := newFlags("room bridge")
	direction := fs.String("direction", "both", "both | out | in")
	localRoom := fs.String("local-room", "", "override the local side's room (requires --force if you're not in it)")
	ttl := fs.String("ttl", "", "auto-expire after this long (default: never)")
	force := fs.Bool("force", false, "override the local-room guard, a duplicate bridge, or the bridge cap")
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) != 2 {
		return fmt.Errorf("usage: mess room bridge [--direction both|out|in] [--local-room NAME] [--ttl DUR] [--force] <local-topic> <room>/<remote-topic>")
	}
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	if *direction != "both" && *direction != "out" && *direction != "in" {
		return fmt.Errorf("--direction must be both, out, or in")
	}
	remoteRoom, remoteTopic, err := splitRoomTopic(rest[1])
	if err != nil {
		return err
	}
	resp, err := call(p, Request{
		Op: "bridge", As: name, Topic: rest[0], RemoteRoom: remoteRoom, RemoteTopic: remoteTopic,
		Direction: *direction, LocalRoom: *localRoom, Timeout: *ttl, Force: *force,
	})
	if err != nil {
		return err
	}
	if len(resp.Bridges) == 1 {
		br := resp.Bridges[0]
		fmt.Printf("bridge %s: %s <-%s-> %s\n", br.ID, displayName(br.ARoom, "#"+br.ATopic), br.Direction, displayName(br.BRoom, "#"+br.BTopic))
	}
	return nil
}

// cmdRoomUnbridge tears down a bridge by ID.
func cmdRoomUnbridge(p paths, args []string) error {
	fs, as := newFlags("room unbridge")
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) != 1 {
		return fmt.Errorf("usage: mess room unbridge <id>")
	}
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	resp, err := call(p, Request{Op: "unbridge", As: name, BridgeID: rest[0]})
	if err != nil {
		return err
	}
	if resp.Count == 0 {
		fmt.Printf("no such bridge %q\n", rest[0])
	} else {
		fmt.Printf("removed bridge %s\n", rest[0])
	}
	return nil
}

// cmdRoomBridges lists every active bridge.
func cmdRoomBridges(p paths, args []string) error {
	fs := flag.NewFlagSet("room bridges", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "print bridges as JSON")
	parseAnywhere(fs, args)
	resp, err := call(p, Request{Op: "bridges"})
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(resp.Bridges)
	}
	if len(resp.Bridges) == 0 {
		fmt.Println("no bridges")
		return nil
	}
	for _, br := range resp.Bridges {
		expiry := "never"
		if !br.ExpiresAt.IsZero() {
			expiry = br.ExpiresAt.Format(time.RFC3339)
		}
		fmt.Printf("%-6s %s <-%s-> %s  creator=%s expires=%s\n",
			br.ID, displayName(br.ARoom, "#"+br.ATopic), br.Direction, displayName(br.BRoom, "#"+br.BTopic), br.Creator, expiry)
	}
	return nil
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

// cmdExpire drops unread messages older than maxage from every agent's inbox
// (regardless of whether the agent is currently alive — see Cleanup for
// that), sibling to cmdCleanup. Every drop is durably journaled before being
// committed (see daemon.go's expireDurably) — this is the one command
// capable of deleting real unread mail, so its automatic counterpart
// (MESS_AUTO_EXPIRE) ships opt-in; this manual form is always available.
func cmdExpire(p paths, args []string) error {
	fs := flag.NewFlagSet("expire", flag.ExitOnError)
	dryRun := fs.Bool("dry-run", false, "list what would be dropped without dropping")
	parseAnywhere(fs, args)
	maxAge := ""
	if rest := fs.Args(); len(rest) > 0 {
		if _, derr := time.ParseDuration(rest[0]); derr != nil {
			return fmt.Errorf("invalid duration %q", rest[0])
		}
		maxAge = rest[0]
	}
	resp, err := call(p, Request{Op: "expire", Timeout: maxAge, Peek: *dryRun})
	if err != nil {
		return err
	}
	if resp.Expired == 0 {
		fmt.Println("nothing to expire")
		return nil
	}
	verb := "dropped"
	if *dryRun {
		verb = "would drop"
	}
	fmt.Printf("%s %d unread message(s)\n", verb, resp.Expired)
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
	room := fs.String("room", "", "room the target agent is in (default: your own room)")
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: mess drain <agent>")
	}
	target := rest[0]
	resp, err := call(p, Request{Op: "drain", As: target, Room: *room}) // clear target's inbox (no touch, no ack)
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

// cmdExport dumps a conversation's full history — a topic's own log (--topic,
// independent of who's currently subscribed), a thread's root+replies from
// your own view (--thread), or your direct-message history with a peer
// (--to) — as text or JSON, to stdout or a file.
// cmdReply replies within the currently open thread (see `mess thread
// close`), or starts a new one from the most recently seen message if none is
// open — so you never have to read or type a message id to reply to
// whatever just arrived. Routes to a topic (pub) or a direct peer (send)
// depending on what the root message was.
func cmdReply(p paths, args []string) error {
	fs, as := newFlags("reply")
	parseAnywhere(fs, args)
	body, err := bodyFrom(fs.Args())
	if err != nil {
		return err
	}
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}

	if open, ok := readOpenThread(p); ok {
		return sendReply(p, name, open.Kind, open.Topic, open.To, open.ThreadID, body)
	}

	last, ok := readLastMsg(p)
	if !ok {
		return fmt.Errorf("nothing to reply to yet — run `mess recv` first, or use `mess send`/`mess pub --thread ID` directly")
	}
	if err := writeOpenThread(p, openThreadInfo{ThreadID: last.ID, Kind: last.Kind, Topic: last.Topic, To: last.From}); err != nil {
		return err
	}
	return sendReply(p, name, last.Kind, last.Topic, last.From, last.ID, body)
}

// sendReply posts body as a threaded reply, routing to a topic or a direct
// peer depending on kind.
func sendReply(p paths, from, kind, topic, to, threadID, body string) error {
	switch kind {
	case KindTopic:
		resp, err := call(p, Request{Op: "pub", As: from, Topic: topic, Body: body, ThreadID: threadID})
		if err != nil {
			return err
		}
		fmt.Printf("replied in #%s (thread %s) — delivered to %d subscriber(s)\n", topic, threadID, resp.Count)
	case KindDirect:
		if _, err := call(p, Request{Op: "send", As: from, To: to, Body: body, ThreadID: threadID}); err != nil {
			return err
		}
		fmt.Printf("replied to %s (thread %s)\n", to, threadID)
	default:
		return fmt.Errorf("unsupported reply target kind %q", kind)
	}
	return nil
}

// cmdThread handles `mess thread ...`: "close" ends the thread `mess reply`
// is continuing so the next `mess reply` starts fresh, and "list" shows every
// thread the caller has seen activity in.
func cmdThread(p paths, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: mess thread close | mess thread list")
	}
	switch args[0] {
	case "close":
		if err := clearOpenThread(p); err != nil {
			return err
		}
		fmt.Println("thread closed; next `mess reply` starts a new one")
		return nil
	case "list":
		return cmdThreadList(p, args[1:])
	default:
		return fmt.Errorf("usage: mess thread close | mess thread list")
	}
}

func cmdThreadList(p paths, args []string) error {
	fs, as := newFlags("thread list")
	parseAnywhere(fs, args)
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	resp, err := call(p, Request{Op: "thread-list", As: name})
	if err != nil {
		return err
	}
	if len(resp.Threads) == 0 {
		fmt.Println("no threads seen yet")
		return nil
	}
	open, hasOpen := readOpenThread(p)
	for _, th := range resp.Threads {
		where := "#" + th.Topic
		if th.Kind == KindDirect {
			where = th.Peer
		}
		marker := ""
		if hasOpen && open.ThreadID == th.ID {
			marker = " (open)"
		}
		fmt.Printf("%s  %-12s %d repl(y/ies), %d participant(s), last %s%s\n",
			th.ID, where, th.Replies, th.Participants, th.LastActivity.Format("15:04:05"), marker)
		if th.RootBody != "" {
			fmt.Printf("    %s\n", truncate(th.RootBody, 100))
		}
	}
	return nil
}

func cmdExport(p paths, args []string) error {
	fs, as := newFlags("export")
	topic := fs.String("topic", "", "export this topic's full history")
	thread := fs.String("thread", "", "export this thread (root + replies) from your own received view (your own sent replies won't appear; use --topic for the complete log)")
	to := fs.String("to", "", "export your direct-message history with this peer (received view only, same caveat as --thread)")
	format := fs.String("format", "text", "text | json")
	out := fs.String("out", "", "write to this file instead of stdout")
	max := fs.Int("max", 0, "limit to the most recent N messages (0 = all)")
	parseAnywhere(fs, args)
	targets := 0
	for _, v := range []string{*topic, *thread, *to} {
		if v != "" {
			targets++
		}
	}
	if targets != 1 {
		return fmt.Errorf("usage: mess export --topic NAME | --thread ID | --to AGENT [--format text|json] [--out FILE] [--max N]")
	}
	if *format != "text" && *format != "json" {
		return fmt.Errorf("--format must be text or json")
	}
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	resp, err := call(p, Request{Op: "export", As: name, Topic: *topic, ThreadID: *thread, To: *to, Max: *max})
	if err != nil {
		return err
	}

	var w io.Writer = os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	if *format == "json" {
		enc := json.NewEncoder(w)
		for _, m := range resp.Messages {
			if err := enc.Encode(m); err != nil {
				return err
			}
		}
		return nil
	}
	for _, m := range resp.Messages {
		fmt.Fprintln(w, formatMessageLine(m.Time.Format("2006-01-02 15:04:05"), m))
	}
	if *out != "" {
		fmt.Printf("exported %d message(s) to %s\n", len(resp.Messages), *out)
	}
	return nil
}

// cmdLog searches the durable journal — every message ever sent, unbounded
// (unlike recv/replay/export, which only ever see a bounded recent window).
func cmdLog(p paths, args []string) error {
	fs, as := newFlags("log")
	from := fs.String("from", "", "only messages from this sender")
	topic := fs.String("topic", "", "only messages on this topic")
	grep := fs.String("grep", "", "only messages whose body matches this regexp")
	since := fs.String("since", "", "only messages newer than this (e.g. 90s, 15m, 3h, 2d, 1w)")
	all := fs.Bool("all", false, "search every room, not just your own")
	format := fs.String("format", "text", "text | json")
	out := fs.String("out", "", "write to this file instead of stdout")
	max := fs.Int("max", 0, "limit to the most recent N messages (0 = all)")
	parseAnywhere(fs, args)
	if *format != "text" && *format != "json" {
		return fmt.Errorf("--format must be text or json")
	}
	name, err := agentName(p, *as)
	if err != nil {
		return err
	}
	resp, err := call(p, Request{Op: "log", As: name, From: *from, Topic: *topic, Grep: *grep, Since: *since, All: *all, Max: *max})
	if err != nil {
		return err
	}

	var w io.Writer = os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	if *format == "json" {
		enc := json.NewEncoder(w)
		for _, m := range resp.Messages {
			if err := enc.Encode(m); err != nil {
				return err
			}
		}
		return nil
	}
	for _, m := range resp.Messages {
		fmt.Fprintln(w, formatMessageLine(m.Time.Format("2006-01-02 15:04:05"), m))
	}
	if *out != "" {
		fmt.Printf("logged %d message(s) to %s\n", len(resp.Messages), *out)
	}
	return nil
}

// cmdRm removes an agent from the network (its inbox, subscriptions, presence).
func cmdRm(p paths, args []string) error {
	fs, _ := newFlags("rm")
	room := fs.String("room", "", "room the target agent is in (default: your own room)")
	parseAnywhere(fs, args)
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("usage: mess rm <agent>")
	}
	resp, err := call(p, Request{Op: "rm", To: rest[0], Room: *room})
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
	thread := fs.String("thread", "", "show only this thread's messages (root + replies), not combined with --wait")
	ifIdle := fs.Bool("if-idle", false, "drain only if not currently busy, checked atomically with the drain (not combined with --wait/--follow) — for a caller that must not steal mail out from under a turn that just started")
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
	if *ifIdle && (*wait || *follow) {
		return fmt.Errorf("--if-idle can't be combined with --wait/--follow")
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

	req := Request{Op: "recv", As: name, Wait: *wait, Timeout: timeout, Peek: *peek, Max: *max, Kinds: kinds, Batch: *batch, ThreadID: *thread, IfIdle: *ifIdle}
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
	if resp.Busy {
		if *asJSON {
			fmt.Println(`{"busy":true}`)
		} else {
			fmt.Println("busy — not drained")
		}
		return nil
	}
	if *thread == "" { // a --thread query is browsing history, not "new mail"
		updateLastMsg(p, resp.Messages)
	}
	printMessages(resp.Messages, *asJSON)
	return nil
}

// updateLastMsg records the most recent direct/topic message in msgs (skips
// broadcasts, which have no coherent reply target) as the implicit root for a
// future `mess reply`. Best-effort; called after a successful mess recv.
func updateLastMsg(p paths, msgs []Message) {
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		// If the newest message is itself a reply, target its thread's root —
		// not the reply's own ID — so a further `mess reply` stays flat under
		// the same root instead of spawning a reply-to-a-reply sub-thread.
		root := m.ID
		if m.ThreadID != "" {
			root = m.ThreadID
		}
		switch m.Kind {
		case KindTopic:
			writeLastMsg(p, lastMsgInfo{ID: root, Kind: m.Kind, Topic: m.Topic, From: m.From})
			return
		case KindDirect:
			writeLastMsg(p, lastMsgInfo{ID: root, Kind: m.Kind, From: m.From})
			return
		}
	}
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
		updateLastMsg(p, resp.Messages)
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
	room := fs.String("room", "", "show this room instead of your own")
	all := fs.Bool("all", false, "show every room")
	parseAnywhere(fs, args)
	resp, err := call(p, Request{Op: "ps", Room: *room, All: *all})
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(resp)
	}
	// displayAgent/displayTopic render "name" scoped to one room (the common
	// case: identical to pre-rooms mess) or "room/name" when --all mixes rooms.
	displayAgent := func(a AgentInfo) string { return a.Name }
	displayTopic := func(t TopicInfo) string { return t.Name }
	if *all {
		displayAgent = func(a AgentInfo) string { return displayName(a.Room, a.Name) }
		displayTopic = func(t TopicInfo) string { return displayName(t.Room, t.Name) }
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
			line := fmt.Sprintf("  %-16s %-7s %-9s %d pending", displayAgent(a), presence, status, a.Pending)
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
			line := fmt.Sprintf("  #%-15s %s", displayTopic(t), strings.Join(t.Subscribers, ", "))
			if len(t.Bridged) > 0 {
				line += "  <-> " + strings.Join(t.Bridged, ", ")
			}
			fmt.Println(line)
		}
	}
	if len(resp.Bridges) > 0 {
		fmt.Println("bridges:")
		for _, br := range resp.Bridges {
			expiry := "never"
			if !br.ExpiresAt.IsZero() {
				expiry = br.ExpiresAt.Format(time.RFC3339)
			}
			fmt.Printf("  %-6s %s <-%s-> %s  creator=%s expires=%s\n",
				br.ID, displayName(br.ARoom, "#"+br.ATopic), br.Direction, displayName(br.BRoom, "#"+br.BTopic), br.Creator, expiry)
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

// formatMessageLine renders one message as "TIME FROM #topic: body" / "TIME
// FROM (broadcast): body" / "TIME FROM: body". ts is pre-formatted by the
// caller, since recv/listen show time-only while export shows a full date
// (its output isn't necessarily today's). An attachment, if present, is
// appended distinctly (hash truncated for readability — message ids in this
// codebase are short "m42"-style, so a full 64-hex-char sha256 would visually
// dominate the line; JSON output always carries the full hash untruncated).
func formatMessageLine(ts string, m Message) string {
	var line string
	switch m.Kind {
	case KindTopic:
		line = fmt.Sprintf("%s %s #%s: %s", ts, m.From, m.Topic, m.Body)
	case KindBroadcast:
		line = fmt.Sprintf("%s %s (broadcast): %s", ts, m.From, m.Body)
	default:
		line = fmt.Sprintf("%s %s: %s", ts, m.From, m.Body)
	}
	if m.AttachPath != "" {
		hash := m.AttachHash
		if i := strings.IndexByte(hash, ':'); i >= 0 && len(hash) > i+13 {
			hash = hash[:i+13] // "sha256:" + 12 hex chars
		}
		line += fmt.Sprintf(" [attached: %s (%s, %s)]", m.AttachPath, hash, humanBytes(m.AttachSize))
	}
	return line
}

// humanBytes renders a byte count as a short, human-readable size (B/KB/MB/GB).
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGTPE"[exp])
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
		line := formatMessageLine(m.Time.Format("15:04:05"), m)
		// Only tag actual thread replies with their id — a plain message
		// needs no id, since `mess reply` implicitly threads off the most
		// recent message without you ever having to read/type one.
		if m.ThreadID != "" {
			line = fmt.Sprintf("[thread %s] %s", m.ThreadID, line)
		}
		fmt.Println(line)
	}
}
