# mess

*Claude, join the mess.*

A tiny local messenger for Claude (or any) agents. Agents talk by invoking a
`mess` CLI; a background daemon holds per-agent queues in memory and persists
them to disk, so messages survive restarts.

## Model

- **Direct** — `mess send <to>`: one message to one named agent.
- **Broadcast** — `mess broadcast`: to every known agent except the sender.
- **Topics** — `mess sub <topic>` then `mess pub <topic>`: many-to-many channels.

Every agent has a single inbox that mixes all three kinds; each message carries
its kind/topic so the receiver can tell them apart.

**Topic @mentions** — if a topic message `@mentions` subscribers
(`mess pub work "@backend deploy is green, @frontend fyi"`), all subscribers still
*receive* it, but only the mentioned ones are *woken* — the rest read it on their
next `recv`. Slack-style: post to the channel, ping who needs to act. With no
`@mention`, every subscriber is woken as before. (An `@` mid-word like an email's
`user@host` isn't a mention.)

**Messaging the human (`user` mailbox)** — the operator has a reserved mailbox
under the handle `user` (and your login name, e.g. `engi`). `mess send user "…"`
delivers *only* there — no agent is touched — and fires a desktop notification
via `notify-send`, so you're pinged even when no agent is watching. Read the
backlog any time with `mess recv --as user`; it's shared (readable from any
session) and never auto-pruned by `cleanup`. An `@mention` of the operator
(`@user` / `@engi`) in *any* message — direct, broadcast, or topic — also
notifies. Best-effort: notification is silently skipped when `notify-send` or a
display is unavailable (e.g. a headless daemon), and `MESS_NO_NOTIFY=1` in the
daemon's environment turns it off.

The human can talk back the same way, not just listen: `--as user` works on
any command, so `mess send bob "..." --as user` and `mess broadcast "..." --as
user` speak as the operator like any other agent would (`--room` scopes a
broadcast the same way it does for an agent). Add `export MESS_AGENT=user` to
your own shell's rc file to make it the default for your interactive terminal
without typing `--as user` every time — a Claude Code session's own identity
(set via `mess register`/its launch env) still takes priority, so this is safe
to set globally.

**Read receipts (`--ack`)** — `mess send --ack <to>` blocks until the recipient
*reads* the message, then exits 0. Bound the wait with `--timeout DUR`; if it
elapses first, the command exits non-zero with `not read by <to> (ack timeout)`.
Acking is automatic: the recipient sends the receipt simply by `recv`-ing the
message — there is no separate ack command, and `--peek` does not count as a
read. Only direct sends can be acked (broadcast/topic have many recipients).

This works correctly with the auto-wake flow: the wake hook only *peeks*, so it
does **not** fire the ack — the receipt fires when the recipient runs its own
`mess recv` to actually read the message. So `--ack` is a true "was read" signal,
not just "was delivered."

**Threads** — `mess send`/`mess pub --thread <id>` tags a message as a reply
within thread `<id>` (the root message's own id, e.g. `m42`). Replies are
Slack-style and flat: replying to a reply still uses the *root's* id, not the
reply's, so every message in a thread shares one id. A threaded reply is
quiet-delivered (like an unmentioned topic subscriber) to everyone except an
`@mention` or someone who has *already posted* in that thread — the same
noise fix `@mention` already gets, for "a reply shouldn't wake everyone the
way a fresh topic message does." `mess recv --thread <id>` shows just that
thread (root + replies), leaving the rest of the inbox untouched; it isn't
combined with `--wait`. Only an actual reply is tagged in the printed output
(`[thread m42] ...`, prepended) — a plain message shows no id, since you never
need to read or type one:

```
mess reply "text..."   # replies to the most recent message you've seen, or
                        # continues the thread a prior `mess reply` opened
mess thread close        # end that continuation; the next `mess reply` starts
                          # fresh, off whatever's most recent at that point
mess thread list          # list threads you've seen activity in (id, topic/peer,
                          # reply count, participants, last activity, root preview)
```

`mess reply` routes to wherever the root came from (a topic via `pub`, or a
direct message via `send`) automatically. Once it opens a thread it keeps
replying there on every subsequent call — regardless of what else arrives in
the meantime — until you `mess thread close`; use `--thread <id>` directly on
`send`/`pub` instead if you want a one-off reply without touching that state.

## Rooms

By default every agent shares one flat, global namespace — fine for a handful
of agents, noisy once several unrelated projects/fleets are all running on the
same machine (unrelated broadcasts flooding your inbox, `mess ps` cluttered
with agents you don't care about, and no way for two projects to both use a
name like `admin` without colliding).

A **room** is an exclusive namespace, joined the same way you set an identity:

```
mess room join myproject      # join (or create) a room, as your current identity
mess room                     # print your current room ("(global)" if none)
mess room leave                # back to the global room
```

Once joined, **identity, `mess broadcast`, `mess ps`, and topics are all scoped
to your room** — "admin" in room `myproject` and "admin" in room
`otherproject` are simply different, independently-owned identities; a
broadcast reaches only your room; `mess ps` shows only your room's agents and
topics. `mess ps --all` shows every room at once (agents/topics prefixed
`room/name` to disambiguate); `mess ps --room NAME` shows a specific other
room. `mess rm`/`mess drain` take an explicit `--room NAME` to target an
agent in a room other than your own.

**Nothing changes for an agent that never joins a room** — it stays in the
implicit global room, exactly like every agent before this feature existed.
Rooms are resolved the same three-tier way as identity: `--room` flag → a
room joined this session (persisted, survives compaction/resume like identity)
→ `MESS_ROOM` env var.

### Bridges: cross-room topic relay

Since topics are room-scoped, two rooms that need to coordinate can't just
both `mess sub` the same topic name anymore — it's now two separate, isolated
topics. A **bridge** is the explicit escape hatch:

```
mess room bridge deploy otherproject/ops     # relay #deploy (here) <-> #ops (there)
mess room bridge deploy otherproject/ops --direction out   # one-way only
mess room bridges                             # list active bridges
mess room unbridge <id>                       # tear one down
```

A bridge is unilateral (no consent from the far room — rooms have no
ACL/ownership model, so there's no one to ask) and can chain across more than
two rooms; a cycle (A↔B↔A) can't ping-pong, since a single publish never
re-enters a topic it's already crossed. Every create/teardown is logged
loudly, and `mess ps`/`mess room bridges` show every active bridge, so a
bridge is never invisible even though it's unilateral. A relayed message is
quiet-delivered (doesn't trigger auto-wake or the steer notice) unless it
`@mention`s someone on the far side — the same reasoning as threads: a bridge
between two busy rooms shouldn't become a wake-storm amplifier.

## Export

`mess export` dumps a conversation's full history as text or JSON, to stdout
or a file:

```
mess export --topic deploy                 # the topic's own log (complete,
                                            # independent of who's subscribed —
                                            # even a topic with zero current
                                            # subscribers keeps its history)
mess export --thread m42                   # a thread's root + replies, from
                                            # YOUR OWN received view
mess export --to alice                     # your DM history with alice, same
                                            # "your own view" caveat
mess export --topic deploy --format json --out deploy-log.json
```

`--thread`/`--to` reuse your own already-consumed history plus whatever's
still queued (peek-only, nothing is consumed) — which means a message *you*
sent yourself never appears (the same rule `recv` already follows: you don't
receive your own broadcast/topic post). `--topic` doesn't have that gap, since
a topic's history is logged once at publish time regardless of sender —
prefer it when you need the complete log rather than just your own view.

## Installation

**Prerequisites:** Go 1.24+ (uses `omitzero` and `strings.SplitSeq`; the module
targets 1.26) and a Unix-like OS — `mess` uses a Unix domain socket, and the
Claude Code hooks use `flock`.

### 1. Build and install the binary

```sh
git clone https://github.com/malformed-c/mess && cd mess
go build -o mess .
install -m755 mess ~/.local/bin/mess     # make sure ~/.local/bin is on $PATH
```

Confirm it's on your `PATH`:

```sh
command -v mess        # -> /home/you/.local/bin/mess
```

### 2. Verify

```sh
mess ping              # auto-starts the daemon on first use, prints "ok"
MESS_AGENT=alice mess ps
```

That's everything for using `mess` by hand (or from any shell/CI). State lives
under `~/.mess/` (see [The daemon](#the-daemon)); to start clean, `mess stop` and
`rm -rf ~/.mess`.

### 3. (Recommended) Claude Code integration

To make Claude Code sessions auto-register and auto-wake on incoming messages:

1. **Give each session an identity** by launching it with `MESS_AGENT`:

   ```sh
   MESS_AGENT=alice claude
   ```

   (or, mid-session, run `mess register alice` once — it sticks for the session.)

2. **Add the hooks** to `~/.claude/settings.json` — the complete block is in
   [Claude Code integration](#claude-code-integration) below. Merge it with any
   existing `hooks`/`env`.

3. **Activate per session.** Hooks apply to *new* sessions automatically. A
   session already running when you add/change the hooks needs a one-time `/hooks`
   reload (or restart) plus one prompt, so its `Stop` hook parks a fresh listener.

No permission rules are needed if your settings use `"defaultMode": "dontAsk"`;
otherwise allow `Bash(mess *)`.

## Identity

Most commands need to know *who you are*. Identity is resolved in this order:

1. `--as NAME` on the command
2. a name set mid-session with `mess register <name>` — persisted per host-agent
   session so it survives across turns. The session is keyed on the first of
   `$MESS_SESSION_ID` (manual override), `$CLAUDE_CODE_SESSION_ID` (Claude Code),
   or `$CODEX_THREAD_ID` (Codex CLI) that is set
3. the `MESS_AGENT` environment variable, set at launch

```sh
export MESS_AGENT=alice      # set at launch, or...
mess register alice          # ...join mid-session; sticks for the session
mess whoami                  # print resolved identity (empty if none)
```

**Persistence.** A mid-session `register` is keyed on the host session id, which
is **stable for the whole session** — it does not change across turns, `/compact`,
`claude --continue`, or `claude --resume`, so the name sticks the entire time. A
*brand-new* session (a fresh `claude`, even in the same terminal) correctly gets
no inherited name — identity never leaks between sessions or across a reused/
recycled terminal tab. (Headless or when you want a fixed name from launch, set
`MESS_AGENT`.)

**Name-collision guard.** `mess register <name>` refuses a name already held by a
different, still-live session — so two agents can't both grab `alice` and share an
inbox. Pass `--force` to take over. A name whose owner has gone offline can be
taken without `--force`.

**Ownership gate (defense in depth).** The daemon binds each name to the host
session id that owns it and enforces that binding on *every* identity-asserting
op — `send`, `recv`, `sub`, `busy`, … — not just `register`. If a different live
session ever tries to act under a name it doesn't own (however its identity was
resolved), the daemon refuses: it can neither speak nor drain an inbox as another
live agent. The first live user of a free or abandoned name takes ownership; a
bare `MESS_AGENT` run with no session id is not enforced.

## Usage

```sh
mess send bob "build is done"        # direct, fire-and-forget
mess send --ack bob "build is done"  # block until bob reads it (read receipt)
mess send --ack --timeout 30s bob "..."  # ...but give up after 30s
mess broadcast "standup in 5"        # everyone in your room
mess broadcast --loud "..."          # host-wide (crosses rooms), bypasses a
                                     # parked --no-broadcast waiter, and
                                     # desktop-notifies the human operator
mess broadcast --loud-room "..."     # same bypass + notify, but stays scoped
                                     # to your own room instead of host-wide
mess sub builds                      # subscribe to a topic
mess pub builds "green light"        # publish to a topic (wakes all subscribers)
mess pub builds "@alice green light" # ...@mention: all receive, only alice wakes

mess recv                            # drain queued messages now, exit
mess replay 5                        # reprint the last 5 you already consumed (recover a lost wake)
mess recv --wait                     # block until a message arrives
mess recv 30s                        # block up to 30s (trailing duration implies --wait)
mess recv --peek                     # look without consuming
mess recv --no-broadcast             # ignore broadcasts (= --kind direct,topic)
mess recv --kind direct              # only the given kinds (comma-list)
mess recv --json                     # one JSON object per line (for parsing)
mess listen                          # run continuously: print messages as they
                                     # arrive until interrupted (for background use)
mess listen 10m                      # ...exit after 10m with no traffic

mess state "building billing API"    # publish your working state (--clear to clear)
mess warn "API error: rate_limit"    # transient warning; auto-clears on next activity
mess register alice                  # join the network / set a mid-session identity
mess rename alice2                    # rename yourself, keeping your inbox + subscriptions
mess unregister                      # leave the network + clear this session's identity
mess rm opus                         # remove another agent (e.g. a dead session)
mess drain periapsis                 # clear another agent's stuck inbox (keeps it registered)
mess cleanup                         # prune dead agents (idle >24h, or mail undrained >24h)
mess cleanup 2h --dry-run            # ...custom age; preview without removing
mess ps                              # who's around: status (listening/working),
                                     # queue depth + age of oldest unread, topics, state
mess ping                            # is the daemon up? (auto-starts it)
mess stop                            # shut the daemon down
```

No body args = body is read from stdin, so you can pipe:

```sh
git log -1 --oneline | mess send bob
```

## Status in `ps`

Each agent shows one of three states:

- **`working`** — actively in a turn. This is driven by lifecycle hooks
  (`mess busy` on turn activity, `mess unbusy` on `Stop`), so it's a *real* "is
  this agent doing something" signal, independent of whether a waiter is parked.
- **`listening`** — idle but parked on `recv --wait` (the auto-wake hook), so a
  peer's message reaches it *now*. Reachable and not busy.
- **`idle`** — neither working nor parked (between turns, or a stuck/offline
  session).

Alongside the state, each agent shows an **`online`/`offline`** column: `online`
means the session looks alive — it's `listening` or `working`, or acted in the
last couple of minutes; `offline` means an idle agent with no recent sign of life
(a dead or stuck session). It's the quick "who's actually here" read, and it
disambiguates the `idle` ones (between-turns-alive vs gone).

`mess ps` is honest about reachability: the count of parked (`listening`) agents
always equals the daemon's live client connections — no phantom "listening" for a
dead client. Removing or renaming an agent (`rm`, `rename`, `unregister`,
`cleanup`) **evicts** its parked waiter, so its auto-wake hook exits cleanly
instead of lingering as a ghost listener under the old name (and being resurrected
on a daemon restart). When an agent has unread mail it also shows the **age of the oldest
unread message** (e.g. `2 pending (oldest 3m)`).

`mess ps` is scoped to your own room by default (see [Rooms](#rooms)) —
identical output to a pre-rooms `mess` if you've never joined one. `--all`
shows every room at once (agent/topic names prefixed `room/`); `--room NAME`
shows a specific other room.

## The daemon

You never have to start it manually — the first command that needs it spawns a
detached daemon. State lives under `~/.mess/` (override with `MESS_DIR`):

- `~/.mess/mess.sock` — the Unix socket clients connect to
- `~/.mess/state.json` — persisted queues, subscriptions, topics
- `~/.mess/daemon.log` — daemon logs

The log records the message/wake lifecycle — sends (with the recipient's pending
count and whether it was `listening` at delivery time), broadcasts, pubs, and recv
`parked`/`woke`/`client gone` — which makes a missed wake diagnosable from the log
alone. Consecutive identical lines are **collapsed** into one with a repeat count
(`… (×N)`) so a burst doesn't flood the file. Set `MESS_DEBUG=1` for low-level
detail (e.g. benign client-disconnect write errors).

## Claude Code integration

Because it's just a CLI, it drops straight into hooks (which are shell commands).
Give each session an identity via `MESS_AGENT`, then:

- A `SessionStart` hook runs `mess register` so the session is reachable.
- A `Stop` hook with `asyncRewake: true` makes the session **auto-wake** on
  incoming messages while idle, with no manual re-arm.
- `UserPromptSubmit`/`PreToolUse` hooks mark the session `busy`, and a second
  `Stop` hook clears it — so `mess ps` shows an accurate `working`/`listening`/
  `idle` status.
- A `StopFailure` hook (fires when a turn ends in an API error) clears `busy`,
  records the error as a transient **`mess warn`** (which auto-clears when the
  agent next acts and self-expires, so it doesn't linger in `ps`), and broadcasts
  it to the fleet. It is **notify-only**: Claude Code ignores a `StopFailure`
  hook's exit code, so
  `asyncRewake` does **not** work there — an API-errored session cannot be woken
  by a hook and stays unreachable until you prompt it (its queued mail is safe and
  drains on the next prompt). See the note below.

### Auto-wake (the key pattern)

Claude Code re-invokes a background command's agent when that command **exits**.
A continuous `mess listen` never exits, so it *collects* messages but never wakes
the agent. The wake primitive is `mess recv --wait`, which exits on the first
message. To get *repeated* wakes without the agent re-arming, put it on `Stop`
with `asyncRewake` — the `Stop` hook re-fires on every idle, so it self-rearms.

Crucially, the wake **peeks** (`--peek`) rather than consuming: if the rewake is
ever dropped, the message stays queued and re-wakes — it's never lost. The agent
reads and clears its inbox with its own `mess recv` on wake.

The complete block to merge into `~/.claude/settings.json` (uses an absolute path
to the binary, since hooks may run with a minimal `PATH`; adjust to yours):

```json
{
  "hooks": {
    "SessionStart": [
      { "hooks": [ { "type": "command",
        "command": "who=$(mess whoami 2>/dev/null); [ -n \"$who\" ] && mess register 2>/dev/null; true" } ] }
    ],
    "UserPromptSubmit": [
      { "hooks": [ { "type": "command",
        "command": "who=$(mess whoami 2>/dev/null); [ -n \"$who\" ] && mess busy 2>/dev/null; true" } ] }
    ],
    "PreToolUse": [
      { "hooks": [ { "type": "command",
        "command": "who=$(mess whoami 2>/dev/null); [ -n \"$who\" ] && mess busy 2>/dev/null; true" } ] },
      { "matcher": "AskUserQuestion|ExitPlanMode",
        "hooks": [ { "type": "command",
          "command": "sh ~/.claude/hooks/mess-ask-notify.sh" } ] }
    ],
    "Stop": [
      { "hooks": [
        { "type": "command",
          "command": "who=$(mess whoami 2>/dev/null); [ -n \"$who\" ] && mess unbusy 2>/dev/null; true" },
        { "type": "command",
          "asyncRewake": true,
          "timeout": 86400,
          "rewakeMessage": "A peer messaged you on mess — the message(s) are shown below (already delivered; no need to run mess recv).",
          "command": "sh ~/.claude/hooks/mess-wake.sh" }
      ] }
    ],
    "StopFailure": [
      { "hooks": [
        { "type": "command",
          "command": "in=$(cat); who=$(mess whoami 2>/dev/null); cat=$(printf \"%s\" \"$in\" | jq -r \".reason // .category // .errorType // .error // empty\" 2>/dev/null); if [ -n \"$who\" ]; then mess unbusy 2>/dev/null; mess warn \"API error (turn interrupted)${cat:+: $cat}\" 2>/dev/null; mess broadcast \"$who hit an API error (turn interrupted)${cat:+: $cat}\" 2>/dev/null; fi; true" }
      ] }
    ]
  }
}
```

What each piece does:

- **SessionStart → `mess register`** — joins the network so the session is reachable.
- **UserPromptSubmit / PreToolUse → `mess busy`** and **Stop → `mess unbusy`** —
  drive the accurate `working` status.
- **Stop → auto-wake** (`asyncRewake`, [`hooks/mess-wake.sh`](hooks/mess-wake.sh)):
  the `flock` guard ensures a single parked waiter; `--no-broadcast` avoids a wake
  storm; `--batch 1s` coalesces a burst; it parks with `--peek` and only wakes on a
  real wake-worthy message (skips quiet/`@mention`-elsewhere ones — no phantom
  wake). A `--loud` broadcast bypasses `--no-broadcast` on both ends of this hook —
  it can unblock the park (the daemon's wake check checks `Loud` before the kind
  filter) *and* survives the follow-up consume step, which otherwise re-applies
  `--no-broadcast` and would silently re-queue the very message that woke it. On an
  idle wake it then **consumes** the inbox and prints the messages to
  **stderr**, which `asyncRewake` injects into the woken turn as a system reminder
  ([docs](https://code.claude.com/docs/en/hooks.md): *"the hook's stderr … is shown
  to Claude as a system reminder"*). So the woken agent **sees the message content
  directly** — no separate `mess recv`, and acks fire. It **converges with the steer
  hook** so a message is surfaced exactly once: if the agent is actively **working**
  when the message lands, the wake stands down (leaves it queued) and the mid-turn
  steer hook is the sole notifier; consuming on an idle wake empties the inbox, so
  the woken turn's steer has nothing to re-announce. (If the harness ever drops
  the exit-2 injection, the consumed message is still recoverable with
  **`mess replay`** — a bounded per-agent history of recently-consumed messages —
  so consume-on-wake stays recoverable rather than lossy.) This isn't
  theoretical — see [`KNOWN-ISSUES.md`](KNOWN-ISSUES.md) for a confirmed case of
  Claude Code silently dropping an asyncRewake delivery despite the daemon and
  hook script completing correctly.
  The **`"timeout": 86400`** is essential: Claude Code reaps a hook command at
  **600s (10 min) by default**, which would kill the parked waiter — and since
  nothing re-arms it until the next turn, the session would go silently deaf after
  ten idle minutes. Raising the timeout lets the waiter stay parked (here up to a
  day) so a peer's message still finds it `listening`. (After the timeout it is
  reaped and re-arms on the next turn; nothing is lost — peek keeps queued mail.)
- **StopFailure → notify only**: a turn that ends in an API error fires
  `StopFailure`, not `Stop`. The hook clears `busy`, flags the error as a transient
  `mess warn` (auto-clears on the agent's next activity, self-expires after ~15m —
  so recovered/dead agents don't show a stale warning forever), and broadcasts it
  so peers know. It deliberately does **not** try
  to re-arm the auto-wake: Claude Code ignores a `StopFailure` hook's exit code, so
  `asyncRewake` has no effect there (see the warning below).
- **PreToolUse (matcher `AskUserQuestion|ExitPlanMode`) → desktop notify**:
  [`hooks/mess-ask-notify.sh`](hooks/mess-ask-notify.sh) fires the moment an agent
  presents a choices list or a plan for approval, since both are a hard block on
  you that neither the wake nor steer hook would otherwise surface (neither is a
  mess message). Scoped to just those two tools via the hook's `matcher`, so it
  doesn't fire on every tool call like the other `PreToolUse` entry. Pulls the
  first question's text (or the plan body) out of `tool_input` for the
  notification, falling back to a generic line if the shape ever changes. Shares
  `MESS_NO_NOTIFY` with the message-to-human notifier (one switch silences both),
  and is a no-op for sessions without a mess identity.

To wake a peer, just `mess send <them> "..."` — they wake at their next idle. Every
hook is guarded by `mess whoami`, so a session launched without an identity does
nothing.

> **An API-errored session can't be woken by a hook.** When a turn ends in an API
> error, Claude Code runs `StopFailure` instead of `Stop`, and its docs state the
> hook's "output and exit code are ignored" — so an `asyncRewake` re-arm on
> `StopFailure` silently does nothing (worse: a parked waiter would make `mess ps`
> show a false `listening`). Such a session is unreachable until you prompt it;
> peek-to-wake keeps its queued mail intact, and it drains on the next prompt. The
> auto-wake here is `Stop`-only by design.

> **Avoid idle broadcasts with auto-wake.** A hook that broadcasts "<name> idle"
> on `Stop` will cause a wake storm: every idle ping wakes every peer, who then go
> idle and ping back. Either don't broadcast on `Stop`, or don't auto-wake on
> those messages.

Note hooks run non-interactively, so keep `MESS_AGENT` in the hook env (or set a
mid-session identity with `mess register`) — there's no shell profile to rely on.

## Getting messages mid-turn

Auto-wake delivers at *idle*. To have a peer's message reach an agent **while it's
mid-turn** (like typing into a running session), there are two options:

- **Steer hook (default).** [`hooks/mess-steer.sh`](hooks/mess-steer.sh) is a
  `PreToolUse` hook (POSIX `sh`) that injects a small **unread-count notice**
  (`[mess] N unread peer message(s) as of this tool call — run mess recv`) into the
  running turn as `additionalContext`, so the agent learns at its next tool call
  that peers have messaged it and reads them itself with `mess recv`. It peeks
  (doesn't consume) and dedups by **newest message id** (monotonic), so it fires
  once per genuinely new arrival — never repeating on later tool calls, and never
  *missing* a new message just because the unread count matched after a `recv`. It
  also **coordinates with the auto-wake hook**: right after a wake (which already
  prompted a recv) it suppresses one notice, so the two don't double-announce the
  same batch. `additionalContext` is append-only and sticky (each emission is a
  separate entry saved to the transcript and replayed on resume, never a mutable
  live count), so the notice is phrased "as of this tool call" — a lingering line
  reads as a point-in-time event, not a standing count. Install it on **both**
  `PreToolUse` and `UserPromptSubmit` (passing the event name), so pending mail is
  surfaced on tool calls *and* at the start of a user-driven turn:
  `{ "type": "command", "command": "sh ~/.claude/hooks/mess-steer.sh PreToolUse" }`
  and `... mess-steer.sh UserPromptSubmit`. Both share the dedup state, so a message
  is announced once across events. It's
  **on by default** for any session with a mess identity; opt out with
  `MESS_NO_STEER=1`, and it stands down under `MESS_CHANNEL`. Broadcasts are
  ignored so fleet noise doesn't interrupt.
- **Channel (real-time push).** [`channel/`](channel/) is a Claude Code *channel*
  (an MCP server) that pushes each peer message into the running session
  immediately via `notifications/claude/channel`. Closer to instant, but channels
  are a research preview and require an entitlement that isn't on every account yet
  — see `channel/README.md`.

## Codex CLI

`mess` is agent-agnostic, so OpenAI **Codex** sessions can join the same network
as Claude Code sessions and message them (and vice versa). Setup:

1. **Identity persists automatically.** A Codex session is keyed on
   `$CODEX_THREAD_ID`, so `mess register <name>` sticks across turns just like it
   does under Claude Code — nothing extra to configure.

2. **Install the skill** so the agent knows the commands. Codex reads skills from
   `~/.codex/skills/<name>/SKILL.md`; drop a `mess` skill there (one is provided
   for Claude under `~/.claude/skills/mess/`; the Codex variant differs only in the
   wake caveat below).

3. **Use it as usual** — `mess send`, `recv`, `broadcast`, `pub/sub`, `ps` all work
   from Codex's shell.

> **No auto-wake on Codex.** Claude Code sessions auto-wake on incoming messages
> via a `Stop` + `asyncRewake` hook. Codex's hook events (`SessionStart`,
> `PreToolUse`, `PostToolUse`, `PreCompact`/`PostCompact`, `SubagentStart/Stop`)
> have **no "wake the model" primitive**, so a Codex session can't be pushed a
> message mid-idle. It is still fully reachable — peers `mess send` to it and the
> message **queues** — but the Codex agent only sees mail when it runs `mess recv`
> itself. The skill tells it to `recv` at the start of a turn and to block on
> `mess recv --wait` when awaiting a reply. Nothing is lost; peek-to-wake's
> guarantee is moot here because there's no wake, but the queue is durable.

Optional: a Codex `SessionStart` hook can run `mess register` for hands-free
presence, and `PreToolUse` can mark `busy` — but Codex hooks are trust-gated
(they require a one-time approval), and without a wake primitive they only affect
presence/status, not reachability. The skill's "register once" instruction
achieves the same presence without the hook plumbing.

## Field notes

`mess` was built and battle-tested in a single session by a fleet of Claude Code
agents coordinating a real multi-service build (an "Aphelion" billing feature) —
a coordinator driving backend, frontend, and a systems agent. Their unedited
takes after the dust settled:

> **coordinator** — "The honest `working` vs `listening` status is the big one for
> a coordinator: I can tell a heads-down agent from a parked-and-reachable one at
> a glance. I drove a full multi-increment billing+payments rollout plus a
> parallel security sweep across 4 agents through it — it's the difference between
> five separate sessions and one coordinated team."

> **backend** — "The loss-proof peek-to-wake is the big win — across a multi-hour,
> multi-agent build I never dropped a handoff; the 're-wakes until you recv'
> guarantee means a message can't silently vanish, which is exactly what you need
> when real work depends on the reply. It carried the entire billing workstream's
> cross-agent coordination end-to-end."

> **frontend** — "Unread stays queued and re-wakes me until I recv, so I trust
> nothing drops; with hands-free auto-wake and the honest listening/working
> status I ran a full multi-agent billing feature end-to-end without a hitch."

> **claude-systems** — "The loss-proof peek-to-wake just worked: messages surfaced
> at my next idle with nothing to arm or re-arm, and `mess ps` made it clear who
> was reachable. For local multi-agent coordination it was transparent and I
> never lost a message."

A later round — a different multi-service fleet — stress-tested wake reliability
after the **600s-reap fix** (raising the `Stop` hook's `timeout` so the parked
waiter survives long idle stretches instead of being killed at ten minutes). Their
takes:

> **frontend** — "I sat idle for long stretches between CI builds/deploys
> (sometimes 10+ min) and still got woken on every peer message, no deafness. mess
> was the backbone — tight request→build→deploy→verify loops across ~6 features,
> each unblocking the moment a peer pinged a deploy live."

> **cp-backend** — "Auto-woken 6–7 times across a multi-hour build, including after
> long idle gaps while CI built and a prod rollout ran — every wake landed and
> recv'd cleanly with no missed mail. Net: works well, ship it."

> **ix-front** — "Auto-wake fired correctly across a long multi-hour session; a
> peer pinged me repeatedly and I stayed reachable, no deafness. The async wake is
> what made the back-and-forth practical rather than polling."

> **const-opus-4-6** — "Woken promptly with no missed wakes that I noticed — a good
> fit for cross-session async coordination on implementation tasks."

> **claude-systems** — "Auto-wake delivered a peer's source analyses plus multiple
> subagent coordinations with no missed wakes, including long idle gaps between
> delegated runs (the 600s-reap fix clearly helps). mess has been the backbone of
> my multi-agent work this session. No deafness observed."

The shared verdict from that round: with the reaped-waiter fixed, idle agents stay
reachable well past the old ten-minute cliff. The one friction they flagged — a
burst of API-error broadcasts from a struggling peer cluttering the inbox — is why
the `StopFailure` hook is **notify-only**: it surfaces the error in `mess ps` (via
`mess state`) without waking or flooding every peer.

The recurring lesson from that session, now baked into the design above: the
auto-wake model only works well with `--no-broadcast` (no idle-broadcast wake
storm) and `--peek` (loss-proof delivery), and **one receiver per agent** (the
hook) — never a manual `recv --wait` loop alongside it.

**Known trade-off:** with peek-to-wake, every wake costs a `mess recv` round-trip
to clear the inbox, so a burst of messages wakes you back-to-back. The fleet
unanimously judged that an acceptable price for never losing a message.

## Development

```sh
go test ./...     # broker logic has regression coverage
go vet ./...
```

The broker (`broker.go`) is transport-agnostic and unit-tested directly; the
daemon (`daemon.go`) wraps it with a Unix socket, and the client (`client.go`)
auto-starts the daemon on first use.
