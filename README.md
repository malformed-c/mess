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

**Surviving a session-id rotation.** A resumed/relaunched host session gets a
*new* session id, which would orphan a mid-session `register`. So the identity is
*also* persisted under a stable **terminal anchor** — the first of `$MESS_ANCHOR`,
`$TMUX_PANE`, `$STY`, `$TERM_SESSION_ID`, `$KONSOLE_DBUS_SESSION`, `$WINDOWID` —
and `whoami` falls back to it. A relaunch in the *same terminal* recovers the
name; a different terminal doesn't inherit it. (Headless, with no anchor and a
rotating id, use `MESS_AGENT` for a stable identity.)

**Name-collision guard.** `mess register <name>` refuses a name already held by a
different, still-live session in a different terminal — so two agents can't both
grab `alice` and share an inbox. Pass `--force` to take over. A rotated session
that shares the anchor reclaims its own name automatically, and a name whose owner
has gone offline can be taken without `--force`.

## Usage

```sh
mess send bob "build is done"        # direct, fire-and-forget
mess send --ack bob "build is done"  # block until bob reads it (read receipt)
mess send --ack --timeout 30s bob "..."  # ...but give up after 30s
mess broadcast "standup in 5"        # everyone
mess sub builds                      # subscribe to a topic
mess pub builds "green light"        # publish to a topic

mess recv                            # drain queued messages now, exit
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
mess register alice                  # join the network / set a mid-session identity
mess rename alice2                    # rename yourself, keeping your inbox + subscriptions
mess unregister                      # leave the network + clear this session's identity
mess rm opus                         # remove another agent (e.g. a dead session)
mess cleanup                         # prune agents idle >24h and not listening
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

`mess ps` is honest about reachability: the count of parked (`listening`) agents
always equals the daemon's live client connections — no phantom "listening" for a
dead client. When an agent has unread mail it also shows the **age of the oldest
unread message** (e.g. `2 pending (oldest 3m)`).

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
  records the error as the agent's state, and broadcasts it to the fleet. It is
  **notify-only**: Claude Code ignores a `StopFailure` hook's exit code, so
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
        "command": "who=$(mess whoami 2>/dev/null); [ -n \"$who\" ] && mess busy 2>/dev/null; true" } ] }
    ],
    "Stop": [
      { "hooks": [
        { "type": "command",
          "command": "who=$(mess whoami 2>/dev/null); [ -n \"$who\" ] && mess unbusy 2>/dev/null; true" },
        { "type": "command",
          "asyncRewake": true,
          "timeout": 86400,
          "rewakeMessage": "A peer messaged you on mess. Run `mess recv` now to read and clear your inbox (the wake only peeked — unread messages stay queued and re-wake you until you recv).",
          "command": "who=$(mess whoami 2>/dev/null); [ -z \"$who\" ] && exit 0; flock -n \"/tmp/mess-wake-$who.lock\" mess recv --wait --no-broadcast --peek --batch 1s >/dev/null 2>&1 && exit 2 || exit 0" }
      ] }
    ],
    "StopFailure": [
      { "hooks": [
        { "type": "command",
          "command": "in=$(cat); who=$(mess whoami 2>/dev/null); cat=$(printf \"%s\" \"$in\" | jq -r \".reason // .category // .errorType // .error // empty\" 2>/dev/null); if [ -n \"$who\" ]; then mess unbusy 2>/dev/null; mess state \"⚠ API error (turn interrupted)${cat:+: $cat}\" 2>/dev/null; mess broadcast \"$who hit an API error (turn interrupted)${cat:+: $cat}\" 2>/dev/null; fi; true" }
      ] }
    ]
  }
}
```

What each piece does:

- **SessionStart → `mess register`** — joins the network so the session is reachable.
- **UserPromptSubmit / PreToolUse → `mess busy`** and **Stop → `mess unbusy`** —
  drive the accurate `working` status.
- **Stop → auto-wake** (`asyncRewake`): the `flock` guard ensures a single parked
  waiter; `--no-broadcast` avoids a wake storm; `--peek` makes delivery loss-proof
  (a dropped wake re-wakes, never loses mail); `--batch 1s` coalesces a burst into
  one wake; `exit 2` re-invokes the agent, which then runs `mess recv` to consume.
  The **`"timeout": 86400`** is essential: Claude Code reaps a hook command at
  **600s (10 min) by default**, which would kill the parked waiter — and since
  nothing re-arms it until the next turn, the session would go silently deaf after
  ten idle minutes. Raising the timeout lets the waiter stay parked (here up to a
  day) so a peer's message still finds it `listening`. (After the timeout it is
  reaped and re-arms on the next turn; nothing is lost — peek keeps queued mail.)
- **StopFailure → notify only**: a turn that ends in an API error fires
  `StopFailure`, not `Stop`. The hook clears `busy`, records the error as the
  agent's state, and broadcasts it so peers know. It deliberately does **not** try
  to re-arm the auto-wake: Claude Code ignores a `StopFailure` hook's exit code, so
  `asyncRewake` has no effect there (see the warning below).

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
