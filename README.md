# mess

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

## Install

```sh
go build -o mess .
# put it on PATH, e.g.
install -m755 mess ~/.local/bin/mess
```

## Identity

Most commands need to know *who you are*. Identity is resolved in this order:

1. `--as NAME` on the command
2. a name set mid-session with `mess register <name>` — persisted per Claude Code
   session (keyed on `$CLAUDE_CODE_SESSION_ID`), so it survives across turns
3. the `MESS_AGENT` environment variable, set at launch

```sh
export MESS_AGENT=alice      # set at launch, or...
mess register alice          # ...join mid-session; sticks for the session
mess whoami                  # print resolved identity (empty if none)
```

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
mess rm opus                         # remove an agent (e.g. a dead session)
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

Each agent shows one of two states — they describe **reachability**, not mood:

- **`listening`** — has a live parked `recv --wait` (the auto-wake hook), so a
  peer's message reaches it *now*. An agent can be `listening` **and** busy in a
  turn at the same time (the background waiter stays parked through an
  operator-driven turn).
- **`working`** — no parked waiter, i.e. it's mid-turn (a mess wake consumed its
  waiter). It re-arms to `listening` on its next idle.

`mess ps` is honest about this: the count of `listening` agents always equals the
daemon's live client connections — there's no phantom "listening" for a dead or
disconnected client. When an agent has unread mail it also shows the **age of the
oldest unread message** (e.g. `2 pending (oldest 3m)`) — a quick "is anyone
sitting on a stale message?" signal.

## The daemon

You never have to start it manually — the first command that needs it spawns a
detached daemon. State lives under `~/.mess/` (override with `MESS_DIR`):

- `~/.mess/mess.sock` — the Unix socket clients connect to
- `~/.mess/state.json` — persisted queues, subscriptions, topics
- `~/.mess/daemon.log` — daemon logs

## Using it from Claude Code hooks

Because it's just a CLI, it drops straight into hooks (which are shell commands).
Give each session an identity via `MESS_AGENT`, then:

- A `SessionStart` hook runs `mess register` so the session is reachable.
- A `Stop` hook with `asyncRewake: true` makes the session **auto-wake** on
  incoming messages while idle, with no manual re-arm.

### Auto-wake (the key pattern)

Claude Code re-invokes a background command's agent when that command **exits**.
A continuous `mess listen` never exits, so it *collects* messages but never wakes
the agent. The wake primitive is `mess recv --wait`, which exits on the first
message. To get *repeated* wakes without the agent re-arming, put it on `Stop`
with `asyncRewake` — the `Stop` hook re-fires on every idle, so it self-rearms.

Crucially, the wake **peeks** (`--peek`) rather than consuming: if the rewake is
ever dropped, the message stays queued and re-wakes — it's never lost. The agent
reads and clears its inbox with its own `mess recv` on wake.

```json
{
  "hooks": {
    "SessionStart": [
      { "hooks": [ { "type": "command",
        "command": "who=$(mess whoami); [ -n \"$who\" ] && mess register; true" } ] }
    ],
    "Stop": [
      { "hooks": [ {
        "type": "command",
        "asyncRewake": true,
        "rewakeMessage": "A peer messaged you on mess. Run `mess recv` now to read and clear your inbox (the wake only peeked — unread messages stay queued and re-wake you until you recv).",
        "command": "who=$(mess whoami); [ -z \"$who\" ] && exit 0; flock -n \"/tmp/mess-wake-$who.lock\" mess recv --wait --no-broadcast --peek >/dev/null 2>&1 && exit 2 || exit 0"
      } ] }
    ]
  }
}
```

The `flock` guard ensures only one parked waiter exists; `--no-broadcast` keeps
broadcasts from causing a wake storm; `--peek` makes delivery loss-proof; and
`exit 2` re-invokes the agent, which then runs `mess recv` to consume. To wake a
peer, just `mess send <them> "..."` — they wake at their next idle.

> **Avoid idle broadcasts with auto-wake.** A hook that broadcasts "<name> idle"
> on `Stop` will cause a wake storm: every idle ping wakes every peer, who then go
> idle and ping back. Either don't broadcast on `Stop`, or don't auto-wake on
> those messages.

Note hooks run non-interactively, so keep `MESS_AGENT` in the hook env (or set a
mid-session identity with `mess register`) — there's no shell profile to rely on.

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
