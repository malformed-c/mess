# Known issues

## Identity leaks into Claude Code subagents (Task/Agent tool)

**Symptom:** a subagent spawned via Claude Code's Task/Agent tool inherits the
*exact same* `CLAUDE_CODE_SESSION_ID` as its parent session. Since `mess`
identity is keyed solely on that session id (see `identity.go`), any subagent
that runs `mess` resolves to the **same identity as its parent** — `mess
whoami` inside a subagent reports the parent's name, with no indication it's
actually a subagent speaking.

Confirmed empirically (2026-07-04): spawned a subagent from a session
registered as `K` and asked it to run `mess whoami` — it reported `K`, same as
the parent.

**Consequences, all confirmed reachable:**
- A subagent's `mess recv` (not `--peek`) **drains the parent's inbox**. Since
  drain is destructive, the parent then sees nothing — a message can arrive,
  get consumed by a subagent doing unrelated work, and vanish from the parent's
  point of view with zero trace. This is a distinct failure mode from the
  `asyncRewake` drop above: that one is a harness bug outside `mess`; this one
  is `mess`'s own identity model failing to account for a caller that isn't
  really the session it claims to be.
- A subagent's `mess register <name>` would rename the parent's identity out
  from under it (next `mess whoami` in the parent returns the new name).
- Two parallel subagents that both happen to touch `mess` collide under the
  identical name, same as any other "two waiters, one inbox" race the README
  already warns about — except here neither side has any way to know it's
  sharing an identity, since both look like a normal single session.

**Why the daemon's session-ownership guard (`ClaimIdentity` /
`foreignLiveOwnerLocked` in `broker.go`) doesn't catch this:** that guard exists
specifically to reject a *different* live session acting under a name it
doesn't own — but it treats a *matching* session id as proof of being the same,
trusted actor. A subagent sharing its parent's session id passes that check
trivially; the guard's whole model assumes session-id equality implies
same-actor, which is exactly the assumption a shared child session id breaks.

**Attempted fix, reverted:** `CLAUDE_CODE_CHILD_SESSION=1` looks at first like
the distinguishing signal — set for subagent invocations. It isn't usable: it's
set identically for the *top-level session's own* Bash tool calls too, not just
subagents' (confirmed by dumping `env` from both a subagent and the top-level
session's own shell — identical, including this var). Gating `sessionID()` on
it breaks normal top-level `mess` usage entirely without actually distinguishing
the two cases, so it was reverted. No environment variable Claude Code
currently exposes distinguishes "a subagent's tool call" from "the top-level
session's own tool call" — both get the same `CLAUDE_CODE_SESSION_ID` *and* the
same `CLAUDE_CODE_CHILD_SESSION` marker.

**Current status: unfixed.** There's no known way to close this from `mess`'s
side without a distinguishing signal Claude Code doesn't currently provide. If
you deliberately want a subagent to speak on `mess` as its own identity, use an
explicit `--as <name>` or `MESS_AGENT=<name>` in its invocation rather than
relying on ambient session-id resolution.

## Claude Code sometimes drops an `asyncRewake` delivery entirely

**Symptom:** an idle agent's `mess-wake.sh` correctly parks, receives a peer
message, and fully drains it (confirmed in the daemon log: `recv <agent> woke ->
drained N (peek; left queued)` followed by a non-peek `recv <agent> drained N`,
meaning the script reached its `exit 2` with the message printed to stderr) — but
the message never surfaces in the agent's actual transcript. No injected system
reminder, no `task-notification`, nothing. The agent has no idea a message
arrived until it happens to `mess recv` on its own (or `mess replay` a stale one).

This is **not** the documented working/steer handoff (where an actively busy
agent correctly stands down from waking and lets the mid-turn steer hook notify
instead — see the README's "Getting messages mid-turn" section). It's also not
a race with a concurrent user prompt: reproduced and ruled out below.

### Evidence (2026-07-03)

Agent `dwarf-main` (`/home/engi/git/dwarf`, session
`9902ace8-5f15-4aa8-9645-a48c17ba374e`) was genuinely idle when peer `trail-main`
sent it a direct message:

```
mess daemon log:
12:37:04  (Stop hook fires — dwarf-main goes idle, mess-wake.sh parks)
12:37:09  send trail-main -> dwarf-main | listening=true
12:37:09  recv dwarf-main woke -> drained 1 (peek; left queued)
12:37:10  recv dwarf-main drained 1        <- full, non-peek consume; script
                                               would print to stderr + exit 2
```

dwarf-main's own transcript (`.jsonl`) shows the Stop hook's synchronous summary
at `09:37:04.963Z` (`hookCount: 2`, includes `mess-wake.sh`, `hasOutput: false`
— exactly what's expected for a still-pending async hook). The **next** entry in
the transcript, **106 seconds later**, is an ordinary human-typed prompt
(`origin: human`, `promptSource: typed`) completely unrelated to trail-main.
Nothing in between.

Crucially: Claude Code's internal transcript also logs `{"type":
"queue-operation", "operation": "enqueue"/"dequeue"/...}` entries every time an
async hook's completion is turned into a visible `task-notification` turn. In
this same session, that pattern succeeded **60+ other times**, always as a clean
`enqueue` → `dequeue` pair a few milliseconds apart. For the trail-main wake at
`09:37:09`, there is **no `enqueue` entry anywhere** — the nearest ones are ~2
hours before and ~7 hours after. The background script did its job; Claude Code
never even attempted to turn that into a visible notification.

### Race with a concurrent user prompt — reproduced, but it's a different (correct) behavior

We separately reproduced a real race: sending a peer message to an agent at the
same moment a human types a fresh prompt into that agent's terminal. In that
case the wake script's own `working` check (see `mess-wake.sh`) correctly saw
the agent had just gone busy, stood down, and the mid-turn steer hook surfaced
the message instead — the agent still saw it (via `[mess] N unread...` +
`mess recv`), just through the documented fallback path. No data loss, just a
different, working delivery path. This is *not* what happened to dwarf-main —
there was no human input anywhere near its 106-second gap, and the daemon log
shows the wake script reached full completion (not a stand-down).

### Conclusion — confirmed upstream, root cause identified

This matches a known (if under-reported) Claude Code bug:
[anthropics/claude-code#39632](https://github.com/anthropics/claude-code/issues/39632),
"stream-json: background-task notification doesn't wake idle session (race in
`runAsyncAgentLifecycle`)". Traced there in v2.1.86-dev: `completeAgentTask()`
flips the task to "completed" (so `hasRunningBg` reads false) *before*
`enqueueAgentNotification()` runs — and two `await`s sit in between (a handoff
classification and a worktree lookup). `runHeadlessStreaming`'s idle-poll loop
checks `hasRunningBg || hasQueued` every 100ms; if a poll tick lands in that gap,
it sees both false, exits the loop, and goes idle *before* the notification is
enqueued. The later enqueue (priority `"later"`) does fire a queue-changed
signal, but the stream-json handler only aborts idle for priority `"now"` — so
it's never picked up until something else (a new user prompt) starts a fresh
turn. A follow-up comment on that issue reports a **23% drop rate** (3/13) for
async completions under concurrent load in a headless pipeline. The issue was
closed only by a stale-bot for inactivity — never fixed.

So: not a `mess` bug, not a race with a concurrent user prompt (ruled out
separately below) — it's this exact upstream timing hole. The daemon-level
mechanics (idle detection, park, wake, full drain) all worked exactly as
designed on the `mess` side.

### Mitigation

This is exactly the scenario `mess replay` exists for — the consumed message is
still recoverable from the per-agent replay buffer even if the harness drops the
injection. If a wake seems to silently not happen for a genuinely idle agent,
`mess replay` before assuming the message is lost. There is currently no way to
detect this failure automatically (the drop leaves no trace on the mess side by
design, since the message actually was delivered/drained from the daemon's point
of view) — it can only be caught after the fact by an operator noticing a peer
never responded.
