# Known issues

## Fixed (2026-07-28): an empty wake was read as "stand down" → idle agents went permanently deaf

**Symptom:** an agent parks correctly, a peer sends, and the agent is never
woken. Minutes later a send to it logs `listening=false` — there is no parked
waiter at all, and none appears until the agent's next `Stop`, which for a
genuinely idle agent never comes. This is the single largest cause of "waking
up is unreliable", and it accounts for the "never parks at all" class of
symptom previously recorded (below) as inference about rapid `Stop` events.

**Root cause:** `parkAndDrain` with `--batch` sleeps the coalescing window
*before* it drains, so for a full second after committing to return, another
consumer can empty the inbox — a second wake-hook instance, or the agent's own
mid-turn `mess recv`. The park then returns zero messages. `mess-wake.sh` could
not tell that from "nothing to do" and ran `[ -z "$peek" ] && exit 0`; under
`asyncRewake`, exit 0 neither wakes nor re-arms.

Two wake-hook instances routinely coexisted despite the `flock`, because
`flock -n "$LOCK" mess recv --wait …` held the lock only for the *park* — the
busy-poll drain loop after it ran unlocked for as long as the agent stayed in
its turn (16 s in one logged case), and a `Stop` in that window started a second
instance that took the free lock and parked.

**Evidence:** 31 `woke -> drained 0` events in a five-week daemon log; 5 of the
9 most recent were followed within minutes by a `listening=false` send to that
same agent, e.g.

```
09:27:23  recv periapsis-opus drained 1 (if-idle)      <- instance A takes it
09:27:23  recv periapsis-opus woke -> drained 0        <- instance B gets nothing, exits 0
09:31:02  send periapsis-fable -> periapsis-opus | pending=1 listening=false
```

**Fix:**
1. **daemon**: an empty blocking recv now says *why* — `Response.Reason` is
   `raced` (woke on real mail, another consumer drained it first), `timeout`, or
   `evicted`. The CLI turns those into exit codes 75/76/77 for `recv --wait`.
2. **mess-wake.sh**: re-parks on `raced` (and on a wake that turned up only
   quiet mail) instead of exiting; holds its `flock` for the whole script
   lifetime, so the "one waiter per agent" invariant actually covers the drain
   loop. An unclassified empty return (daemon older than the CLI) also re-parks,
   guarded against spinning.

**Tests:** `TestBlockingRecvReports{Raced,Timeout,Evicted}` and
`TestBlockingRecvHasNoReasonWhenItDelivers` (daemon_test.go);
`TestWakeHookRepArksAfterLosingTheDrainRace` and
`TestWakeHookRepArksWhenItsWakeLeavesOnlyQuietMail` (hooks_test.go, which drives
the real scripts against a throwaway daemon). All fail against the previous
hooks and pass after.

## Fixed (2026-07-28): every mess error looked exactly like "no mail"

**Symptom:** an agent stops receiving entirely — no wake, no mid-turn notice —
with no indication anything is wrong. `mess whoami` still reports the right
name, so the agent has no reason to suspect it is cut off.

**Root cause:** both hooks sent every error to `/dev/null` and treated the
resulting empty output as an empty inbox. `whoami` reads a local identity file,
so it keeps working even when the daemon refuses every *operation* — e.g. the
ownership guard rejecting `name "X" is in use by another live session`, which is
exactly what an orphaned waiter (below) provokes for a restarted session.
Reproduced: mail pending, `whoami` → the right name, steer prints nothing, wake
exits 0 without parking.

**Fix:** `mess-wake.sh` reports an outage as a *wake* (exit 2) so it is actually
seen, and `mess-steer.sh` emits it as a notice instead of staying silent. Both
fire once per episode via a marker file cleared on recovery — reporting
unconditionally would loop, since a rewake ends in another `Stop` that re-runs
the hook into the same error.

**Test:** `TestSteerHookReportsAnOutageInsteadOfLookingEmpty`.

## Fixed (2026-07-28): the mid-turn notice was fire-and-forget, so one dropped injection lost the message

**Symptom:** mid-turn `[mess] N unread…` notices are unreliable — a message
arrives while the agent is working and is simply never mentioned.

**Root cause:** `mess-steer.sh` advanced its "highest id already announced"
watermark the moment it *printed* the notice, with no confirmation the agent
ever saw it. If Claude Code dropped or ignored that `additionalContext` (or the
tool call was denied), that message was never announced again — verified: a
second `PreToolUse` with the same message still unread printed nothing.

**Fix:** dedup is now time-bounded as well as id-bounded — still-unread mail is
re-announced at most once per `RENOTIFY` window (60 s), so a drop costs one
window instead of the message. Also hardened `maxid`: it used
`ltrimstr("m") | tonumber` across the whole batch, so one unparseable id made jq
error out and suppressed the notice for *every* message in that batch;
`tonumber?` now skips the offender instead. (Not reachable today — all message
ids are `m<N>` — but it was one id-format change from silencing the hook
entirely.)

**Test:** `TestSteerHookReAnnouncesStillUnreadMail`.

## Fixed (2026-07-21): Grok Stop armed asyncRewake before `unbusy` → wake lost on pending mail

**Symptom:** Grok session registers, sometimes parks, but a peer `mess send` while
idle (or mail already queued at turn end) never re-enters the agent. Inbox stays
pending; `mess islistening` is false after a brief park.

**Root cause:** On `Stop`, Grok armed `asyncRewake` *before* running sync Stop
hooks. `mess-wake.sh` peeks mail then drains with `recv --if-idle`. If mail was
already queued and busy was still set (unbusy not run yet), drain returned
`{"busy":true}` and the script **exited 0** — which does not rewake and does
not re-arm. Mid-turn stand-down is intentional, but exit-0 on busy at arm time
is permanent un-park until the next human Stop (which hit the same race).

**Fix:**
1. **grok-build** (`hook_dispatch.rs`): run sync Stop hooks first, then arm
   asyncRewake. Commit after `24884fe` on `async-rewake`.
2. **mess-wake.sh**: on busy, poll until idle and drain — never exit 0 solely
   because the agent is mid-turn (belt-and-suspenders for arm-order races and
   hosts that still arm early).

**After upgrade:** install new `grok-from-source`, restart the Grok session so
the new arm order is live. mess-wake.sh is picked up on next Stop without
restart.

## Fixed (2026-07-18): Grok Build tool shells lacked `GROK_SESSION_ID` → mess auto-wake never parked

**Symptom:** Grok sessions could `mess register` and appear in `mess ps`, but peers'
`mess send` always saw `listening=false`; `mess ask` timed out; `--ack` never
completed. Claude Code agents on the same host woke normally.

**Root cause:** Grok injects `GROK_SESSION_ID` into **hook** processes (Stop /
PreToolUse / SessionStart) but **not** into tool/bash shells. `mess` keys
mid-session identity on the host session id (`GROK_SESSION_ID` among others).
Agents then either failed `mess register` or invented a fake
`MESS_SESSION_ID=grok-build-game-$$` that did not match the real UUID the
Stop-hook `mess-wake.sh` uses. Result: wake script's `mess whoami` was empty →
exit 0 without parking → no listener.

**Fix (in grok-build, not mess):** inject `GROK_SESSION_ID` from
`owner_session_id` into every agent tool shell (persistent + non-persistent),
exclude it from shell-state dumps like `GROK_AGENT`, re-export after snapshot
restore. mess already accepted `GROK_SESSION_ID` in `sessionEnvVars`; error
messages updated to mention it. New Grok binary must be installed and sessions
restarted for tool shells to pick it up.

**Landed:** grok-build branch `async-rewake`:
- `0ce88e6` — Claude-compatible `asyncRewake` Stop hooks (park + exit-2 synthetic turn)
- `24884fe` — inject `GROK_SESSION_ID` into tool shells (this issue)
Installed as `~/.grok/downloads/grok-from-source` (`grok 0.1.220-alpha.4 (24884fe)`).
Stock `0.2.101` still lacks both.

**After upgrade:** re-`mess register <name>` once in each Grok session (or copy
identity from any fake key onto the real session UUID under `~/.mess/ident/`).

## Fixed (plausible root cause): auto-wake hook could steal a message out from under a turn that just started, leaving `mess recv` to find nothing

**Symptom, as reported (2026-07-09, habr-editor):** a peer sent a long
multi-paragraph message; the mid-turn steer hook correctly announced "1
unread"; the very next `mess recv` printed nothing (exit 0); a later `mess
recv` also found nothing; `mess replay` recovered the full message, proving
it was delivered and consumed, just never seen via `recv`.

**Root cause (plausible, not conclusively reproduced against the exact
incident — see caveat below):** `hooks/mess-wake.sh` decided whether to drain
an idle agent's inbox via *two separate, independent round trips*: a `mess ps`
call to check `working`, and — only if that came back false — a *separate*
`mess recv` call to actually drain. There was a real gap between them. If a
new turn started (`mess busy` fired, e.g. from a concurrent human prompt or a
parallel tool call) in that gap — after the `ps` check returned "not busy" but
before the drain ran — the wake hook would still drain the message, believing
the agent was idle, and hand it to its own stderr injection instead of the
turn's own `mess recv`. That injection is exactly the kind of async delivery
Claude Code can drop (see the `asyncRewake` section below) — so the message
could vanish from the agent's view entirely while still being genuinely
consumed (hence recoverable via `replay`, which reads consumed history
regardless of who did the consuming).

**Fix:** added `Broker.DrainIfIdle` — checks `busyUntil` and drains in the
*same lock acquisition*, so there's no gap between "is this agent busy" and
"drain its inbox" for a concurrent `mess busy` to land in. Exposed as `mess
recv --if-idle` (non-blocking only); `mess-wake.sh` now uses it instead of the
old separate `ps`-then-`recv` sequence.

**Caveat:** this closes a real, verified TOCTOU race (see
`TestDrainIfIdleStandsDownWhenBusy`/`TestDrainIfIdleClearedBusyStillDrains` in
broker_test.go, and the hand-driven busy/idle reproduction against a
throwaway daemon in this fix's own testing), and it's a plausible match for
every symptom in the report. It was not reproduced against the *exact*
reported incident (that would need precise timing of a real concurrent turn
start, which is impractical to force deterministically) — so treat this as
the most likely structural cause found and closed, not a confirmed root
cause with a before/after repro of the original failure.

## Fixed: parallel tool calls could make the steer hook double-notify (or notify for already-consumed mail)

**Symptom, as reported (2026-07-07/08):** a duplicate/"reformatted" wake
notice for the same message, and separately a `[mess] 1 unread peer
message(s)` notice for mail that a `mess recv` moments earlier (same turn)
had already fully consumed — a follow-up `mess recv` right after showed zero
unread, confirming there was nothing left to report.

**Root cause:** `hooks/mess-steer.sh` (the `PreToolUse`/`UserPromptSubmit`
mid-turn notifier) dedups on a "highest message id already announced" marker
persisted in `${TMPDIR}/mess-steer-<agent>.id`, but read-checked-wrote it with
no locking. Claude Code can dispatch several tool calls from one turn in
parallel, each with its own `PreToolUse`, so multiple instances of the script
can run at the same moment — e.g. one of the parallel calls is itself `mess
recv`. Without a lock, two (or more) instances all read the same stale
`prev` before any of them writes, so all of them independently decide "this
id is new" and all fire the same notice — including one firing for a message
a sibling call is about to (or just did) consume via its own `mess recv`,
which reads to the agent as a stale/redundant notification for mail it
already fetched.

**Reproduced (2026-07-08):** launched 5 concurrent invocations of the script
against a single new message with no lock — all 5 printed the identical
notice. With the lock added, the same test produces exactly 1.

**Fix:** wrapped the read-check-write of the dedup marker in a `flock` (same
pattern `mess-wake.sh` already used for its own park call), so only one
instance of a simultaneous batch ever announces a given message id — see
`hooks/mess-steer.sh`. `mess-wake.sh`'s own park step was already
lock-guarded; its drain step wasn't re-examined further since draining is
destructive and daemon-serialized, so two wake-hook instances can't both
print the same message's content (whichever drains first empties the inbox
for the other).

## Fixed (2026-07-28, wake-hook half): an orphaned wake-hook process can make a dead session look falsely online

**Symptom:** `mess ps` reports an agent `online`/`listening` (or `working`) long
after its actual Claude Code session has exited — the human sees the terminal
is gone, but mess still thinks the agent is reachable.

**Confirmed (2026-07-05):** `breeze-notify-test` showed `online working` in
`mess ps` after its session had already ended. The parked wake process was
still alive and holding its lock:

```
ps ancestry for the parked `flock ... mess recv --wait` process:
  flock (2697367) -> sh mess-wake.sh (2697364) -> sh (2697345) -> systemd --user (1025)
```

No `claude` process anywhere in that chain — it's been reparented straight to
the user's systemd, i.e. orphaned. The actual session died (crash, killed
terminal, etc.) without a clean `Stop` event, so nothing ever killed its
`asyncRewake` background hook (`mess-wake.sh`'s parked `mess recv --wait`
child). That process is independent of the parent once spawned, so it keeps
running, keeps holding the flock, and keeps parking/waking on `mess recv
--wait` — with nothing behind it to actually receive or act on an injection
even if a message arrives (there's no live Claude Code turn to inject into).

Separately, `Working` can also read stale: `mess busy` defaults to a **1 hour**
backstop TTL (`cmdBusy` in `main.go`), refreshed on every `UserPromptSubmit`/
`PreToolUse`. If a session dies without the `Stop`-hook's `mess unbusy` firing,
`busyUntil` stays in the future for up to an hour, and `aliveLocked` treats
"busy in the future" as alive — so a genuinely dead session can show `working`
for up to an hour with no process behind it at all, independent of the orphaned
wake-process issue above.

**Why this happens:** there's no `SessionEnd` hook wired up (only `SessionStart`,
`UserPromptSubmit`, `PreToolUse`, `Stop`, `StopFailure` — see the README's hooks
section), so an unclean exit (closed terminal, crash, kill) never runs `mess
unregister`/`mess unbusy`. `aliveLocked` (`broker.go`) has three ways to call an
agent alive — `listeners[name] > 0`, `busyUntil` in the future, or `lastSeen`
within 2 minutes — and an orphaned wake process or a not-yet-expired busy TTL
each independently satisfy one of those with nothing real behind it.

**Confirmed still live (2026-07-28):** three parked wake hooks reparented to
`systemd --user` with no `claude` anywhere in their ancestry — two of them 2d6h
old. `mess ps` showed `cp-backend online listening 3 pending (oldest 12h)`:
senders saw `listening=true` and believed their wake had landed, with nothing
behind it. Worse, because the park was taken with `flock -n`, a *restarted*
session under the same name could never park either — it lost the lock race and
exited 0 — and every daemon op it tried was refused as "in use by another live
session", which both hooks then swallowed as "no mail" (see the silent-error
entry above).

**Fixed (wake-hook half):** `mess-wake.sh` now walks up at startup to the
nearest host-agent ancestor (`claude`/`node`/`grok`) and remembers it, then
bounds each park (`PARK_TIMEOUT`, 15m) so it re-checks that session between
parks — re-parking immediately while it lives, standing down once it is gone.
comm is compared as well as pid, so a recycled pid isn't mistaken for the
session. If no host-agent ancestor can be identified (unknown harness, no
procfs) it never stands down, so this can only ever retire a waiter that really
was orphaned. Test: `TestWakeHookStandsDownWhenItsSessionDies`.

**Still unfixed (the rest):** `mess busy`'s 1-hour backstop TTL can still make a
dead session read `working` for up to an hour, and there is still no `SessionEnd`
hook running `unregister`/`unbusy` on an unclean exit. `aliveLocked`'s three
independent liveness signals (listener count, `busyUntil`, `lastSeen`) remain a
heuristic rather than a real probe. `mess rm`/`cleanup` are still the manual
remedy.

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

**Also tried: process-tree / PID-based detection — also a dead end.** Compared
full process ancestry between a top-level Bash tool call and a subagent's Bash
tool call (`ps`/`/proc/<pid>` walk up from `$$`). Both are direct children of
the *exact same* `claude` process (identical PID at every level of the
ancestry chain, e.g. `1902866` in one test run) — Claude Code runs subagents
in-process and forks tool-execution children flatly, not as a separate process
subtree per subagent. So there's no PID to bind identity to that distinguishes
them either:
- The `claude` process's own PID is identical for both parent and subagent —
  same problem as the session id itself.
- The immediate shell PID of each individual Bash tool call *is* different
  per-call (confirmed: two different PIDs for a top-level call vs. a subagent
  call) — but it's *also* different across the top-level session's own
  successive Bash calls (each tool call appears to get a fresh shell process).
  Binding identity to that PID would break identity persistence across a
  session's own ordinary turns, not just fail to stop subagents.

**Current status: unfixed.** There's no known way to close this from `mess`'s
side without a distinguishing signal Claude Code doesn't currently provide —
neither environment variables nor process ancestry expose one. If you
deliberately want a subagent to speak on `mess` as its own identity, use an
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

### A second, distinct failure mode: never parks at all (2026-07-05)

The case above (`dwarf-main`) parked, fully drained, and still dropped the
injection. A second confirmed instance shows the async wake failing *earlier*
— it never even parks:

Agent `peri-sonnet-5` (`/home/engi/git/apsis-io/periapsis`) had a rapid run of
turns (multiple `Stop` events within seconds of each other, `11:10`–`11:14`
UTC), ending with a clean `Stop` at `11:18:44` (`stop_hook_summary`,
`hasOutput: false` — the expected shape for a still-pending async hook,
identical to every other successful case in this session). `claude-verify`
then sent it a direct message at `11:22:26` — but the daemon log shows
`listening=false` at that exact moment:

```
11:18:44  (Stop hook fires — mess-wake.sh dispatched, hasOutput: false)
11:22:26  send claude-verify -> peri-sonnet-5 | recipient pending=4 listening=false
```

`listening=false` means no parked waiter existed at all — contrast with
`dwarf-main`, where `listening=true` and the daemon fully drained the message
(`woke -> drained N` then a non-peek `drained N`). Here there's nothing to
drain because nothing ever parked. peri-sonnet-5's transcript between
`11:18:44` and the next activity (`11:22:45`, a human typing `"can you recv"` —
coincidental, unrelated to the pending mail) contains zero mess-related
entries: no `queue-operation`, no task-notification, nothing. Checked
`/tmp/mess-wake-peri-sonnet-5.lock` for an orphaned holder (the
`breeze-notify-test` pattern from the section above) — none found; this isn't
that.

So: the `Stop` hook fired and dispatched `mess-wake.sh` as expected, but that
invocation apparently never got as far as actually parking on `recv --wait`.
Given the immediately preceding burst of rapid-fire `Stop` events on the same
session, a plausible trigger is Claude Code's async-hook lifecycle tracking
losing or overwriting one under rapid succession — but this is inference, not
confirmed the way the `dwarf-main` root cause was. Recorded here as a second,
structurally distinct symptom of the same general instability (upstream,
outside `mess`'s control) rather than a new investigation.

### Mitigation

This is exactly the scenario `mess replay` exists for — the consumed message is
still recoverable from the per-agent replay buffer even if the harness drops the
injection. If a wake seems to silently not happen for a genuinely idle agent,
`mess replay` before assuming the message is lost. There is currently no way to
detect this failure automatically (the drop leaves no trace on the mess side by
design, since the message actually was delivered/drained from the daemon's point
of view) — it can only be caught after the fact by an operator noticing a peer
never responded.
