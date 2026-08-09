#!/usr/bin/env sh
# Stop-hook auto-wake for mess (asyncRewake).
# Parks on `mess recv --wait`. On a wake-worthy peer message while idle,
# consumes the inbox and prints messages to stderr (asyncRewake exit 2).
#
# The governing rule: exit 0 un-parks this agent until its next Stop, and for a
# genuinely idle agent that Stop never comes — so exit 0 ONLY when standing down
# is actually correct (evicted, session gone, another instance holds the park).
# Every "nothing to deliver right now" case goes back to parking instead.
#
# If the agent is working when mail arrives, do NOT exit 0 (that un-parks
# forever until the next Stop). Poll until idle and drain, so a turn that
# ends with mail already queued — or a host that arms rewake before unbusy —
# still wakes. Mid-turn steer remains the in-turn notifier; we only deliver
# once --if-idle succeeds.
[ -n "$MESS_CHANNEL" ] && exit 0

# Shared preamble: resolves MESS and `who` (see mess-common.sh). Sourcing it
# rather than repeating it is what stops the four hooks drifting apart again.
# Guard with -r rather than `. ... || exit 0`: a failed `.` is FATAL in POSIX
# sh, so the || never runs and a missing preamble would take the hook down
# noisily instead of standing it down.
_common="$(dirname "$0")/mess-common.sh"
[ -r "$_common" ] || exit 0
. "$_common"
[ -z "$who" ] && exit 0

STATE="${TMPDIR:-/tmp}/mess-wake-$who"
LOCK="$STATE.lock"
ERRF="$STATE.err"     # marks an outage already reported, so it reports once
ERRTMP="$STATE.stderr"

# How long a single park lasts before we re-check that the session is still
# alive and park again. Bounded only for that liveness check — a live agent
# re-parks immediately, so it is still continuously listening.
PARK_TIMEOUT=${MESS_WAKE_PARK_TIMEOUT:-15m}

# One waiter per agent, for this script's WHOLE lifetime. The old form
# (flock -n "$LOCK" mess recv --wait ...) released the lock the moment the park
# returned, leaving the busy-poll drain loop below unlocked for as long as the
# agent stayed in its turn. A Stop during that window started a second instance
# that took the free lock and parked, and whichever instance drained first left
# the other with an empty wake — the "raced" case handled below.
exec 9>"$LOCK" || exit 0
flock -n 9 || exit 0   # a live park already holds it; that instance is the waiter

# Orphan guard. When a session dies without a clean Stop (crash, closed
# terminal), this background hook is reparented to init/systemd and would
# otherwise park forever: holding the lock so the agent's next session can
# never park, and keeping a phantom `listening` in `mess ps` that makes senders
# believe their wake landed. `mess session-pid` resolves the host agent process
# we belong to — the same walk the daemon uses to probe liveness, asked for
# rather than re-implemented here, so the two can't drift. If it can't identify
# one (unknown harness, no procfs) we never stand down, so this can only ever
# retire a waiter that really was orphaned. The clean path is the SessionEnd
# hook, which evicts the waiter immediately; this is the crash backstop.
session_pid="${MESS_WAKE_SESSION_PID:-$("$MESS" session-pid 2>/dev/null)}"
session_comm=$(cat "/proc/$session_pid/comm" 2>/dev/null)

# session_alive re-reads comm rather than just checking the pid exists, so a
# recycled pid isn't mistaken for the session that launched us.
session_alive() {
  [ -z "$session_comm" ] && return 0
  [ "$(cat "/proc/$session_pid/comm" 2>/dev/null)" = "$session_comm" ]
}

# stand_down reports a mess outage to the agent as a wake (exit 2), so a broken
# auto-wake is visible instead of looking exactly like "no mail" — every error
# path here used to be swallowed by 2>/dev/null and exit 0. The marker file
# makes it fire ONCE per episode: a rewake ends in another Stop, which re-runs
# this hook into the same error, so reporting unconditionally would loop
# forever. A successful park clears the marker.
stand_down() {
  if [ ! -f "$ERRF" ]; then
    : > "$ERRF"
    {
      printf '[mess] auto-wake stood down — peer messages will NOT wake this session until it recovers.\n'
      printf '[mess] reason: %s\n' "$1"
      printf '[mess] check `mess whoami` and `mess ps`; run `mess recv` manually meanwhile.\n'
    } >&2
    exit 2
  fi
  exit 0
}

# Outer loop: park until mail, drain when idle, re-park whenever there was
# nothing to hand over (raced, all-quiet, or consumed by a mid-turn recv).
spins=0
while true; do
  # Park until a wake-worthy non-quiet direct/topic message arrives.
  # --no-broadcast is the wake filter; loud broadcasts still unblock server-side.
  t0=$(date +%s)
  raw=$("$MESS" recv --wait --no-broadcast --peek --json --batch 1s "$PARK_TIMEOUT" 2>"$ERRTMP")
  case $? in
    0) rm -f "$ERRF" ;;                                   # delivered something
    75) continue ;;                                       # raced: another consumer drained it — still our park
    76) session_alive && continue                         # park expired: re-park if the session is still there
        exit 0 ;;                                         # session gone — release the lock and the phantom listener
    77) exit 0 ;;                                         # evicted (removed/renamed) — stop cleanly
    *) stand_down "$(head -3 "$ERRTMP" 2>/dev/null || echo 'mess recv --wait failed')" ;;
  esac

  # Empty with no reason: a daemon older than this CLI doesn't classify its
  # empty returns. Re-parking is the safe reading — standing down is precisely
  # what left agents unwakeable. The only thing to guard against is a park that
  # returns *instantly*, which would spin rather than wait.
  if [ -z "$raw" ]; then
    if [ "$(($(date +%s) - t0))" -lt 2 ]; then
      spins=$((spins + 1))
      [ "$spins" -ge 5 ] && stand_down "blocking recv returned instantly and empty 5x (daemon older than this CLI?)"
    else
      spins=0
    fi
    continue
  fi
  spins=0

  # Everything we woke on was quiet (delivered without notifying) — nothing to
  # wake for, but the park is still ours, so go back to waiting.
  peek=$(printf '%s\n' "$raw" | jq -c 'select(.quiet != true)' 2>/dev/null)
  [ -z "$peek" ] && continue

  # Idle-only drain. If busy, poll — never exit 0 on busy (that was the Grok
  # wake-loss bug: arm-before-unbusy + pending mail → permanent un-park).
  while true; do
    has_loud=$(printf '%s\n' "$peek" | jq -s 'map(.loud == true) | any')
    if [ "$has_loud" = "true" ]; then
      resp=$("$MESS" recv --if-idle --json 2>/dev/null)
    else
      resp=$("$MESS" recv --if-idle --no-broadcast --json 2>/dev/null)
    fi

    if printf '%s\n' "$resp" | grep -qx '{"busy":true}'; then
      # Still in a turn. Brief sleep then re-check; if mid-turn recv ate the
      # mail, go back to the outer wait instead of spinning.
      sleep 0.5
      peek=$("$MESS" recv --peek --no-broadcast --json 2>/dev/null \
        | jq -c 'select(.quiet != true)' 2>/dev/null)
      [ -z "$peek" ] && break
      continue
    fi

    drained="$resp"
    [ -z "$drained" ] && break

    n=$(printf '%s\n' "$drained" | grep -c .)
    {
      printf '[mess] %s new peer message(s) (delivered on wake - no recv needed):\n' "$n"
      printf '%s\n' "$drained" | jq -r \
        '"  " + (if .ask then "[QUESTION \(.id) - answer with mess reply, or @mention the asker] " else "" end) + .from + (if .topic then " #\(.topic)" else "" end) + ": " + .body'
    } >&2
    exit 2
  done
done
