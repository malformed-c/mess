#!/usr/bin/env sh
# Stop-hook auto-wake for mess (asyncRewake). Parks on `mess recv --wait` and,
# when a wake-worthy peer message arrives WHILE IDLE, CONSUMES the inbox and
# prints the messages to stderr — which Claude Code injects into the woken turn
# as a system reminder (asyncRewake exit 2). So the woken agent sees the message
# content directly, with no separate `mess recv` needed, and acks fire.
#
# Converges with the mid-turn steer hook: if the agent is actively WORKING when
# the message lands, the wake stands down (leaves it queued) and the steer hook
# is the sole notifier. Consuming on an idle wake empties the inbox, so the woken
# turn's steer naturally has nothing to announce — no double-notify.
[ -n "$MESS_CHANNEL" ] && exit 0

MESS=/home/engi/.local/bin/mess
who=$("$MESS" whoami 2>/dev/null)
[ -z "$who" ] && exit 0

# Park (peek) until a wake-worthy (non-quiet, direct/topic) message arrives.
peek=$(flock -n "${TMPDIR:-/tmp}/mess-wake-$who.lock" "$MESS" recv --wait --no-broadcast --peek --json --batch 1s 2>/dev/null | jq -c 'select(.quiet != true)' 2>/dev/null)
[ -z "$peek" ] && exit 0 # timed out / consumed / only-quiet — nothing to wake for

# Active agent -> the steer hook surfaces it; leave the message queued.
working=$("$MESS" ps --json 2>/dev/null | jq -r --arg me "$who" '(.agents[] | select(.name==$me) | .working) // false')
[ "$working" = "true" ] && exit 0

# Idle: consume the inbox (direct + topic) and inject the messages. Broadcasts
# stay queued (read later). stderr is what asyncRewake delivers on exit 2.
drained=$("$MESS" recv --no-broadcast --json 2>/dev/null)
[ -z "$drained" ] && exit 0
n=$(printf '%s\n' "$drained" | grep -c .)
{
  printf '[mess] %s new peer message(s) (delivered on wake — no recv needed):\n' "$n"
  printf '%s\n' "$drained" | jq -r '"  \(.from)\(if .topic then " #\(.topic)" else "" end): \(.body)"'
} >&2
exit 2
