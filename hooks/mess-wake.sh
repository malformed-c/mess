#!/usr/bin/env sh
# Stop-hook auto-wake for mess (asyncRewake). Parks on `mess recv --wait` and
# wakes the agent (exit 2) when a wake-worthy peer message arrives WHILE IDLE.
#
# Converges with the mid-turn steer hook so the two never double-notify:
#   - If the agent is actively WORKING when the message lands, stand down — the
#     PreToolUse/UserPromptSubmit steer hook surfaces it instead.
#   - If the message was already announced (by steer or a prior wake), stand down.
#   - Otherwise wake, and drop the mess-woke flag so the woken turn's steer stays
#     quiet (it syncs the shared dedup and suppresses one notice).
# The shared dedup state (mess-steer-$who.id, newest announced message id) is what
# both hooks read, so a given message is announced exactly once.
[ -n "$MESS_CHANNEL" ] && exit 0

MESS=/home/engi/.local/bin/mess
who=$("$MESS" whoami 2>/dev/null)
[ -z "$who" ] && exit 0

# Park until a wake-worthy (non-quiet, direct/topic) peer message arrives.
json=$(flock -n "${TMPDIR:-/tmp}/mess-wake-$who.lock" "$MESS" recv --wait --no-broadcast --peek --json --batch 1s 2>/dev/null | jq -c 'select(.quiet != true)' 2>/dev/null)
[ -z "$json" ] && exit 0 # timed out / consumed / only-quiet — nothing to wake for

# Active agent -> let the steer hook be the sole notifier.
working=$("$MESS" ps --json 2>/dev/null | jq -r --arg me "$who" '(.agents[] | select(.name==$me) | .working) // false')
[ "$working" = "true" ] && exit 0

# Already announced (newest id not newer than what's been surfaced)? Don't re-wake.
maxid=$(printf '%s\n' "$json" | jq -rs 'if length==0 then 0 else ([.[].id | ltrimstr("m") | tonumber] | max) end')
[ -z "$maxid" ] && maxid=0
statef="${TMPDIR:-/tmp}/mess-steer-$who.id"
prev=$(cat "$statef" 2>/dev/null || echo 0)
[ "$maxid" -le "$prev" ] && exit 0

# New message, agent idle -> wake. Flag it so the woken turn's steer stays quiet.
touch "${TMPDIR:-/tmp}/mess-woke-$who"
exit 2
