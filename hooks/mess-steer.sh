#!/usr/bin/env bash
# PreToolUse steer hook for `mess`.
#
# When MESS_STEER is set on the session, this injects a small "N unread peer
# message(s)" NOTICE into the RUNNING turn (as additionalContext), so the agent
# learns mid-turn — at its next tool call — that peers have messaged it, instead
# of finding out only at the next idle auto-wake. It does NOT dump the message
# bodies; the agent reads them itself with `mess recv`.
#
# Opt-in: launch the session with `MESS_STEER=1`. Messages are peeked (not
# consumed) so `mess recv` still returns them. To avoid repeating the notice on
# every tool call, it only fires when the unread count has grown since last time.
[ -z "$MESS_STEER" ] && exit 0

MESS=/home/engi/.local/bin/mess
who=$("$MESS" whoami 2>/dev/null)
[ -z "$who" ] && exit 0

# Count pending direct/topic messages without consuming them (broadcasts ignored).
n=$("$MESS" recv --kind direct,topic --peek --json 2>/dev/null | grep -c .)

statef="${TMPDIR:-/tmp}/mess-steer-$who.n"
prev=$(cat "$statef" 2>/dev/null || echo 0)
printf '%s' "$n" > "$statef"

# Only announce when there are unread messages AND the count grew (new arrivals),
# so the notice appears once per new batch rather than on every tool call.
if [ "$n" -gt 0 ] && [ "$n" -gt "$prev" ]; then
  jq -cn --arg c "[mess] $n unread message(s) from peers — run \`mess recv\` to read them." \
    '{hookSpecificOutput:{hookEventName:"PreToolUse",additionalContext:$c}}'
fi
exit 0
