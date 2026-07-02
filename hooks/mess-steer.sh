#!/usr/bin/env bash
# PreToolUse steer hook for `mess` — DEFAULT ON.
#
# Injects a small "N unread peer message(s)" NOTICE into the RUNNING turn (as
# additionalContext) so the agent learns mid-turn — at its next tool call — that
# peers have messaged it, instead of finding out only at the next idle auto-wake.
# It does NOT dump message bodies; the agent reads them with `mess recv`.
#
# Scope: fires for any session that has a mess identity. No-op for non-mess
# sessions. Opt out with MESS_NO_STEER=1. Stands down under MESS_CHANNEL (a
# channel session delivers messages itself). Messages are peeked (not consumed)
# so `mess recv` still returns them, and the notice only fires when the unread
# count has grown, so it appears once per new batch rather than every tool call.
[ -n "$MESS_NO_STEER" ] && exit 0
[ -n "$MESS_CHANNEL" ] && exit 0

MESS=/home/engi/.local/bin/mess
who=$("$MESS" whoami 2>/dev/null)
[ -z "$who" ] && exit 0

# Count pending direct/topic messages without consuming them (broadcasts ignored).
n=$("$MESS" recv --kind direct,topic --peek --json 2>/dev/null | grep -c .)

statef="${TMPDIR:-/tmp}/mess-steer-$who.n"
prev=$(cat "$statef" 2>/dev/null || echo 0)
printf '%s' "$n" > "$statef"

if [ "$n" -gt 0 ] && [ "$n" -gt "$prev" ]; then
  jq -cn --arg c "[mess] $n unread message(s) from peers — run \`mess recv\` to read them." \
    '{hookSpecificOutput:{hookEventName:"PreToolUse",additionalContext:($c)}}'
fi
exit 0
