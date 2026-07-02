#!/usr/bin/env sh
# PreToolUse steer hook for `mess` — DEFAULT ON. POSIX sh.
#
# Injects a small "N unread peer message(s)" NOTICE into the RUNNING turn (as
# additionalContext) so the agent learns mid-turn — at its next tool call — that
# peers have messaged it, instead of finding out only at the next idle auto-wake.
# It does NOT dump message bodies; the agent reads them with `mess recv`.
#
# Dedup is by newest message id (monotonic), not count: a genuinely new message
# always has a higher id, so this fires once per new arrival and never misses one
# just because the unread count happened to match after a recv. The notice is
# phrased "as of this tool call" because additionalContext is sticky (saved to
# the transcript), so a lingering line reads as a point-in-time event.
#
# Scope: fires for any session that has a mess identity. No-op for non-mess
# sessions. Opt out with MESS_NO_STEER=1. Stands down under MESS_CHANNEL (a
# channel session delivers messages itself). Messages are peeked (not consumed)
# so `mess recv` still returns them. It also stands down right after the auto-wake
# hook has already prompted a recv, so the two don't double-announce one batch.
[ -n "$MESS_NO_STEER" ] && exit 0
[ -n "$MESS_CHANNEL" ] && exit 0

# The hook event this fires on (PreToolUse before a tool, or UserPromptSubmit on
# a user message). additionalContext's hookEventName must match. Default keeps
# older single-arg installs working.
EVENT="${1:-PreToolUse}"
case "$EVENT" in
  PreToolUse) at="this tool call" ;;
  UserPromptSubmit) at="this prompt" ;;
  *) at="now" ;;
esac

MESS=/home/engi/.local/bin/mess
who=$("$MESS" whoami 2>/dev/null)
[ -z "$who" ] && exit 0

# Peek pending direct/topic messages (broadcasts ignored), dropping quiet ones
# (a topic message that @-mentioned other subscribers, not me); derive count + id.
json=$("$MESS" recv --kind direct,topic --peek --json 2>/dev/null | jq -c 'select(.quiet != true)' 2>/dev/null)
n=$(printf '%s\n' "$json" | grep -c .)
maxid=$(printf '%s\n' "$json" | jq -rs 'if length==0 then 0 else ([.[].id | ltrimstr("m") | tonumber] | max) end' 2>/dev/null)
[ -z "$maxid" ] && maxid=0

statef="${TMPDIR:-/tmp}/mess-steer-$who.id"
prev=$(cat "$statef" 2>/dev/null || echo 0)

# (The auto-wake hook consumes on an idle wake, so a woken turn's inbox is empty
# here — no flag coordination needed. When the agent is working, the wake stands
# down and this hook is the sole notifier.)
if [ "$n" -gt 0 ] && [ "$maxid" -gt "$prev" ]; then
  jq -cn --arg c "[mess] $n unread peer message(s) as of $at — run \`mess recv\` to read them." \
    --arg ev "$EVENT" \
    '{hookSpecificOutput:{hookEventName:$ev,additionalContext:($c)}}'
  printf '%s' "$maxid" > "$statef"
fi
exit 0
