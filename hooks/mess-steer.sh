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
# Dedup is also time-bounded (RENOTIFY): while the same mail stays unread we
# re-announce at most once per window. Watermarking purely on id made this
# fire-and-forget — the marker advanced the moment the notice was *printed*,
# with no confirmation the agent saw it, so a single dropped injection meant
# that message was never mentioned again. Re-announcing bounds a drop to one
# window instead of losing it outright, without spamming every tool call.
#
# Scope: fires for any session that has a mess identity. No-op for non-mess
# sessions. Opt out with MESS_NO_STEER=1. Stands down under MESS_CHANNEL (a
# channel session delivers messages itself). Messages are peeked (not consumed)
# so `mess recv` still returns them. It also stands down right after the auto-wake
# hook has already prompted a recv, so the two don't double-announce one batch.
[ -n "$MESS_NO_STEER" ] && exit 0
[ -n "$MESS_CHANNEL" ] && exit 0

# Grok Build: map hook-injected GROK_SESSION_ID so whoami resolves mid-turn.
if [ -z "$MESS_SESSION_ID" ] && [ -n "$GROK_SESSION_ID" ]; then
  export MESS_SESSION_ID="$GROK_SESSION_ID"
fi

# The hook event this fires on (PreToolUse before a tool, or UserPromptSubmit on
# a user message). additionalContext's hookEventName must match. Default keeps
# older single-arg installs working.
EVENT="${1:-PreToolUse}"
case "$EVENT" in
  PreToolUse) at="this tool call" ;;
  UserPromptSubmit) at="this prompt" ;;
  *) at="now" ;;
esac

MESS=${MESS_BIN:-/home/engi/.local/bin/mess}  # MESS_BIN lets the tests drive a throwaway build
who=$("$MESS" whoami 2>/dev/null)
[ -z "$who" ] && exit 0

RENOTIFY=${MESS_STEER_RENOTIFY:-60}  # seconds; re-announce still-unread mail at most this often

statedir="${TMPDIR:-/tmp}"
errf="$statedir/mess-steer-$who.err"  # marks an outage already reported

# emit prints one additionalContext notice. Anything this hook says to the
# agent goes through here, so there is exactly one JSON object on stdout.
emit() {
  jq -cn --arg c "$1" --arg ev "$EVENT" \
    '{hookSpecificOutput:{hookEventName:$ev,additionalContext:$c}}'
}

# Peek pending direct/topic messages (broadcasts ignored except a --loud one,
# which is meant to surface even to a busy agent), dropping quiet ones (a topic
# message that @-mentioned other subscribers, not me); derive count + id.
#
# A failed peek must NOT look like an empty inbox. Swallowing the error (the old
# 2>/dev/null) made every mess outage silently indistinguishable from "no mail":
# `mess whoami` keeps working off a local file even when the daemon refuses
# every operation (e.g. the name is held by another live session), so the agent
# had no signal at all that it had stopped receiving. Report it once per episode.
direct_raw=$("$MESS" recv --kind direct,topic --peek --json 2>"$statedir/mess-steer-$who.stderr")
if [ $? -ne 0 ]; then
  if [ ! -f "$errf" ]; then
    : > "$errf"
    emit "[mess] can't read your inbox — peer messages may not be reaching you: $(head -1 "$statedir/mess-steer-$who.stderr" 2>/dev/null). Check \`mess whoami\` and \`mess ps\`."
  fi
  exit 0
fi
rm -f "$errf"

direct=$(printf '%s\n' "$direct_raw" | jq -c 'select(.quiet != true)' 2>/dev/null)
loud=$("$MESS" recv --kind broadcast --peek --json 2>/dev/null | jq -c 'select(.loud == true)' 2>/dev/null)
json=$(printf '%s\n%s\n' "$direct" "$loud" | sed '/^$/d')
n=$(printf '%s\n' "$json" | grep -c .)
# tonumber? skips any id that isn't m<N> instead of erroring out. The old form
# aborted the whole computation on one unparseable id, leaving maxid empty ->
# 0 -> no notice at all, suppressing the notice for every message in the batch.
maxid=$(printf '%s\n' "$json" | jq -rs '[.[].id | tostring | ltrimstr("m") | tonumber?] | max // 0' 2>/dev/null)
[ -z "$maxid" ] && maxid=0
[ "$n" -eq 0 ] && exit 0
# Call out any pending `mess ask` roots distinctly — an answer only satisfies
# the asker's wait if it's threaded (`mess reply`) or @mentions them, so a plain
# unmentioning send back leaves them blocking. This notice is the one place a
# busy agent (not seeing the wake hook's fuller injection) would otherwise miss
# that.
askn=$(printf '%s\n' "$json" | jq -s 'map(select(.ask == true)) | length')
asknote=""
if [ "${askn:-0}" -gt 0 ]; then
  asknote=" ($askn of them a question — answer with \`mess reply\`, or @mention the asker)"
fi

# Claude Code can dispatch several tool calls from one turn in parallel (each
# with its own PreToolUse), so two instances of this script can run at the same
# moment — e.g. one of the parallel calls is itself `mess recv`. Without a lock,
# both instances can read the same stale prev before either writes, so both fire
# the same notice — one of them for a message the *other* call is about to (or
# just did) consume, which reads to the agent as a stale/redundant notification
# for mail it already fetched. flock serializes the read-check-write so only one
# instance of a simultaneous batch ever announces a given id.
lockf="$statedir/mess-steer-$who.lock"
exec 9>"$lockf"
flock 9

# State is "<highest announced id> <when we announced it>". A bare id is the
# older format; treat its timestamp as 0 so it re-announces once immediately.
statef="$statedir/mess-steer-$who.id"
prev=0
prev_at=0
if [ -r "$statef" ]; then
  read -r prev prev_at < "$statef" 2>/dev/null
fi
case "$prev" in ''|*[!0-9]*) prev=0 ;; esac
case "$prev_at" in ''|*[!0-9]*) prev_at=0 ;; esac
now=$(date +%s)

# (The auto-wake hook consumes on an idle wake, so a woken turn's inbox is empty
# here — no flag coordination needed. When the agent is working, the wake stands
# down and this hook is the sole notifier.)
if [ "$maxid" -gt "$prev" ] || [ "$((now - prev_at))" -ge "$RENOTIFY" ]; then
  again=""
  [ "$maxid" -le "$prev" ] && again=" (still unread)"
  emit "[mess] $n unread peer message(s)$asknote as of $at$again — run \`mess recv\` to read them."
  printf '%s %s\n' "$maxid" "$now" > "$statef"
fi
exit 0
