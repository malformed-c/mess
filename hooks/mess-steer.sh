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
# Re-announcing exists because watermarking purely on id was fire-and-forget:
# the marker advanced the moment the notice was *printed*, with no confirmation
# the agent saw it, so one dropped injection meant that message was never
# mentioned again. But a FIXED window was calibrated against the wrong clock —
# during a build/test stretch a single tool call can take longer than the
# window, so "at most once a minute" degraded to "on every call". Two agents
# reported the same thing independently; one counted it on eight consecutive
# calls and said that by call four it had stopped carrying information. A line
# that repeats identically reads as decoration, so it got drained LATER than a
# single nudge would have managed. The interval now backs off (doubling to
# RENOTIFY_MAX), which keeps the dropped-injection safety net while making the
# repeat cheap.
#
# Every notice is stamped with the time it was emitted, and names the newest
# message id. additionalContext is sticky — each notice stays in the transcript
# — so a line re-read later must not read as present tense. The stamp makes a
# stale line obviously historical, and the id makes "same mail" distinguishable
# from "new mail" at a glance. The old "(still unread)" wording did the exact
# opposite: it asserted, in the present tense, something only true when written.
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

# Shared preamble: resolves MESS and `who` (see mess-common.sh). Sourcing it
# rather than repeating it is what stops the four hooks drifting apart again.
# Guard with -r rather than `. ... || exit 0`: a failed `.` is FATAL in POSIX
# sh, so the || never runs and a missing preamble would take the hook down
# noisily instead of standing it down.
_common="$(dirname "$0")/mess-common.sh"
[ -r "$_common" ] || exit 0
. "$_common"
[ -z "$who" ] && exit 0

RENOTIFY=${MESS_STEER_RENOTIFY:-120}          # seconds before the FIRST re-announce
RENOTIFY_MAX=${MESS_STEER_RENOTIFY_MAX:-900}  # ...doubling up to this ceiling

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

# State is "<highest announced id> <when> <consecutive repeats>". Older formats
# have fewer fields; the missing ones default to 0, which just means the next
# notice fires immediately and starts the backoff over.
statef="$statedir/mess-steer-$who.id"
prev=0
prev_at=0
repeats=0
if [ -r "$statef" ]; then
  read -r prev prev_at repeats < "$statef" 2>/dev/null
fi
case "$prev" in ''|*[!0-9]*) prev=0 ;; esac
case "$prev_at" in ''|*[!0-9]*) prev_at=0 ;; esac
case "$repeats" in ''|*[!0-9]*) repeats=0 ;; esac
now=$(date +%s)

# How long this repeat has to wait: RENOTIFY doubled once per previous repeat,
# capped. New mail never waits.
wait=$RENOTIFY
i=0
while [ "$i" -lt "$repeats" ]; do
  wait=$((wait * 2))
  if [ "$wait" -ge "$RENOTIFY_MAX" ]; then
    wait=$RENOTIFY_MAX
    break
  fi
  i=$((i + 1))
done

# (The auto-wake hook consumes on an idle wake, so a woken turn's inbox is empty
# here — no flag coordination needed. When the agent is working, the wake stands
# down and this hook is the sole notifier.)
if [ "$maxid" -gt "$prev" ]; then
  repeats=0
elif [ "$((now - prev_at))" -ge "$wait" ]; then
  repeats=$((repeats + 1))
else
  exit 0
fi
emit "[mess] $(date +%H:%M:%S) — $n unread peer message(s)$asknote, newest m$maxid. Run \`mess recv\` to read them."
printf '%s %s %s\n' "$maxid" "$now" "$repeats" > "$statef"
exit 0
