#!/usr/bin/env sh
# Stop-hook auto-wake for mess (asyncRewake).
# Parks on `mess recv --wait`. On a wake-worthy peer message while idle,
# consumes the inbox and prints messages to stderr (asyncRewake exit 2).
#
# If the agent is working when mail arrives, stands down (leaves queued)
# so the mid-turn steer hook is the sole notifier.
[ -n "$MESS_CHANNEL" ] && exit 0

# Grok Build injects GROK_SESSION_ID into hooks; mess keys identity on session id.
if [ -z "${MESS_SESSION_ID:-}" ] && [ -n "${GROK_SESSION_ID:-}" ]; then
  export MESS_SESSION_ID="$GROK_SESSION_ID"
fi

MESS=/home/engi/.local/bin/mess
who=$("$MESS" whoami 2>/dev/null)
[ -z "$who" ] && exit 0

# Park until a wake-worthy non-quiet direct/topic message arrives.
# --no-broadcast is the wake filter; loud broadcasts still unblock server-side.
peek=$(flock -n "${TMPDIR:-/tmp}/mess-wake-$who.lock" \
  "$MESS" recv --wait --no-broadcast --peek --json --batch 1s 2>/dev/null \
  | jq -c 'select(.quiet != true)' 2>/dev/null)
[ -z "$peek" ] && exit 0

# Idle-only drain via --if-idle (atomic busy check + consume).
has_loud=$(printf '%s\n' "$peek" | jq -s 'map(.loud == true) | any')
if [ "$has_loud" = "true" ]; then
  resp=$("$MESS" recv --if-idle --json 2>/dev/null)
else
  resp=$("$MESS" recv --if-idle --no-broadcast --json 2>/dev/null)
fi

# --if-idle prints a single busy object when standing down.
printf '%s\n' "$resp" | grep -qx '{"busy":true}' && exit 0
drained="$resp"
[ -z "$drained" ] && exit 0

n=$(printf '%s\n' "$drained" | grep -c .)
{
  printf '[mess] %s new peer message(s) (delivered on wake - no recv needed):\n' "$n"
  printf '%s\n' "$drained" | jq -r \
    '"  " + (if .ask then "[QUESTION \(.id) - reply with mess reply, not a plain send] " else "" end) + .from + (if .topic then " #\(.topic)" else "" end) + ": " + .body'
} >&2
exit 2
