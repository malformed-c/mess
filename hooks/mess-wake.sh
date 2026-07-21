#!/usr/bin/env sh
# Stop-hook auto-wake for mess (asyncRewake).
# Parks on `mess recv --wait`. On a wake-worthy peer message while idle,
# consumes the inbox and prints messages to stderr (asyncRewake exit 2).
#
# If the agent is working when mail arrives, do NOT exit 0 (that un-parks
# forever until the next Stop). Poll until idle and drain, so a turn that
# ends with mail already queued — or a host that arms rewake before unbusy —
# still wakes. Mid-turn steer remains the in-turn notifier; we only deliver
# once --if-idle succeeds.
[ -n "$MESS_CHANNEL" ] && exit 0

# Grok Build injects GROK_SESSION_ID into hooks; mess keys identity on session id.
if [ -z "${MESS_SESSION_ID:-}" ] && [ -n "${GROK_SESSION_ID:-}" ]; then
  export MESS_SESSION_ID="$GROK_SESSION_ID"
fi

MESS=/home/engi/.local/bin/mess
who=$("$MESS" whoami 2>/dev/null)
[ -z "$who" ] && exit 0

LOCK="${TMPDIR:-/tmp}/mess-wake-$who.lock"

# Outer loop: park until mail, drain when idle, re-park if mail was consumed
# by a mid-turn recv before we could drain.
while true; do
  # Park until a wake-worthy non-quiet direct/topic message arrives.
  # --no-broadcast is the wake filter; loud broadcasts still unblock server-side.
  # flock -n: only one waiter per agent (a second Stop re-arm loses the race and
  # exits 0 — the holder is the live park).
  peek=$(flock -n "$LOCK" \
    "$MESS" recv --wait --no-broadcast --peek --json --batch 1s 2>/dev/null \
    | jq -c 'select(.quiet != true)' 2>/dev/null)
  [ -z "$peek" ] && exit 0

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
        '"  " + (if .ask then "[QUESTION \(.id) - reply with mess reply, not a plain send] " else "" end) + .from + (if .topic then " #\(.topic)" else "" end) + ": " + .body'
    } >&2
    exit 2
  done
done
