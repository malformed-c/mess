#!/usr/bin/env sh
# SessionEnd hook for `mess` — POSIX sh.
#
# Retires this session's presence on the way out. Without it, a session that
# exits (cleanly or not) keeps looking present: `mess busy` carries a one-hour
# crash backstop so `ps` shows it `working`, and its parked wake hook keeps a
# listener — and the per-agent lock — until that hook notices its session is
# gone. Peers then believe a message reached a live agent, and a relaunch under
# the same name can't park because the old waiter still holds the lock.
#
# `mess session-end` clears the in-a-turn flag and evicts the parked waiter. It
# deliberately does NOT unregister: ending a session is not leaving the network,
# and mail peers already delivered must survive a relaunch. The daemon's pid
# probe covers the unclean exits this hook never gets to run for.
[ -n "$MESS_CHANNEL" ] && exit 0

# Grok Build injects GROK_SESSION_ID into hooks; mess keys identity on session id.
if [ -z "${MESS_SESSION_ID:-}" ] && [ -n "${GROK_SESSION_ID:-}" ]; then
  export MESS_SESSION_ID="$GROK_SESSION_ID"
fi

MESS=${MESS_BIN:-/home/engi/.local/bin/mess}  # MESS_BIN lets the tests drive a throwaway build
who=$("$MESS" whoami 2>/dev/null)
[ -z "$who" ] && exit 0

"$MESS" session-end 2>/dev/null
exit 0
