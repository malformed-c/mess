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

# Shared preamble: resolves MESS and `who` (see mess-common.sh). Sourcing it
# rather than repeating it is what stops the four hooks drifting apart again.
# Guard with -r rather than `. ... || exit 0`: a failed `.` is FATAL in POSIX
# sh, so the || never runs and a missing preamble would take the hook down
# noisily instead of standing it down.
_common="$(dirname "$0")/mess-common.sh"
[ -r "$_common" ] || exit 0
. "$_common"
[ -z "$who" ] && exit 0

"$MESS" session-end 2>/dev/null
exit 0
