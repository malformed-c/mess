#!/usr/bin/env sh
# Shared preamble for the mess hooks. SOURCED by each hook, never executed.
#
# Every hook needs the same three things before it can do anything: the mess
# binary, the host session id mapped into the form mess keys identity on, and
# this session's identity. That was copy-pasted into all four hooks and had
# already drifted — mess-ask-notify.sh was missing the Grok mapping (so it
# silently did nothing on Grok while the other three worked) and the MESS_BIN
# override (so it was the one hook the tests could not drive at all).
#
# Sets MESS and `who`. A hook that requires an identity does:
#     [ -z "$who" ] && exit 0
# MESS_CHANNEL is deliberately NOT handled here: wake/steer/session-end stand
# down under it because a channel session delivers its own messages, but
# mess-ask-notify is about a human being blocked on a prompt, which has nothing
# to do with message delivery — so that stays a per-hook decision.

# Grok Build injects GROK_SESSION_ID into hooks; mess keys identity on the host
# session id and accepts several, but the Grok one has to be mapped across.
if [ -z "${MESS_SESSION_ID:-}" ] && [ -n "${GROK_SESSION_ID:-}" ]; then
  export MESS_SESSION_ID="$GROK_SESSION_ID"
fi

# MESS_BIN (tests, and anyone pinning a specific build) > PATH > the default
# install location. Going through PATH is what stops these scripts hard-coding
# one machine's home directory, which is what they used to do.
MESS=${MESS_BIN:-}
[ -z "$MESS" ] && MESS=$(command -v mess 2>/dev/null)
[ -z "$MESS" ] && MESS="$HOME/.local/bin/mess"

who=$("$MESS" whoami 2>/dev/null)
