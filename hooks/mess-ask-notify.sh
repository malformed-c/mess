#!/usr/bin/env sh
# PreToolUse hook, matched to AskUserQuestion and ExitPlanMode only — DEFAULT ON
# for any session with a mess identity. Fires a desktop notification the moment an
# agent presents a choices list or a plan for approval, since both are a hard
# block on the human operator (unlike a mess message, which the wake/steer hooks
# already surface) and easy to miss if they're not watching that terminal.
#
# PreToolUse fires before the tool call executes/blocks, so the ping lands right
# as the choice/plan is presented rather than after the user has already answered.
#
# Scope: no-op outside a mess identity, and reuses the same opt-outs as the
# message-to-human notifier (MESS_NO_NOTIFY) so one switch silences both.
[ -n "$MESS_NO_NOTIFY" ] && exit 0

MESS=/home/engi/.local/bin/mess
who=$("$MESS" whoami 2>/dev/null)
[ -z "$who" ] && exit 0

in=$(cat)
tool=$(printf '%s' "$in" | jq -r '.tool_name // empty' 2>/dev/null)

case "$tool" in
  ExitPlanMode)
    summary="mess: $who is waiting on plan approval"
    body=$(printf '%s' "$in" | jq -r '.tool_input.plan // "Awaiting plan approval…"' 2>/dev/null)
    ;;
  *)
    summary="mess: $who is asking a question"
    body=$(printf '%s' "$in" | jq -r '.tool_input.questions[0].question // "Awaiting your choice…"' 2>/dev/null)
    ;;
esac

command -v notify-send >/dev/null 2>&1 &&
  notify-send -a mess -i dialog-question "$summary" "$(printf '%.200s' "$body")" 2>/dev/null

exit 0
