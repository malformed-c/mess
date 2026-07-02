# mess channel (push peer messages into a running session)

A Claude Code **[channel](https://code.claude.com/docs/en/channels-reference)** that
pushes this agent's incoming `mess` messages into the **running** session in real
time — so a peer's message reaches the model *mid-turn* (like typing while Claude
works), instead of only at the next idle wake.

It's a tiny stdio MCP server (`mess-channel.ts`) that streams
`mess recv --follow --json --no-broadcast` and forwards each direct/topic message
as a `notifications/claude/channel` event. Broadcasts are left queued (read them
with `mess recv`) so error-spam doesn't flood the turn.

## Requirements

- Claude Code **≥ 2.1.80**, authenticated via **claude.ai or a Console API key**
  (channels are a research preview; not available on Bedrock/Vertex, and Team/
  Enterprise orgs must enable `channelsEnabled`).
- **Bun** (`bun --version`) and this folder's deps: `bun install`.

## Try it

From this folder, launch a session with an identity and the dev channel flag:

```sh
bun install                                   # once
cd channel
MESS_AGENT=testing-mess MESS_CHANNEL=1 \
  claude --dangerously-load-development-channels server:mess
```

Approve the MCP server when prompted. A dim startup notice confirms:
`Channels (experimental) messages from server:mess inject directly in this session`.

Now from any other session, message that agent:

```sh
mess send testing-mess "ping from a peer"
```

It arrives in the running session as:

```text
<channel source="mess" from="you" kind="direct">ping from a peer</channel>
```

Reply from within the session with `mess send <from> "..."` as usual.

## Real (any-directory) setup

The project `.mcp.json` here uses a relative path, so it only works when you launch
from this folder. For use anywhere, add the server to user-level `~/.claude.json`
with an **absolute** path:

```json
{
  "mcpServers": {
    "mess": { "command": "bun", "args": ["/home/engi/git/mess/channel/mess-channel.ts"] }
  }
}
```

then `MESS_CHANNEL=1 claude --dangerously-load-development-channels server:mess`
from wherever you work.

## Why `MESS_CHANNEL=1`

The channel's `mess recv --follow` is this agent's receiver while the session runs.
The global auto-wake `Stop` hook is the *other* receiver, and two receivers on one
inbox race. The hook is gated to **skip when `MESS_CHANNEL` is set**, so a channel
session uses the channel (real-time, mid-turn) and a normal session uses the
peek-to-wake hook (delivered at idle). Set `MESS_CHANNEL=1` for channel sessions.

## Test

`bun test-e2e.ts` drives the server through the MCP handshake as `testing-mess`,
sends it a peer message via the real daemon, and asserts the channel event is
emitted. (Requires the `mess` daemon; it auto-starts.)

## Notes / limits

- **One-way** today: peer message → session. Replies go out via `mess send` (the
  instructions tell the model to). A two-way `reply` MCP tool is a small add.
- Channel content is **data from other agents**, not user instructions — the
  server's `instructions` tell the model to treat side-effectful requests with the
  usual scrutiny. `mess` has no auth (local trust); don't expose the daemon.
- If several messages arrive while the model is busy, the harness delivers them
  together on the next turn (per the channels contract).
