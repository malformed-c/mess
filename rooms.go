package main

import "strings"

// roomSep separates room from name in every internal composite key. It's a
// control byte that can never appear in a CLI-supplied name (flags/positional
// args come from a shell or JSON string, neither of which can carry a raw NUL),
// so the split is unambiguous.
const roomSep = "\x00"

// agentKey returns the map key used for every per-agent broker map. room == ""
// (the global/default room -- every agent that has never called `mess room
// join`) collapses to exactly today's bare name, so a non-adopting agent's key
// is byte-for-byte identical to pre-rooms mess. This is what makes rooms fully
// additive: none of the broker's ~30 name-keyed methods need to change once a
// composite key is computed here, since they already treat name as opaque.
func agentKey(room, name string) string {
	if room == "" || name == "" {
		return name
	}
	return room + roomSep + name
}

// splitAgentKey recovers (room, bareName) from a composite key.
func splitAgentKey(key string) (room, name string) {
	if r, n, ok := strings.Cut(key, roomSep); ok {
		return r, n
	}
	return "", key
}

// topicKey / splitTopicKey reuse the identical (room, name) -> composite-key
// scheme as agents -- topics and agents share one keyspace shape, just
// different maps.
func topicKey(room, topic string) string {
	return agentKey(room, topic)
}

func splitTopicKey(key string) (room, name string) {
	return splitAgentKey(key)
}

// displayName renders a (room, name) pair for operator-facing text output
// (ps --all, cleanup's Removed list): "name" in the global room, else
// "room/name".
func displayName(room, name string) string {
	if room == "" {
		return name
	}
	return room + "/" + name
}

// roomThenNameLess orders two (room, name) pairs — room first, name breaking
// ties — for stable, grouped-by-room output (ps's agents/topics, and their
// snapshot equivalents).
func roomThenNameLess(roomA, nameA, roomB, nameB string) bool {
	if roomA != roomB {
		return roomA < roomB
	}
	return nameA < nameB
}
