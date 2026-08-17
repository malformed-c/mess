package main

import "mess/wire"

// The protocol lives in package wire so a separate module (the TUI) can speak
// it. These aliases — real aliases, not new types — keep every existing
// reference in package main working unchanged, which is what made extracting
// it a rename rather than a rewrite. It only works because none of these types
// carry methods here; a method would have had to move with them.
type (
	Message       = wire.Message
	Request       = wire.Request
	Response      = wire.Response
	AgentInfo     = wire.AgentInfo
	TopicInfo     = wire.TopicInfo
	BridgeInfo    = wire.BridgeInfo
	ThreadInfo    = wire.ThreadInfo
	InviteSummary = wire.InviteSummary
)

const (
	KindDirect    = wire.KindDirect
	KindBroadcast = wire.KindBroadcast
	KindTopic     = wire.KindTopic

	ReasonRaced   = wire.ReasonRaced
	ReasonTimeout = wire.ReasonTimeout
	ReasonEvicted = wire.ReasonEvicted
)
