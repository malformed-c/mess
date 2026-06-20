package main

import "time"

// Message kinds.
const (
	KindDirect    = "direct"
	KindBroadcast = "broadcast"
	KindTopic     = "topic"
)

// Message is a single delivered item in an agent's inbox.
type Message struct {
	ID    string    `json:"id"`
	From  string    `json:"from"`
	To    string    `json:"to,omitempty"`    // recipient for direct messages
	Topic string    `json:"topic,omitempty"` // channel for topic messages
	Kind  string    `json:"kind"`            // direct | broadcast | topic
	Body  string    `json:"body"`
	Time  time.Time `json:"time"`

	// AckRequested marks a direct message whose sender is blocking for a read
	// receipt. Set on send; the sender is notified when the message is consumed.
	AckRequested bool `json:"ack,omitempty"`
}

// Request is one command sent from a client to the daemon.
type Request struct {
	Op      string   `json:"op"`
	As      string   `json:"as,omitempty"`      // identity of the calling agent
	To      string   `json:"to,omitempty"`      // direct recipient
	Topic   string   `json:"topic,omitempty"`   // topic for pub/sub
	Body    string   `json:"body,omitempty"`    // message body
	Ack     bool     `json:"ack,omitempty"`     // (send) block until recipient reads it
	Wait    bool     `json:"wait,omitempty"`    // block until a message arrives
	Timeout string   `json:"timeout,omitempty"` // optional wait timeout (duration)
	Peek    bool     `json:"peek,omitempty"`    // recv without consuming
	Max     int      `json:"max,omitempty"`     // recv at most N messages (0 = all)
	Kinds   []string `json:"kinds,omitempty"`   // recv only these kinds (nil = all)
}

// AgentInfo is reported by the `ps` op.
type AgentInfo struct {
	Name      string   `json:"name"`
	Pending   int      `json:"pending"`
	Topics    []string `json:"topics,omitempty"`
	Listening bool     `json:"listening,omitempty"` // has an active streaming listener
	State     string   `json:"state,omitempty"`     // self-reported working state
}

// TopicInfo is reported by the `ps` op.
type TopicInfo struct {
	Name        string   `json:"name"`
	Subscribers []string `json:"subscribers"`
}

// Response is the daemon's reply to a Request.
type Response struct {
	OK       bool        `json:"ok"`
	Error    string      `json:"error,omitempty"`
	Acked    bool        `json:"acked,omitempty"` // (send --ack) recipient read the message
	Messages []Message   `json:"messages,omitempty"`
	Agents   []AgentInfo `json:"agents,omitempty"`
	Topics   []TopicInfo `json:"topics,omitempty"`
	Count    int         `json:"count,omitempty"`
}
