package main

import (
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"mess/wire"
)

// A bubbletea model is a pure function of its messages, so it can be driven
// directly — no terminal, no pty, no sleeping. That matters here because the
// alternative is "it compiles", which says nothing about whether the thing
// renders or routes a keystroke.

func newTestTUI(t *testing.T, w, h int) *tuiModel {
	t.Helper()
	m := newTUIModel(t.TempDir(), "engi", "", true, time.Hour)
	tm, _ := m.Update(tea.WindowSizeMsg{Width: w, Height: h})
	return tm.(*tuiModel)
}

func feed(t *testing.T, m *tuiModel, msgs ...wire.Message) *tuiModel {
	t.Helper()
	tm, _ := m.Update(tuiDataMsg{msgs: msgs})
	return tm.(*tuiModel)
}

func key(t *testing.T, m *tuiModel, k string) *tuiModel {
	t.Helper()
	var msg tea.KeyMsg
	switch k {
	case "tab":
		msg = tea.KeyMsg{Type: tea.KeyTab}
	case "enter":
		msg = tea.KeyMsg{Type: tea.KeyEnter}
	case "esc":
		msg = tea.KeyMsg{Type: tea.KeyEsc}
	default:
		msg = tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(k)}
	}
	tm, _ := m.Update(msg)
	return tm.(*tuiModel)
}

var (
	msgTopic = wire.Message{ID: "m1", From: "trail", Topic: "peri", Kind: wire.KindTopic, Body: "build is green", Time: time.Now()}
	msgToMe  = wire.Message{ID: "m2", From: "fable", To: "engi", Kind: wire.KindDirect, Body: "look at this", Time: time.Now()}
	msgMine  = wire.Message{ID: "m3", From: "engi", To: "fable", Kind: wire.KindDirect, Body: "on it", Time: time.Now()}
	msgOther = wire.Message{ID: "m4", From: "a", To: "b", Kind: wire.KindDirect, Body: "not mine", Time: time.Now()}
)

// Topics and broadcasts are shared channels; direct messages appear only when
// this operator is one of the two ends. Somebody else's DMs are not a channel.
func TestTUIRoutesMessagesIntoConversations(t *testing.T) {
	m := feed(t, newTestTUI(t, 100, 30), msgTopic, msgToMe, msgMine, msgOther)

	var keys []string
	for _, c := range m.convos {
		keys = append(keys, c.key)
	}
	want := []string{"#peri", "@fable", "a ⇄ b"}
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Fatalf("want conversations %v, got %v", want, keys)
	}
	// Both directions of the DM belong to the same conversation.
	if got := len(m.byKey["@fable"].msgs); got != 2 {
		t.Fatalf("a DM conversation should hold both sides, got %d", got)
	}
	// Two other agents talking is shown too — watching that is the point of an
	// operator view, and hiding it was the bug that made the UI look empty.
	if _, ok := m.byKey["a ⇄ b"]; !ok {
		t.Fatalf("fleet traffic should be visible, got %v", keys)
	}
}

// Polling re-reads the whole window, so the same message arriving twice must
// not double up.
func TestTUIPollingIsIdempotent(t *testing.T) {
	m := feed(t, newTestTUI(t, 100, 30), msgTopic, msgToMe)
	m = feed(t, m, msgTopic, msgToMe) // same poll again
	if got := len(m.byKey["#peri"].msgs); got != 1 {
		t.Fatalf("re-reading the journal duplicated a message: %d copies", got)
	}
}

// The focus is what makes a chat TUI usable: j/k must scroll the conversation
// when the messages have focus, and type into the composer when it does.
func TestTUIFocusRoutesKeysToOnePlaceAtATime(t *testing.T) {
	m := feed(t, newTestTUI(t, 100, 30), msgTopic, msgToMe)
	if m.focus != focusChannels {
		t.Fatalf("should start on the channel list, got %v", m.focus)
	}

	// j/k walk the channel list, and do NOT reach the composer.
	m = key(t, m, "j")
	if m.sel != 1 {
		t.Fatalf("j should move down the channel list, sel=%d", m.sel)
	}
	if m.input.Value() != "" {
		t.Fatalf("navigation keys must not leak into the composer: %q", m.input.Value())
	}

	// enter hands over to the composer, where the same key is now text.
	m = key(t, m, "enter")
	if m.focus != focusComposer {
		t.Fatalf("enter should move focus to the composer, got %v", m.focus)
	}
	m = key(t, m, "j")
	if m.input.Value() != "j" {
		t.Fatalf("with the composer focused, j is text; got %q", m.input.Value())
	}

	// esc backs out without losing the buffer, and without quitting.
	m = key(t, m, "esc")
	if m.focus == focusComposer || m.input.Value() != "j" {
		t.Fatalf("esc should leave the composer and keep the draft, focus=%v draft=%q", m.focus, m.input.Value())
	}
}

// Unread counts exist to be cleared by looking, and never for the conversation
// you are already looking at.
func TestTUIUnreadCountsClearOnSelect(t *testing.T) {
	m := feed(t, newTestTUI(t, 100, 30), msgTopic, msgToMe) // initial load: not "new"
	for _, c := range m.convos {
		if c.fresh != 0 {
			t.Fatalf("the first load is history, not unread news: %s has %d", c.key, c.fresh)
		}
	}
	// A later arrival in a conversation we are not on counts.
	m = feed(t, m, wire.Message{ID: "m9", From: "fable", To: "engi", Kind: wire.KindDirect, Body: "ping", Time: time.Now()})
	if m.byKey["@fable"].fresh != 1 {
		t.Fatalf("a new message elsewhere should count as unread, got %d", m.byKey["@fable"].fresh)
	}
	for i, c := range m.convos { // select it
		if c.key == "@fable" {
			m.sel = i
		}
	}
	m = key(t, m, "j")
	m = key(t, m, "k")
	if m.byKey["@fable"].fresh != 0 {
		t.Fatal("selecting a conversation should clear its unread count")
	}
}

// A narrow terminal drops the sidebar rather than squeezing both panes into
// something neither can use.
func TestTUICollapsesOnANarrowTerminal(t *testing.T) {
	wide := feed(t, newTestTUI(t, 120, 30), msgTopic)
	if wide.compact || wide.sidebarWidth() == 0 {
		t.Fatal("a wide terminal should keep the sidebar")
	}
	narrow := feed(t, newTestTUI(t, 40, 20), msgTopic)
	if !narrow.compact || narrow.sidebarWidth() != 0 {
		t.Fatal("a narrow terminal should drop the sidebar entirely")
	}
	if out := narrow.View(); out == "" {
		t.Fatal("compact mode still has to render something")
	}
}

// The view has to actually contain the conversation, which is the one thing
// "it compiles" cannot tell you.
func TestTUIViewRendersTheSelectedConversation(t *testing.T) {
	m := feed(t, newTestTUI(t, 100, 30), msgTopic, msgToMe)
	out := m.View()
	// Channels keep their sigil; a DM is the peer's name under a "direct"
	// heading, which is the convention every chat client uses.
	for _, want := range []string{"#peri", "direct", "fable", "engi"} {
		if !strings.Contains(out, want) {
			t.Fatalf("view should show %q:\n%s", want, out)
		}
	}
	if !strings.Contains(out, "build is green") {
		t.Fatalf("the selected conversation's messages should be visible:\n%s", out)
	}
	// Your own messages read as yours.
	for i, c := range m.convos {
		if c.key == "@fable" {
			m.sel = i
		}
	}
	m.render()
	if !strings.Contains(m.View(), "you") {
		t.Fatalf("your own messages should be attributed to you:\n%s", m.View())
	}
}
