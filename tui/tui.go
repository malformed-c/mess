package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"mess/wire"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// A chat-shaped view of the fleet, for the human rather than for agents.
//
// It reads from the JOURNAL, not from an inbox, and that is the central design
// choice. An inbox read is destructive — `recv` consumes, and `listen` would
// too — so a TUI built on one would quietly eat the operator's mail every time
// they glanced at it, and would compete with the auto-wake hook for the single
// receiver each agent is allowed. The journal is an append-only record of every
// message actually sent, so reading it can be done continuously, by any number
// of viewers, without taking anything away from anyone.
//
// It also shows more: an inbox only holds what was addressed to you, while the
// journal holds every topic the fleet is talking on. Watching that is the whole
// job this is for.

const (
	tuiPollInterval = 2 * time.Second // journal + presence refresh
	tuiHistory      = 24 * time.Hour  // how far back to load
	tuiMaxMessages  = 4000            // ...and a ceiling on it

	// The sidebar is a fraction of the width, not a fixed column count, and
	// below tuiCompactCols it collapses entirely so the messages still get a
	// readable pane. Borrowed from tuilegram, which had both — a fixed sidebar
	// eats a narrow terminal alive.
	tuiSidebarRatio = 0.26
	tuiSidebarMin   = 14
	tuiSidebarMax   = 32
	tuiCompactCols  = 60
)

// focusPane is which pane the keyboard is driving. Without this every key
// except a hardcoded few went to the composer, so the message pane could not be
// scrolled and the channel list could not be walked — you cannot type and
// navigate with the same keys, which is why tuilegram carries an explicit focus
// too. Tab moves the focus; the composer only sees keys while it holds it.
type focusPane int

const (
	focusChannels focusPane = iota
	focusMessages
	focusComposer
)

func (f focusPane) String() string {
	switch f {
	case focusChannels:
		return "channels"
	case focusMessages:
		return "messages"
	}
	return "compose"
}

// convo is one addressable conversation in the sidebar. key is the routing
// identity ("#topic", "@peer", or the broadcast pseudo-channel); it is what
// Enter sends to.
type convo struct {
	key   string
	kind  string // wire.KindTopic, wire.KindDirect, wire.KindBroadcast
	name  string // bare topic or peer name
	msgs  []wire.Message
	fresh int // messages arrived since this conversation was last looked at
}

// tuiModel is the whole UI state.
type tuiModel struct {
	sock string
	jrnl string
	me   string
	room string

	convos []*convo
	byKey  map[string]*convo
	sel    int

	agents []wire.AgentInfo
	seen   map[string]bool // message ids already placed, so a re-poll can't duplicate

	input   textinput.Model
	vp      viewport.Model
	w, h    int
	focus   focusPane
	compact bool // too narrow for a sidebar; show one pane at a time
	status  string
	ready   bool
}

// tuiTickMsg drives the poll; tuiDataMsg carries a poll's results.
type tuiTickMsg time.Time
type tuiDataMsg struct {
	msgs   []wire.Message
	agents []wire.AgentInfo
	err    error
}

func (m *tuiModel) Init() tea.Cmd {
	return tea.Batch(textinput.Blink, m.poll(), tuiTick())
}

func tuiTick() tea.Cmd {
	return tea.Tick(tuiPollInterval, func(t time.Time) tea.Msg { return tuiTickMsg(t) })
}

// poll reads the journal and presence. Both are cheap local reads, and neither
// consumes anything, so it is safe to do on a timer forever.
func (m *tuiModel) poll() tea.Cmd {
	jrnl, sock, room := m.jrnl, m.sock, m.room
	return func() tea.Msg {
		msgs, err := wire.SearchJournal(jrnl, wire.Filter{
			Room: room, Since: tuiHistory, Max: tuiMaxMessages, Now: time.Now(),
		})
		if err != nil {
			return tuiDataMsg{err: err}
		}
		var agents []wire.AgentInfo
		if resp, err := wire.Call(sock, wire.Request{Op: "ps"}); err == nil {
			agents = resp.Agents
		}
		return tuiDataMsg{msgs: msgs, agents: agents}
	}
}

// place files a message into its conversation, creating it on first sight.
// Returns false if it was already known, so polling is idempotent.
func (m *tuiModel) place(msg wire.Message, initial bool) bool {
	if m.seen[msg.ID] {
		return false
	}
	key, kind, name := m.route(msg)
	if key == "" {
		return false // an agent-to-agent DM: not this operator's conversation
	}
	m.seen[msg.ID] = true
	c, ok := m.byKey[key]
	if !ok {
		c = &convo{key: key, kind: kind, name: name}
		m.byKey[key] = c
		m.convos = append(m.convos, c)
	}
	c.msgs = append(c.msgs, msg)
	if !initial && m.current() != c {
		c.fresh++
	}
	return true
}

// route decides which conversation a message belongs to. Topics and broadcasts
// are shared channels and always shown; direct messages are shown only when
// this operator is one of the two ends, since the rest are other people's mail.
func (m *tuiModel) route(msg wire.Message) (key, kind, name string) {
	switch msg.Kind {
	case wire.KindTopic:
		return "#" + msg.Topic, wire.KindTopic, msg.Topic
	case wire.KindBroadcast:
		return "#broadcast", wire.KindBroadcast, ""
	case wire.KindDirect:
		switch {
		case msg.To == m.me:
			return "@" + msg.From, wire.KindDirect, msg.From
		case msg.From == m.me:
			return "@" + msg.To, wire.KindDirect, msg.To
		}
	}
	return "", "", ""
}

func (m *tuiModel) current() *convo {
	if m.sel < 0 || m.sel >= len(m.convos) {
		return nil
	}
	return m.convos[m.sel]
}

func (m *tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
		m.layout()
		m.ready = true
		m.render()
		return m, nil

	case tuiTickMsg:
		return m, tea.Batch(m.poll(), tuiTick())

	case tuiDataMsg:
		if msg.err != nil {
			m.status = "journal: " + msg.err.Error()
			return m, nil
		}
		m.agents = msg.agents
		initial := len(m.seen) == 0
		added := 0
		for _, x := range msg.msgs {
			if m.place(x, initial) {
				added++
			}
		}
		if added > 0 {
			m.sortConvos()
			m.render()
		}
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, tea.Quit
		case "esc":
			// Esc backs out of the composer rather than quitting: losing a
			// half-typed message to a stray keypress is not a good trade.
			if m.focus == focusComposer {
				m.focus = focusChannels
				m.input.Blur()
				return m, nil
			}
			return m, tea.Quit
		case "tab":
			return m, m.cycleFocus(1)
		case "shift+tab":
			return m, m.cycleFocus(-1)
		}

		switch m.focus {
		case focusChannels:
			switch msg.String() {
			case "up", "k":
				m.selectDelta(-1)
			case "down", "j":
				m.selectDelta(1)
			case "enter", "i":
				m.focus = focusComposer
				return m, m.input.Focus()
			case "g":
				m.vp.GotoTop()
			case "G":
				m.vp.GotoBottom()
			}
			return m, nil
		case focusMessages:
			switch msg.String() {
			case "up", "k":
				m.vp.LineUp(1)
			case "down", "j":
				m.vp.LineDown(1)
			case "pgup", "ctrl+u":
				m.vp.HalfPageUp()
			case "pgdown", "ctrl+d":
				m.vp.HalfPageDown()
			case "g":
				m.vp.GotoTop()
			case "G":
				m.vp.GotoBottom()
			case "enter", "i":
				m.focus = focusComposer
				return m, m.input.Focus()
			}
			return m, nil
		case focusComposer:
			if msg.String() == "enter" {
				return m, m.send()
			}
		}
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

// cycleFocus moves the keyboard between panes, focusing/blurring the composer
// so a keystroke only ever lands in one place.
func (m *tuiModel) cycleFocus(d int) tea.Cmd {
	m.focus = focusPane((int(m.focus) + d + 3) % 3)
	if m.focus == focusComposer {
		return m.input.Focus()
	}
	m.input.Blur()
	return nil
}

// send posts whatever is in the composer to the selected conversation. It
// routes by the conversation's own kind rather than asking the user to know
// whether they are in a topic or a DM — that is the entire point of a channel
// list. Broadcast is deliberately not sendable here: it reaches every agent on
// the machine, which is not something to do by pressing Enter in the wrong pane.
func (m *tuiModel) send() tea.Cmd {
	body := strings.TrimSpace(m.input.Value())
	c := m.current()
	if body == "" || c == nil {
		return nil
	}
	if c.kind == wire.KindBroadcast {
		m.status = "#broadcast is read-only here — use `mess broadcast` or `mess shout` deliberately"
		return nil
	}
	m.input.SetValue("")
	sock, me := m.sock, m.me
	req := wire.Request{Op: "send", As: me, To: c.name, Body: body}
	if c.kind == wire.KindTopic {
		req = wire.Request{Op: "pub", As: me, Topic: c.name, Body: body}
	}
	return func() tea.Msg {
		if _, err := wire.Call(sock, req); err != nil {
			return tuiDataMsg{err: err}
		}
		return tuiTickMsg(time.Now()) // pick our own message up on the next read
	}
}

func (m *tuiModel) selectDelta(d int) {
	if len(m.convos) == 0 {
		return
	}
	m.sel = (m.sel + d + len(m.convos)) % len(m.convos)
	if c := m.current(); c != nil {
		c.fresh = 0
	}
	m.render()
}

// sortConvos keeps channels above DMs, each alphabetical — a stable order, so
// a conversation does not move under the cursor when a message arrives.
func (m *tuiModel) sortConvos() {
	cur := m.current()
	sort.SliceStable(m.convos, func(i, j int) bool {
		a, b := m.convos[i], m.convos[j]
		if (a.kind == wire.KindDirect) != (b.kind == wire.KindDirect) {
			return b.kind == wire.KindDirect
		}
		return a.key < b.key
	})
	for i, c := range m.convos {
		if c == cur {
			m.sel = i
		}
	}
}

// sidebarWidth is a share of the terminal, clamped, and zero when there is not
// enough width to justify one at all.
func (m *tuiModel) sidebarWidth() int {
	if m.compact {
		return 0
	}
	w := int(float64(m.w) * tuiSidebarRatio)
	return max(tuiSidebarMin, min(tuiSidebarMax, w))
}

func (m *tuiModel) layout() {
	m.compact = m.w < tuiCompactCols
	bodyH := max(3, m.h-4) // header, composer, status
	w := m.w
	if sb := m.sidebarWidth(); sb > 0 {
		w -= sb + 1 // +1 for the divider
	}
	w = max(20, w)
	m.vp = viewport.New(w, bodyH)
	m.input.Width = max(10, w-2)
}

var (
	tuiDim      = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	tuiAccent   = lipgloss.NewStyle().Foreground(lipgloss.Color("62")).Bold(true)
	tuiSelected = lipgloss.NewStyle().Foreground(lipgloss.Color("231")).Background(lipgloss.Color("62")).Bold(true)
	tuiOnline   = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	tuiFresh    = lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Bold(true)
	tuiAuthor   = lipgloss.NewStyle().Bold(true)
	tuiBar      = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	tuiBody     = lipgloss.NewStyle().PaddingLeft(2)
)

// render rebuilds the message pane for the selected conversation.
func (m *tuiModel) render() {
	c := m.current()
	if c == nil {
		m.vp.SetContent(tuiDim.Render("nothing yet — no messages in the last 24h"))
		return
	}
	var b strings.Builder
	for _, msg := range c.msgs {
		who := msg.From
		if msg.From == m.me {
			who = "you"
		}
		fmt.Fprintf(&b, "%s %s\n", tuiDim.Render(msg.Time.Format("15:04")), tuiAuthor.Render(who))
		// Wrap to the pane rather than letting the viewport clip: agents send
		// paragraphs and stack traces, and a message whose end is cut off is a
		// message you have to leave the UI to read.
		fmt.Fprintf(&b, "%s\n", tuiBody.Width(m.vp.Width-2).Render(strings.TrimRight(msg.Body, "\n")))
		if msg.Invite != "" {
			fmt.Fprintf(&b, "  %s\n", tuiAccent.Render("[invite "+msg.ID+" — mess accept "+msg.ID+"]"))
		}
		b.WriteString("\n")
	}
	m.vp.SetContent(b.String())
	m.vp.GotoBottom()
}

// presence renders the status letter for an agent name, so the sidebar shows
// who is actually reachable rather than just who has ever existed.
func (m *tuiModel) presence(name string) string {
	for _, a := range m.agents {
		if a.Name != name {
			continue
		}
		switch {
		case a.Working:
			return tuiOnline.Render("*")
		case a.Listening:
			return tuiOnline.Render("o")
		case a.Online:
			return tuiOnline.Render(".")
		}
		return tuiDim.Render(" ")
	}
	return tuiDim.Render(" ")
}

func (m *tuiModel) View() string {
	if !m.ready {
		return "starting…"
	}
	sb := m.sidebarWidth()
	// In compact mode there is no sidebar column, but the channel list is still
	// rendered full-width when it holds focus — so the label budget is the
	// whole terminal, not a zero-width column. Getting this wrong fed a
	// negative width to trimTo and panicked the UI on any narrow terminal.
	labelW := sb
	if labelW <= 0 {
		labelW = m.w
	}
	var side strings.Builder
	side.WriteString(tuiAccent.Render("channels") + "\n")
	inDMs := false
	for i, c := range m.convos {
		if c.kind == wire.KindDirect && !inDMs {
			side.WriteString("\n" + tuiAccent.Render("direct") + "\n")
			inDMs = true
		}
		label := c.key
		if c.kind == wire.KindDirect {
			label = m.presence(c.name) + " " + c.name
		}
		if c.fresh > 0 {
			label += tuiFresh.Render(fmt.Sprintf(" %d", c.fresh))
		}
		if i == m.sel {
			label = tuiSelected.Render(" " + trimTo(label, labelW-2) + " ")
		} else {
			label = " " + trimTo(label, labelW-2)
		}
		side.WriteString(label + "\n")
	}

	title := "mess"
	if c := m.current(); c != nil {
		title = c.key
		if c.kind == wire.KindTopic {
			title += tuiDim.Render("  (topic)")
		}
	}
	header := tuiBar.Render(fmt.Sprintf("%s — you are %s%s", title, m.me, roomSuffix(m.room)))
	status := m.status
	if status == "" {
		status = tuiDim.Render(fmt.Sprintf("[%s] tab pane · j/k move · enter compose · esc back · ctrl+c quit", m.focus))
	}

	// Compact terminals get one pane at a time: the channel list when it has
	// focus, the conversation otherwise. A sidebar squeezed into 30 columns
	// serves neither.
	body := m.vp.View()
	if sb > 0 {
		body = lipgloss.JoinHorizontal(lipgloss.Top,
			lipgloss.NewStyle().Width(sb).Render(side.String()),
			tuiDim.Render("│"),
			m.vp.View(),
		)
	} else if m.focus == focusChannels {
		body = side.String()
	}
	return strings.Join([]string{header, body, m.input.View(), status}, "\n")
}

func roomSuffix(room string) string {
	if room == "" {
		return ""
	}
	return " in " + room
}

func trimTo(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if lipgloss.Width(s) <= n {
		return s
	}
	r := []rune(s)
	if len(r) > n {
		r = r[:n]
	}
	return string(r)
}

// newTUIModel builds the model. Split out from main so the whole thing can be
// driven by tests without a terminal — a bubbletea model is a pure function of
// its messages, and "it compiles" is not evidence that it renders.
func newTUIModel(dir, me, room string) *tuiModel {
	in := textinput.New()
	in.Placeholder = "message…"
	in.Prompt = "› "

	return &tuiModel{
		sock:  wire.Socket(dir),
		jrnl:  wire.Journal(dir),
		me:    me,
		room:  room,
		byKey: map[string]*convo{},
		seen:  map[string]bool{},
		input: in,
		vp:    viewport.New(40, 10),
	}
}

func main() {
	var as, room string
	// `user` is the default because this is the operator's window, and `user`
	// is the handle mess already reserves for the human. An agent watching its
	// own conversations passes --as.
	flag.StringVar(&as, "as", envOr("MESS_AGENT", "user"), "identity to act as")
	flag.StringVar(&room, "room", os.Getenv("MESS_ROOM"), "room to watch (default: global)")
	flag.Parse()

	m := newTUIModel(wire.Dir(), as, room)
	if _, err := tea.NewProgram(m, tea.WithAltScreen()).Run(); err != nil {
		fmt.Fprintln(os.Stderr, "mess-tui:", err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
