package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
)

// cmdTUI hands off to the mess-tui binary.
//
// The UI is a separate module because mess is deliberately dependency-free and
// a build tag cannot preserve that — `go mod tidy` considers every build
// configuration, so a tagged-out import is still a requirement in go.mod. Only
// a separate module keeps the core clean. This shim exists so `mess tui` still
// works as a command; it is an exec, not a dependency.
func cmdTUI(_ paths, args []string) error {
	bin, err := exec.LookPath("mess-tui")
	if err != nil {
		return errors.New("mess-tui is not on PATH — the terminal UI is a separate module " +
			"(it pulls in bubbletea, which mess itself deliberately does not). Build it with:\n" +
			"  cd " + repoHint() + "/tui && go build -o ~/.local/bin/mess-tui .")
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			os.Exit(ee.ExitCode())
		}
		return fmt.Errorf("running mess-tui: %w", err)
	}
	return nil
}

// repoHint names where the source probably is, for the build instruction above.
func repoHint() string {
	if wd, err := os.Getwd(); err == nil {
		if _, err := os.Stat(wd + "/tui/go.mod"); err == nil {
			return wd
		}
	}
	return "<mess repo>"
}
