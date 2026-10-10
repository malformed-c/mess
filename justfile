# Go-focused workflows, ported from periapsis's justfile (engi, 2026-10-10).
# mess has no Makefile — just is the only task runner, so build is here too.
#
# golangci-lint must be on PATH; it is not a go tool, so `go run` cannot reach
# it. periapsis pins v2.13.2; this tree runs the distro build (v2.14.0).
set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
  @just --list

build:
  go build -o mess .

install: build
  # The running daemon exec-holds ~/.local/bin/mess ("Text file busy" until it
  # stops), so the daemon restart is part of every install.
  go install . && cp "$(go env GOPATH)/bin/mess" ~/.local/bin/mess

fmt:
  test -z "$(gofmt -l .)"

vet:
  go vet ./...

# Whitespace style, through golangci-lint's bundled wsl_v5. See .golangci.yaml.
wsl:
  golangci-lint run --enable-only=wsl_v5 ./...

wsl-fix:
  golangci-lint run --fix --enable-only=wsl_v5 ./...

modernize:
  golangci-lint run --enable-only=modernize ./...

modernize-fix:
  golangci-lint run --fix --enable-only=modernize ./...

lint:
  golangci-lint run ./...

# Apply the rules rather than report them. Mechanical, and reviewed as its own
# commit.
lint-fix:
  golangci-lint run --fix ./...

test:
  go test ./...

race:
  go test -race ./...

# The same combination CI would run — -race AND the lint set together.
check: fmt vet lint test race

bench:
  go test ./... -run '^$' -bench . -benchmem
