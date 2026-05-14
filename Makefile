# Цели для локальной разработки (GNU Make: Git for Windows, MSYS2, Linux, macOS).
# Если в родительской папке есть go.work без этого модуля, перед make задайте: export GOWORK=off (bash) или $env:GOWORK='off' (PowerShell).

.PHONY: fmt vet tidy test test-race build run lint help

GO   ?= go
PORT ?= 8080

help:
	@echo "Цели: fmt vet tidy test test-race build run lint"

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

test:
	$(GO) test -v ./...

test-race:
	$(GO) test -race -v ./...

build:
	$(GO) build -trimpath -ldflags="-s -w" -o queuebroker .

run:
	$(GO) run . $(PORT)

lint:
	golangci-lint run --timeout=5m
