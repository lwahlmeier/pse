GO_VERSION ?= 1.19
GO_FILES := $(shell find ./* -iname '*.go')
GO_RUN := docker run --rm -e GO111MODULE=on -e HOME=/build/.cache -u $$(id -u $${USER}):$$(id -g $${USER}) -v $$PWD:/build -w /build golang:$(GO_VERSION)
.PHONY: vendor test

test:
	$(GO_RUN) go test -count=1 -v -cover ./...

vendor:
	$(GO_RUN) go mod vendor
	$(GO_RUN) go mod tidy
