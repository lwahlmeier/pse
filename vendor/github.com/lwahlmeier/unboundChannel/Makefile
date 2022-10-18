GO_VERSION ?= 1.19
GO_FILES := $(shell find ./* -iname '*.go')
DOCKER_EXEC := docker run --rm -e HOME=/build/.cache -u $$(id -u $${USER}):$$(id -g $${USER}) -v $$PWD:/build -w /build
GO_RUN := $(DOCKER_EXEC) golang:$(GO_VERSION)
GO_LINT := $(DOCKER_EXEC) golangci/golangci-lint:v1.50.0 golangci-lint run

test:
	$(GO_RUN) go test -count=10 -cover ./...

lint:
	$(GO_LINT)

clean:
	rm -rf ./bin
