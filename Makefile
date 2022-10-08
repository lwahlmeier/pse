VERSION ?= 0.0.0-local
GO_VERSION ?= 1.19
GO_FILES := $(shell find ./* -iname '*.go')
GO_RUN := docker run --rm -e GO111MODULE=on -e CGO_ENABLED=0 -e GOOS=linux -e GOARCH=amd64 -e VERSION=$(VERSION) -e HOME=/tmp -u $(id -u ${USER}):$(id -g ${USER}) -v $$PWD:/build -w /build golang:$(GO_VERSION)

build: build-docker

build-docker: bin/pse-$(VERSION)
	docker build --build-arg VERSION=$(VERSION) . -t lwahlmeier/pse:$(VERSION)

bin/pse-$(VERSION): $(GO_FILES)
	mkdir -p ./bin
	$(GO_RUN) go build -v -mod=vendor -ldflags="-s -w -X main.version=$(VERSION)" -a -o ./bin/pse-$(VERSION)

clean:
	rm -rf ./bin
