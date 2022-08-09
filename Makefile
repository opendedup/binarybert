PWD := $(shell pwd)
GOPATH := $(shell go env GOPATH)

GOARCH := $(shell go env GOARCH)
GOOS := $(shell go env GOOS)

VERSION ?= $(shell git describe --tags)
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
HASH := $(shell git rev-parse HEAD)
TAG ?= "sdfs/bertchunker:$(VERSION)"

all: build

checks:
	@echo "Checking dependencies"
	@(env bash $(PWD)/buildscripts/checkdeps.sh)

getdeps:
	@mkdir -p ${GOPATH}/bin
	@which golangci-lint 1>/dev/null || (echo "Installing golangci-lint" && curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.27.0)

crosscompile:
	@(env bash $(PWD)/buildscripts/cross-compile.sh)

verifiers: getdeps fmt lint

fmt:
	@echo "Running $@ check"
	@GO111MODULE=on gofmt -d cmd/
	@GO111MODULE=on gofmt -d pkg/

lint:
	@echo "Running $@ check"
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint cache clean
	@GO111MODULE=on ${GOPATH}/bin/golangci-lint run --timeout=5m --config ./.golangci.yml



# Builds bertchunker locally.
build:
	@mkdir -p $(PWD)/build
	@echo "Building bertchunker binary to '$(PWD)/build/bertchunker'"
	@go build -ldflags="-X 'main.Version=$(BRANCH)' -X 'main.BuildDate=$$(date -Iseconds)'" -o ./build/bertchunker cmd/chunker/*
	@env GOARCH=amd64 GOOS=windows go build -ldflags="-X 'main.Version=$(BRANCH)' -X 'main.BuildDate=$$(date -Iseconds)'" -o ./build/bertchunker.exe cmd/chunker/*

# Builds bertchunker and installs it to $GOPATH/bin.
install: build
	@echo "Installing bertchunker binary to '$(GOPATH)/bin/bertchunker'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/build/bertchunker $(GOPATH)/bin/bertchunker
	@echo "Installation successful. To learn more, try \"bertchunker\"."

clean:
	@echo "Cleaning up all the generated files"
	@find . -name '*.test' | xargs rm -fv
	@find . -name '*~' | xargs rm -fv
	@rm -rvf bertchunker
	@rm -rvf build
	@rm -rvf release
	@rm -rvf .verify*
