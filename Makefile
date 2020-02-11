.PHONY: test exec cover devtools env cloc proto

GOMOD:=git.uestc.cn/sunmxt/sladder

PROJECT_ROOT:=$(shell pwd)
BUILD_DIR:=build
VERSION:=$(shell cat VERSION)
REVISION:=$(shell git rev-parse HEAD)
export GOPATH:=$(PROJECT_ROOT)/$(BUILD_DIR)
export PATH:=$(PROJECT_ROOT)/bin:$(GOPATH)/bin:$(PATH)

COVERAGE_DIR:=coverage

all: test

$(COVERAGE_DIR):
	mkdir -p $(COVERAGE_DIR)

cover: coverage test
	go tool cover -html=$(COVERAGE_DIR)/coverage.out -o $(COVERAGE_DIR)/coverage.html

test: coverage
	go test -v -coverprofile=$(COVERAGE_DIR)/coverage.out -cover ./...
	go tool cover -func=$(COVERAGE_DIR)/coverage.out

build:
	mkdir build

$(GOPATH)/bin:
	test -d "$(GOPATH)/bin" || make "$(GOPATH)/bin"

env:
	@echo "export PROJECT_ROOT=\"$(PROJECT_ROOT)\""
	@echo "export GOPATH=\"\$${PROJECT_ROOT}/build\""

cloc:
	cloc . --exclude-dir=build,bin,ci,mocks

proto: $(GOPATH)/bin/protoc-gen-go
	protoc -I=$(PROJECT_ROOT) --go_out=$(PROJECT_ROOT) proto/core.proto
	find -E . -name '*.pb.go' -type f -not -path './build/*' | xargs sed -i '' "s|\"proto\"|\"$(GOMOD)/proto\"|g"

devtools: $(GOPATH)/bin/gopls $(GOPATH)/bin/goimports

exec:
	$(CMD)

$(GOPATH)/bin/protoc-gen-go: $(GOPATH)/bin
	go get -u github.com/golang/protobuf/protoc-gen-go

$(GOPATH)/bin/gopls: $(GOPATH)/bin
	go get -u golang.org/x/tools/gopls

$(GOPATH)/bin/goimports: $(GOPATH)/bin
	go get -u golang.org/x/tools/cmd/goimports