.PHONY: test exec cover devtools env cloc proto mock

GOMOD:=github.com/sunmxt/sladder

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

bin: 
	mkdir bin

build/bin: bin build
	test -d build/bin || ln -s $$(pwd)/bin build/bin

build:
	mkdir build

cover: coverage test
	go tool cover -html=$(COVERAGE_DIR)/coverage.out -o $(COVERAGE_DIR)/coverage.html

test: coverage
	go test -v -coverprofile=$(COVERAGE_DIR)/coverage.out -cover ./ ./engine/...
	go tool cover -func=$(COVERAGE_DIR)/coverage.out


env:
	@echo "export PROJECT_ROOT=\"$(PROJECT_ROOT)\""
	@echo "export GOPATH=\"\$${PROJECT_ROOT}/build\""
	@echo "export PATH=\"\$${PROJECT_ROOT}/bin:$${PATH}\""

cloc:
	cloc . --exclude-dir=build,bin,ci,mocks --not-match-f=mock_* --exclude-ext=pb.go

proto: $(GOPATH)/bin/protoc-gen-go
	protoc -I=$(PROJECT_ROOT) --go_out=module=$(GOMOD):. proto/core.proto
	protoc -I=$(PROJECT_ROOT) --go_out=module=$(GOMOD):. engine/gossip/pb/pb.proto
	find -E . -name '*.pb.go' -type f -not -path './build/*' | xargs sed -i '' " \
		s|\"proto\"|\"$(GOMOD)/proto\"|g; \
		s|\"engine/gossip/pb\"|\"$(GOMOD)/engine/gossip/pb\"|g"

devtools: bin/gopls bin/goimports bin/protoc-gen-go bin/mockery

mock: bin/mockery
	bin/mockery -inpkg -name EngineInstance -case underscore
	bin/mockery -inpkg -name Engine -case underscore
	bin/mockery -inpkg -name NodeNameResolver -case underscore
	bin/mockery -inpkg -name KVValidator -case underscore

exec:
	$(CMD)

bin/protoc-gen-go: build/bin
	go get -u github.com/golang/protobuf/protoc-gen-go

bin/gopls: build/bin
	go get -u golang.org/x/tools/gopls

bin/goimports: build/bin
	go get -u golang.org/x/tools/cmd/goimports

bin/mockery:
	go get -u github.com/vektra/mockery/cmd/mockery