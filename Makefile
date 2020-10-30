BINARY=mysql-tarantool-replicator
VERSION=`git describe --tags --dirty --always`
COMMIT=`git rev-parse HEAD`
BUILD_DATE=`date +%FT%T%z`
LDFLAGS=-ldflags "-w -s -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildDate=${BUILD_DATE}"

all: build

.PHONY: build
build:
	go build ${LDFLAGS} -o bin/${BINARY} cmd/replicator/main.go

.PHONY: run
run: build
	bin/${BINARY} -config=configs/dev.yml

.PHONY: run_short_tests
run_short_tests:
	go test -count=1 -v -short ./...