SHELL := /bin/bash
BINARY=mysql-tarantool-replicator
VERSION=`git describe --tags --dirty --always`
COMMIT=`git rev-parse HEAD`
BUILD_DATE=`date +%FT%T%z`
LDFLAGS=-ldflags "-w -s -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildDate=${BUILD_DATE}"

all: build

.PHONY: build
build:
	go build ${LDFLAGS} -o bin/${BINARY} cmd/replicator/main.go

.PHONY: lint
lint:
	golangci-lint run -v ./...

.PHONY: run
run: build
	bin/${BINARY} -config=configs/dev.yml

.PHONY: run_short_tests
run_short_tests:
	go test -count=1 -v -short ./...

.PHONY: run_tests
run_tests: env_up
	go test -p=1 -count=1 -v -race ./...

.PHONY: env_up
env_up:
	docker-compose up -d
	./docker/wait.sh
	docker-compose ps

.PHONY: env_down
env_down:
	docker-compose down -v --rmi local --remove-orphans