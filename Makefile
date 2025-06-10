APP_NAME=aggregator
CMD_DIR=cmd/aggregator
BIN_DIR=bin
BIN_PATH=$(BIN_DIR)/$(APP_NAME)

.PHONY: all build test run clean gosec

all: build

build:
	go build -o $(BIN_PATH) ./${CMD_DIR}

test:
	go test -race ./... -coverpkg=./... -count=1 -coverprofile test-coverage.out

run: build
	$(BIN_PATH)

gosec:
	gosec ./...

clean:
	rm -rf $(BIN_DIR)
