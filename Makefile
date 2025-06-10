APP_NAME=aggregator
CMD_DIR=cmd/aggregator
BIN_DIR=bin
BIN_PATH=$(BIN_DIR)/$(APP_NAME)

.PHONY: all build test run clean

all: build

build:
	go build -o $(BIN_PATH) ./${CMD_DIR}

test:
	go test ./...

run: build
	$(BIN_PATH)

clean:
	rm -rf $(BIN_DIR)
