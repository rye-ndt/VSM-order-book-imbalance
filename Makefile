APP_NAME := order-book-imbalance
BIN_DIR := bin

.PHONY: run build test tidy

run:
	go run ./cmd/app

build:
	mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(APP_NAME) ./cmd/app

test:
	go test ./...

tidy:
	go mod tidy

