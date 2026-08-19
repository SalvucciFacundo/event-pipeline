BINARY := bin/event-pipeline

.PHONY: build test vet race cover tidy run frontend-build

build:
	go build -o $(BINARY) ./cmd/event-pipeline

test:
	go test ./...

vet:
	go vet ./...

race:
	go test -race ./...

cover:
	go test -race -cover ./...

tidy:
	go mod tidy

run:
	go run ./cmd/event-pipeline

frontend-build:
	cd web && npm run build