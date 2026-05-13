cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out

test:
	go test -v ./...

build:
	go build -o bin/server ./cmd/server
	go build -o bin/worker ./cmd/worker

check:
	gofmt -l .
	go test ./...

fix_check:
	gofmt -w .

docker-build:
	docker compose build
