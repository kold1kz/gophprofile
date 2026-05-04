cover:
	go test -coverprofile=coverage.out $(shell go list ./... | grep -v -E '(/proto|/mocks|/cmd/staticlint|/cmd/workload)')
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out

test:
	go test -v ./...

build:
	go build -o gophkeeper ./cmd/gophkeeper/main.go
	./gophkeepertest -test.v -test.run=^TestIteration1$ -binary-path=cmd/gophkeeper/gophkeeper

check:
	goimports -l .
	gofmt -l .
	go test -v ./...

fix_check:
	goimports -w .
	gofmt -w .

staticlint:
	go run ./cmd/server ./...
	go run ./cmd/worker ./...

reset:
	go run ./cmd/reset

proto:
	rm -f proto/gophkeeper.pb.go proto/gophkeeper_grpc.pb.go
	protoc \
      -I . \
      --go_out=. --go_opt=paths=source_relative \
      --go-grpc_out=. --go-grpc_opt=paths=source_relative \
      --go_opt=default_api_level=API_OPAQUE \
      proto/gophkeeper.proto
