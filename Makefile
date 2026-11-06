.PHONY: build test vet fmt bench demo clean run

build:
	go build ./...

test: vet
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -l cmd internal

bench:
	go test ./internal/store/ ./internal/wal/ -bench . -benchtime 100000x
	go test ./internal/api/ -bench . -benchtime 10000x

demo:
	./scripts/demo.sh

run:
	go run ./cmd/server

clean:
	rm -rf bin data data1 data2 data3
	go clean -testcache
