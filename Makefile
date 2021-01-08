all: fmt test bench

.PHONY: fmt
fmt:
	go fmt ./pkg/...

.PHONY: test
test:
	go test ./pkg/...

.PHONY: bench
bench:
	go test ./... -bench=. -benchmem

