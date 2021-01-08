all: fmt test bench

.PHONY: fmt
fmt:
	go fmt ./pkg/...

.PHONY: test
test:
	go test ./pkg/... -count=1

.PHONY: bench
bench:
	go test ./... -bench=. -benchmem

.PHONY: bench-%
bench-%:
	go test ./... -bench=$* -benchmem
