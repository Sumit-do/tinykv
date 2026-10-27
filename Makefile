BINARY  := tinykv
PKG     := ./cmd/tinykv
IMAGE   := tinykv:latest
DATA    ?= data
ADDR    ?= :7700
FSYNC   ?= always

.PHONY: all build test bench lint fmt vet run docker docker-run clean

all: lint test build

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/$(BINARY) $(PKG)

test:
	go test -race -count=1 ./...

bench:
	go test -run '^$$' -bench . -benchmem ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

lint: vet
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

run: build
	./bin/$(BINARY) serve --dir $(DATA) --addr $(ADDR) --fsync $(FSYNC)

docker:
	docker build -t $(IMAGE) .

docker-run: docker
	docker run --rm -p 7700:7700 -v tinykv-data:/data $(IMAGE)

clean:
	rm -rf bin
