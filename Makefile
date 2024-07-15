# SRC_ROOT is the top of the source tree.
SRC_ROOT := $(shell git rev-parse --show-toplevel)
TOOLS_BIN_DIR    := $(SRC_ROOT)/../opentelemetry-collector-contrib/.tools
GOTEST_TIMEOUT?=240s
GOTEST_OPT?= -race -timeout $(GOTEST_TIMEOUT)
GO               := go
GOIMPORTS        := $(TOOLS_BIN_DIR)/goimports

.PHONY: fmt generate test

fmt: $(GOIMPORTS)
	gofmt  -w -s ./
	$(GOIMPORTS) -w -local github.com/nicolastakashi/metricsdedupprocessor ./

generate:
	$(GO) generate ./...
	$(MAKE) fmt

tidy:
	$(GO) mod tidy -compat=1.21

test:
	$(GO) test $(GOTEST_OPT) ./...