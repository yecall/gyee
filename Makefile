
THIS_MAKEFILE := $(abspath $(lastword $(MAKEFILE_LIST)))
SRC_ROOT := $(patsubst %/,%,$(dir ${THIS_MAKEFILE}))

OUTPUT := ${SRC_ROOT}/build
OUT_BIN := ${OUTPUT}/bin

GIT_COMMIT := $(shell git rev-parse HEAD)

GO_LD_FLAGS=-ldflags "-X github.com/yeeco/gyee/version.GitCommit=${GIT_COMMIT}"

.PHONY: all clean env bootnode gyee

gyee: env
	@mkdir -p '${OUT_BIN}'
	go build -o '${OUT_BIN}/gyee' ${GO_LD_FLAGS} '${SRC_ROOT}/cmd/gyee'
	@echo "Done building gyee"

all: bootnode gyee
	@echo "Done building all"

bootnode: env

clean: env
	@rm -fr "${OUTPUT}"
	@echo "Done cleaning ${OUTPUT}"

env:
	@echo "SRC_ROOT = ${SRC_ROOT}"
	@echo "OUTPUT = ${OUTPUT}"
	@echo "COMMIT = ${GIT_COMMIT}"
