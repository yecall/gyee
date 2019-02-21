
THIS_MAKEFILE := $(abspath $(lastword $(MAKEFILE_LIST)))
SRC_ROOT := $(patsubst %/,%,$(dir ${THIS_MAKEFILE}))

OUTPUT := ./build
OUT_BIN := ${OUTPUT}/bin

GIT_COMMIT := $(shell git rev-parse HEAD)

GO_LD_FLAGS=-ldflags "-X github.com/yeeco/gyee/version.GitCommit=${GIT_COMMIT}"

.PHONY: all
all: bootnode gyee
	@echo "Done building all"

#
# CMD targets
#

.PHONY: bootnode
bootnode: ${OUT_BIN}/bootnode

.PHONY: gyee
gyee: ${OUT_BIN}/gyee

${OUT_BIN}/%: env
	@mkdir -p '${OUT_BIN}'
	go build -o $@ ${GO_LD_FLAGS} ./cmd/$(@F)
	@echo "Done building cmd $(@F)"

#
# Misc
#

.PHONY: protobufgen
protobufgen: env
	$(MAKE) -C core/pb clean all
	$(MAKE) -C p2p/discover/udpmsg/pb clean all
	$(MAKE) -C p2p/dht/pb clean all
	$(MAKE) -C p2p/peer/pb clean all
	$(MAKE) -C rpc/pb clean all

.PHONY: clean env
clean: env
	@rm -fr "${OUTPUT}"
	@echo "Done cleaning ${OUTPUT}"

env:
	@echo "SRC_ROOT = ${SRC_ROOT}"
	@echo "OUTPUT = ${OUTPUT}"
	@echo "COMMIT = ${GIT_COMMIT}"
