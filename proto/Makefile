# Makefile to generate api clients from proto.

IMAGE_NAME ?= kuberay/proto-generator
IMAGE_TAG ?= $(shell git rev-parse HEAD)

OUTPUT_MODE=import
TMP_OUTPUT=/tmp

.PHONY: generate
generate: build-image
	docker run -it --rm \
		--user $$(id -u):$$(id -g) \
		--mount type=bind,source="$$(pwd)/..",target=/go/src/github.com/ray-project/kuberay/ \
		$(IMAGE_NAME):$(IMAGE_TAG) /go/src/github.com/ray-project/kuberay/proto/hack/generate.sh

.PHONY: build-image
build-image:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) -f Dockerfile .

.PHONY: generate-cmd
generate-cmd:
	# code is exact same to scripts ./hack/generate.sh
	mkdir -p go_client && mkdir -p swagger && \
	protoc -I ./ \
		-I ./third_party/ --go_out ${TMP_OUTPUT} --go_opt paths=${OUTPUT_MODE} \
		--go-grpc_out ${TMP_OUTPUT} --go-grpc_opt paths=${OUTPUT_MODE} \
		--grpc-gateway_out ${TMP_OUTPUT}  --grpc-gateway_opt paths=${OUTPUT_MODE} \
		--openapiv2_opt logtostderr=true --openapiv2_out=:swagger ./*.proto && \
	cp ${TMP_OUTPUT}/github.com/ray-project/kuberay/proto/go_client/* ./go_client

.PHONY: clean
clean:
	rm -rf go_client && rm -rf swagger
