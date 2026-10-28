# Generate client code (go & json) from API protocol buffers
FROM golang:1.26-bookworm AS generator

ENV PROTOC_VERSION 29.2
ENV GOLANG_PROTOBUF_VERSION v1.36.1
ENV GRPC_GATEWAY_VERSION v2.26.0
ENV GRPC_GATEWAY_OPENAPI_VERSION v2.26.0
ENV GRPC_VERSION v1.75.0
ENV GOLANG_GRPC_VERSION v1.5.1

ENV GOBIN=/go/bin

# Install protoc.
USER root
RUN wget https://github.com/jqlang/jq/releases/download/jq-1.7.1/jq-linux64 -O /usr/bin/jq && chmod +x /usr/bin/jq
RUN curl -L -o protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip
RUN apt-get update && apt-get install -y unzip
RUN unzip -o protoc.zip -d /usr/ bin/protoc
RUN unzip -o protoc.zip -d /usr/ 'include/*'
RUN rm -f protoc.zip
ENV PROTOCCOMPILER /usr/bin/protoc
ENV PROTOCINCLUDE /usr/include/google/protobuf

# Install protoc-gen-go && protoc-gen-go-grpc
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@$GOLANG_PROTOBUF_VERSION
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$GOLANG_GRPC_VERSION
# These go binaries are installed under /go/bin
ENV PATH /go/bin:${PATH}

# Install protoc-gen-rpc-gateway && protoc-gen-openapiv2.
RUN go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@$GRPC_GATEWAY_VERSION
RUN go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@$GRPC_GATEWAY_OPENAPI_VERSION

# WORKAROUND: https://github.com/docker-library/golang/issues/225#issuecomment-403170792
ENV XDG_CACHE_HOME /tmp/.cache
# Make all files accessible to non-root users.
RUN chmod -R 775 /usr/bin/
RUN chmod -R 775 /usr/include/google
RUN chmod -R 775 /go
USER 65532:65532
