# Protocol Buffer API Definitions

This directory contains API definitions written in Protocol Buffers. These definitions will be used for building services.

## Prerequisites

To successfully run the generation commands, ensure the following tools are installed in your environment:

- [Make](https://man7.org/linux/man-pages/man1/make.1.html)
- [Docker](https://www.docker.com/)

Run the following command to build the proto-generator image. This image will be used for generating APIs:

> Note: You can actually skip this step, and a prebuilt image will be downloaded instead.

```bash
make build-image
```

## Automatic Generation

### Go Client and Swagger

```bash
make generate
```

### Clients

We postpone the client generation until there is a need for external service communication.

### API Reference Documentation

Use the tools [bootprint-openapi] and [html-inline] to generate API reference documentation from the
Swagger files. For more information on the API and architecture, refer to the [design document].

### Third-Party Protos

Third-party proto dependencies are synchronized back to `proto/third_party` for easier development
(IDE friendly). Ideally, the directory for searching imports should be specified instead.

```bash
protoc -I.
    -I/go/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
    -I/go/src/github.com/grpc-ecosystem/grpc-gateway/ \
    -I/go/src/github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger/options/ \
```

Sources:

- [googleapis](https://github.com/googleapis/googleapis/tree/master/google/api)
- [protoc-gen-openapiv2](https://github.com/grpc-ecosystem/grpc-gateway/tree/master/protoc-gen-openapiv2/options)

[bootprint-openapi]: https://github.com/bootprint/bootprint-monorepo/tree/master/packages/bootprint-openapi
[html-inline]: https://github.com/substack/html-inline
[design document]: ../docs/design/protobuf-grpc-service.md
