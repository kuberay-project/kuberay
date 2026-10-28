# KubeRay APIServer v2 Migration Plan

## Target Customers

- Advanced and early adopters who currently use the APIServer v1 in production
- Infrastructure and platform engineers who have customized or extended the KubeRay APIServer for internal tooling or environments

## Overview

KubeRay APIServer v2 introduces a more maintainable, Kubernetes-native, and flexible interface for managing Ray clusters.

In v1, exposing new fields required modifying protobuf definitions, regenerating HTTP/gRPC clients, and updating
tests — a time-consuming and error-prone process that often delayed support for new features. This manual
synchronization between CRDs and protobuf definitions added significant maintenance overhead and slowed
developer velocity.

With v2, we eliminate these bottlenecks by directly reusing the OpenAPI schema defined in the Kubernetes CRDs.
Instead of manually defining fields or generating new clients, APIServer v2 acts as a transparent HTTP proxy to the
Kubernetes API server. All CRD fields are exposed by default, and advanced behaviors such as compute template
injection, default values, and mutations can be implemented using user-defined middleware functions (UDFs).

To simplify the system further, gRPC support has been removed in favor of HTTP-only APIs. This approach aligns
better with native Kubernetes tooling and improves extensibility, maintainability, and onboarding for
infrastructure engineers. This document outlines the major changes from APIServer v1 to v2.
existing workflows.

## What’s Changed v1 vs v2

| Category                    | v1 (Legacy)                                                                 | v2 (New)                                                                                      |
|-----------------------------|------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| API protocol                | HTTP + gRPC                                                                  | HTTP only (gRPC removed)                                                                      |
| Field exposure model        | Manually defined in protobuf + requires codegen                              | Auto-reflected from Kubernetes CRD OpenAPI schema                                             |
| Client code generation      | Required (for HTTP and gRPC clients)                                         | Not required (simple HTTP proxy with path rewrite)                                            |
| Adding new fields           | Requires protobuf updates, codegen, and tests                                | No protobuf needed; all fields exposed automatically via OpenAPI + optional UDF              |
| Compute template support    | Requires hardcoded logic in API server                                       | Handled via HTTP middleware/UDF; official support starts in Stage 2                          |
| Pagination support          | Manual implementation required                                               | Natively supported using Kubernetes API pagination                                            |
| Documentation policy        | Partial or undocumented                                                      | Official documentation includes only v2                                                       |
| Maintenance and extensibility | High overhead: PR needed per new field, manual sync with operator         | Lightweight: CRD fields auto-reflected, extensible via middleware, 50% less maintenance       |

## Migration Plan

The migration from v1 to v2 will occur in three stages:

- Stage 1 – v2 released and validated
  - v2 becomes publicly available and feature-complete
  - All major features from v1 are supported in v2
  - gRPC support is not included in v2, but remains available in v1 during the deprecation period
  - Integration tests are reused where applicable
  - Middleware-based compute template support is deferred to Stage 2
- Stage 2 – v1 deprecated (v1 and v2 coexist)
  - v2 becomes the primary supported interface
  - v1 remains for compatibility but receives no new features
  - Warning messages may be surfaced for v1 usage
  - A proposal is under consideration to refactor the API server as a reusable SDK (e.g., apiserversdk)
- Stage 3 – v1 fully removed
  - All v1-related code and tests are deleted
  - v2 becomes the only supported API server implementation

## Dev Progress

- Not all v2 features will ship in the kuberay v1.4 release
- Essential features, such as creating KubeRay CR, from v1 are supported

## Issue Report

- When filing GitHub issues, please tag them with apiserver-v1 or v2
- If no version tag is added, or the issue content does not mention a version, we’ll assume it refers to v2
