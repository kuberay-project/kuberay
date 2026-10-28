# Container Images

Images for the various KubeRay components are published at the following locations:

1. [Quay.io](https://quay.io/organization/kuberay)
2. [DockerHub](https://hub.docker.com/u/kuberay)

We recommend using Quay.io as the primary source for images as there are image-pull restrictions on
DockerHub. DockerHub allows you to pull only 100 images per 6 hour window. Refer to [DockerHub rate
limiting] for more details.

## Stable versions

For stable releases, use version tags (e.g. `quay.io/kuberay/operator:v1.1.0`).

## Master commits

The first seven characters of the git SHA specify images built from specific commits
(e.g. `quay.io/kuberay/operator:4892ac1`).

## Nightly images

The nightly tag specifies images built from the most recent master (e.g. `quay.io/kuberay/operator:nightly`).

[DockerHub rate limiting]: https://docs.docker.com/docker-hub/download-rate-limit/
