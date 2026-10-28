# KubeRay Development Guide

This guide provides an overview of the different components in the KubeRay project and instructions
for developing and testing each component. Most developers will be concerned with the KubeRay
Operator; the other components are optional.

## Pre-commit Hooks

We use [pre-commit] to lint and format code before each commit.

1. Install [pre-commit]
1. Run `pre-commit install` to install the pre-commit hooks

## KubeRay Operator

The KubeRay Operator is responsible for managing Ray clusters on Kubernetes.
To learn more about developing and testing the KubeRay Operator, please refer to the
[Operator Development Guide].

## KubeRay APIServer

The KubeRay APIServer is a central component that exposes the KubeRay API for managing Ray clusters.
For more information about developing and testing the KubeRay APIServer, please refer to the
[APIServer Development Guide].

## KubeRay Python client

The KubeRay Python client library provides APIs to handle RayCluster from your Python application.
For more information about developing and testing the KubeRay Python client, please refer to the
[Python Client] and [Python API Client].

## Proto and OpenAPI

KubeRay uses Protocol Buffers (protobuf) and OpenAPI specifications to define the API and data
structures. For more information about developing and testing proto files and OpenAPI
specifications, please refer to the [Proto and OpenAPI Development Guide].

## KubeRay Kubectl Plugin (beta)

A [kubectl plugin] that simplifies common workflows when deploying Ray on Kubernetes. If
you aren't familiar with Kubernetes, this plugin simplifies running Ray on Kubernetes.
For more information about developing and testing the KubeRay Kubectl Plugin, please refer to the
[Kubectl Plugin Development Guide].

## Deploying Documentation Locally

To preview the KubeRay documentation locally, follow these steps:

- Make sure you have Docker installed on your machine.
- Open a terminal and navigate to the root directory of your KubeRay repository.
- Run the following command:

```sh
docker run --rm -it -p 8000:8000 -v ${PWD}:/docs squidfunk/mkdocs-material
```

- Open your web browser and navigate to <http://127.0.0.1:8000/kuberay/> to view the documentation.

If you make any changes to the documentation files, the local preview will automatically update to
reflect those changes.

[pre-commit]: https://pre-commit.com/
[kubectl plugin]: https://kubernetes.io/docs/tasks/extend-kubectl/kubectl-plugins/
[Kubectl Plugin Development Guide]: ../../kubectl-plugin/DEVELOPMENT.md
[Python Client]: https://github.com/ray-project/kuberay/blob/master/components/pythonclient.md
[Python API Client]: https://github.com/ray-project/kuberay/blob/master/components/pythonapiclient.md
[APIServer Development Guide]: https://github.com/ray-project/kuberay/blob/master/apiserver/DEVELOPMENT.md
[Proto and OpenAPI Development Guide]: https://github.com/ray-project/kuberay/blob/master/proto/README.md
[Operator Development Guide]: https://github.com/ray-project/kuberay/blob/master/ray-operator/DEVELOPMENT.md
