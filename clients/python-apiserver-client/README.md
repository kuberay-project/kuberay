<!-- markdownlint-disable MD013 -->
# Python client for KubeRay API server

This Python client is currently only supporting Ray cluster management through usage of the `API server` Ray API. It implements all of the current functionality of the API server (and the operator) and provide pythonic APIs to the capabilities.

The package supports Python objects (documented in the code) that can be used to build and receive payloads for creation, listing and deleting of [template](https://ray-project.github.io/kuberay/components/apiserver/#compute-template) and Ray clusters.

The main class of the package is [KubeRayAPIs](src/python_apiserver_client/kuberay_apis.py) that implements all of the functionality. It leverages [templates](src/python_apiserver_client/params/templates.py) and [cluster](src/python_apiserver_client/params/cluster.py) definitions, allowing to specify all required parameters as straight Python classes. Additional (intermediate) definitions are provided (see [cluster](src/python_apiserver_client/params/cluster.py), [environment variables](src/python_apiserver_client/params/environmentvariables.py), [volumes](src/python_apiserver_client/params/volumes.py), [head group](src/python_apiserver_client/params/headnode.py), [worker group](src/python_apiserver_client/params/workernode.py) and [job submission](src/python_apiserver_client/params/jobsubmission.py))

## Prerequisites

It is also expected that the `kuberay operator` ([Installation instructions are here.](https://github.com/ray-project/kuberay#quick-start)) and `api server` ([Installation instructions are here.](https://ray-project.github.io/kuberay/components/apiserver)) are installed

## Development

Start by installing `setup-tools`

```shell
pip3 install -U setuptools
```

Now install our library locally. From the directory `path/to/kuberay/clients/python_apiserver_client` execute

```shell
pip3 install -e .
```

## Testing

To do testing first create the current images for operator and API server, create kind cluster and install operator and API server.
From apiserver directory, execute:

```shell
make operator-image cluster load-operator-image deploy-operator docker-image load-image install
```

Now you can use the following test files:

* [parameters_test](test/api_params_test.py) exercise parameter creation
* [api_test](test/kuberay_api_test.py) exercise overall package functionality and can also be used as a guide for API usage.

## Clean up

From apiserver directory, execute:

```shell
make clean-cluster
```
