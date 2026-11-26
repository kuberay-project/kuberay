# Overview

## For developers

1. `pip install -U pip setuptools`
1. `cd clients/kuberay-client && pip install -e .`

Uninstall with `pip uninstall kuberay-client`.

## For testing run

From `clients/kuberay-client`, install the generation dependencies and run the tests:

```bash
pip install -e . pytest -r scripts/requirements-python-models.txt
pytest kuberay_client_test/ -v
```

The API integration tests require a Kubernetes cluster and the KubeRay operator. To run
the generation and builder tests without a cluster:

```bash
pytest -q kuberay_client_test/test_generate_python_models.py kuberay_client_test/test_job_builder.py \
    kuberay_client_test/test_utils.py kuberay_client_test/test_director.py
```

### Coverage report

#### Pre-requisites

* `sudo apt install libsqlite3-dev`
* `pyenv install 3.6.5` # or your Python version
* `pip install db-sqlite3 coverage`

__To gather data__
`python -m coverage run -m unittest`

__to generate a coverage report__
`python -m coverage report`

__to generate the test coverage report in HTML format__
`python -m coverage html`
