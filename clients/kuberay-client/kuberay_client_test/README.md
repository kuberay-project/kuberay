# Overview

## For developers

1. `pip install -U pip setuptools`
1. `cd clients/kuberay-client && pip install -e .`

Uninstall with `pip uninstall kuberay-client`.

## For testing run

`python -m unittest discover 'clients/kuberay-client/kuberay_client_test/'`

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
