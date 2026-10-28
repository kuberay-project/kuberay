<!-- markdownlint-disable MD013 -->
# Helm charts release

We host all Helm charts on [kuberay-helm](https://github.com/ray-project/kuberay-helm).
This document describes the process for release managers to release Helm charts.

## The end-to-end workflow

### Step 1: Update versions in Chart.yaml and values.yaml files

Please update the value of `version` in [ray-cluster/Chart.yaml](https://github.com/ray-project/kuberay/blob/master/helm-chart/ray-cluster/Chart.yaml),
[kuberay-operator/Chart.yaml](https://github.com/ray-project/kuberay/blob/master/helm-chart/kuberay-operator/Chart.yaml),
and [kuberay-apiserver/Chart.yaml](https://github.com/ray-project/kuberay/blob/master/helm-chart/kuberay-apiserver/Chart.yaml)
to the new release version (e.g. 0.4.0).

Also make sure `image.tag` has been updated in [kuberay-operator/values.yaml](https://github.com/ray-project/kuberay/blob/master/helm-chart/kuberay-operator/values.yaml) and [kuberay-apiserver/values.yaml](https://github.com/ray-project/kuberay/blob/master/helm-chart/kuberay-apiserver/values.yaml).

### Step 2: Copy the helm-chart directory from kuberay to kuberay-helm

In [kuberay-helm CI](https://github.com/ray-project/kuberay-helm/blob/main/.github/workflows/chart-release.yaml), `helm/chart-releaser-action` will create releases for all charts in the directory `helm-chart` and update `index.yaml` in the [gh-pages](https://github.com/ray-project/kuberay-helm/tree/gh-pages) branch when the PR is merged into `main`. Note that `index.yaml` is necessary when you run the command `helm repo add`. I recommend removing the `helm-chart` directory in the kuberay-helm repository and creating a new one by copying from the kuberay repository.

### Step 3: Validate the charts

When the PR is merged into `main`, the releases and `index.yaml` will be generated.
You can validate the charts as follows:

* Confirm that the [releases](https://github.com/ray-project/kuberay-helm/releases) are created as expected.
* Confirm that [index.yaml](https://github.com/ray-project/kuberay-helm/blob/gh-pages/index.yaml) exists.
* Confirm that [index.yaml](https://github.com/ray-project/kuberay-helm/blob/gh-pages/index.yaml) has the metadata of all releases, including old versions.
* Check the creation/update time of all releases and `index.yaml` to ensure they are updated.

* Install charts from Helm repository.

    ```sh
    helm repo add kuberay https://ray-project.github.io/kuberay-helm/
    helm repo update

    # List all charts
    helm search repo kuberay

    # Install charts (If you want to install charts for a release candidate, add `--version vX.Y.Z-rc.0` to the command below.)
    helm install kuberay-operator kuberay/kuberay-operator
    helm install kuberay-apiserver kuberay/kuberay-apiserver
    helm install raycluster kuberay/ray-cluster
    ```

## Delete the existing releases

`helm/chart-releaser-action` does not encourage users to delete existing releases;
thus, `index.yaml` will not be updated automatically after the deletion.
If you really need to do that, please read this section carefully before you do that.

* Delete the [releases](https://github.com/ray-project/kuberay-helm/releases)
* Remove the related tags using the following command. If tags are not properly removed, you may run into the problem described in [ray-project/kuberay/#561](https://github.com/ray-project/kuberay/issues/561).

    ```sh
    # git remote -v
    # upstream        git@github.com:ray-project/kuberay-helm.git (fetch)
    # upstream        git@github.com:ray-project/kuberay-helm.git (push)

    # The following command deletes the tag "ray-cluster-0.4.0".
    git push --delete upstream ray-cluster-0.4.0
    ```

* Remove `index.yaml`
* Trigger kuberay-helm CI again to create new releases and a new index.yaml.
* Follow "Step 3: Validate the charts" to test it.
