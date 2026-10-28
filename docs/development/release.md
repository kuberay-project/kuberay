<!-- markdownlint-disable MD013 -->
# KubeRay Release Process

## Prerequisite

You need write access to the [KubeRay repo](https://github.com/ray-project/kuberay) to cut a release branch and create a release tag.

## Overview

Each KubeRay minor release series (e.g., `v1.3.X`) is maintained on its own release branch.
For example, the `v1.3.X` releases are cut from the `release-1.3` branch.
Patch releases (e.g., `v1.3.1`) involve cherry-picking commits onto the corresponding release branch.
Release candidates (e.g., `v1.4.0-rc.0`) are usually cut from the `master` branch.

The high-level steps for a new minor or patch release are:

1. For new minor releases, create a release branch (e.g., `release-1.4`) from `master` if it doesn't exist.
2. Update KubeRay version references in the release branch.
3. Create and push a new git tag based on the target release version (e.g., `v1.4.0`).
4. Publish container images by running the `release-image-build` GitHub Actions workflow.
5. Publish kubectl plugin binaries by running the `release-kubectl-plugin` GitHub Actions workflow.
6. Publish the Python client to PyPI by running the `release-python-client` GitHub Actions workflow.
7. Update the [kuberay-helm](https://github.com/ray-project/kuberay-helm) repository to publish the new Helm chart versions.
8. Validate the release artifacts (images, charts).
9. Generate the CHANGELOG for the release.
10. Publish the release notes on GitHub Releases.
11. Update Ray documentation to refer to the new KubeRay version.

See the next section for more details on each step.

## Steps

*(Example commands use `v1.4.0` as the target release and `release-1.4` as the branch.)*

---

### Step 1: Create or Prepare the Release Branch

**For a new minor release (e.g., `v1.4.0` from `master`):**

1. Ensure your local `master` branch is up-to-date:

    ```bash
    # Ensure you are on the master branch
    git checkout master
    # Fetch the latest changes from upstream
    git fetch upstream
    # Reset your local master to match the upstream master
    git reset --hard upstream/master
    # Or, if you have local changes you need to manage carefully:
    # git pull --rebase upstream master
    ```

2. Create the release branch (e.g., `release-1.4`) from `upstream/master`:

    ```bash
    # Create the branch locally from the latest upstream master
    git checkout -b release-1.4 upstream/master
    ```

3. Push the new release branch to the `upstream` repository:

    ```bash
    git push upstream release-1.4
    ```

4. Set the upstream tracking branch locally to avoid accidentally merging `master` later:

    ```bash
    git branch --set-upstream-to upstream/release-1.4
    ```

5. **Verify CI:** GitHub Actions workflows run on `master` and all `release-*` branches. Check the [Actions tab](https://github.com/ray-project/kuberay/actions) for the `kuberay` repository and ensure all workflows are passing on the newly created `release-1.4` branch.

6. Update Ray CI to trigger nightly tests against the new kuberay release branch. See [example PR #51539](https://github.com/ray-project/ray/pull/51539).

---

### Step 2: Update KubeRay Version References

On the release branch (`release-1.4` in this example), update all references to the KubeRay version number (e.g., `1.4.0`).

Update the version in the following files:

* `helm-chart/kuberay-apiserver/Chart.yaml` (chart version)
* `helm-chart/kuberay-apiserver/values.yaml` (image tag)
* `helm-chart/kuberay-operator/Chart.yaml` (chart version)
* `helm-chart/kuberay-operator/values.yaml` (image tag)
* `helm-chart/ray-cluster/Chart.yaml` (chart version)
* `ray-operator/config/default/kustomization.yaml` (image tag)
* `ray-operator/controllers/ray/utils/constant.go` (`KUBERAY_VERSION` constant)

Open a PR to the release branch with these changes.
Refer to a previous version bump PR for guidance, like [PR #3071](https://github.com/ray-project/kuberay/pull/3071/files).

---

### Step 3: Create and Push the Git Tag

1. Ensure you are on the correct release branch and it includes the version bump commit:

    ```bash
    git checkout release-1.4
    git pull --rebase upstream release-1.4
    ```

2. Verify the latest commit is the version bump commit:

    ```bash
    git log -1
    # Check the commit message and changes:
    # git show
    ```

3. Create the git tag (using the `vX.Y.Z` format, e.g., `v1.4.0`):

    ```bash
    # Create a tag
    git tag v1.4.0
    ```

4. Push the tag to the `upstream` repository:

    ```bash
    git push upstream v1.4.0
    ```

---

### Step 4: Publish Release Images

Trigger the [`release-image-build`](https://github.com/ray-project/kuberay/actions/workflows/image-release.yaml) workflow to build and publish the KubeRay container images.

1. Navigate to the workflow page: [https://github.com/ray-project/kuberay/actions/workflows/image-release.yaml](https://github.com/ray-project/kuberay/actions/workflows/image-release.yaml)
2. Click the **"Run workflow"** dropdown button.
3. Set the parameters:
    * **Use workflow from:** Select **`Tags`** and choose the tag you just pushed (e.g., **`v1.4.0`**).
4. Click **"Run workflow"**.

**Verification:** Monitor the workflow run. Once completed successfully, check that the corresponding image tags are available on [quay.io](https://quay.io/repository/kuberay/operator?tab=tags).

---

### Step 5: Publish Kubectl Plugin Release

Trigger the [`release-kubectl-plugin`](https://github.com/ray-project/kuberay/actions/workflows/kubectl-plugin-release.yaml) workflow to build and publish the `kubectl-ray` binary.

1. Navigate to the workflow page: [https://github.com/ray-project/kuberay/actions/workflows/kubectl-plugin-release.yaml](https://github.com/ray-project/kuberay/actions/workflows/kubectl-plugin-release.yaml)
2. Click the **"Run workflow"** dropdown button.
3. Set the parameters:
    * **Use workflow from:** Select **`Tags`** and choose the tag for the release (e.g., **`v1.4.0`**).
4. Click **"Run workflow"**.

**Verification:**

* Monitor the workflow run for success.
* Check the [KubeRay Releases page](https://github.com/ray-project/kuberay/releases). A draft release corresponding to the tag `v1.4.0` should have been created (or updated if it existed) with the plugin binaries attached as assets.
* The workflow should automatically open a Pull Request in the [krew-index repository](https://github.com/kubernetes-sigs/krew-index) to update the plugin version for `kubectl krew`. Check for this PR and ensure it looks correct. It might require manual approval/merge by krew maintainers.

---

### Step 6: Publish Python Client to PyPI

Trigger the [`release-python-client`](https://github.com/ray-project/kuberay/actions/workflows/python-client-release.yaml) workflow to build the `kuberay-client` package and publish it to [PyPI](https://pypi.org/project/kuberay-client/).
The package version is taken from the tag. For example, `v1.4.0` becomes `1.4.0`, and `v1.4.0-rc.0` becomes the pre-release `1.4.0rc0`.

1. Navigate to the workflow page: [https://github.com/ray-project/kuberay/actions/workflows/python-client-release.yaml](https://github.com/ray-project/kuberay/actions/workflows/python-client-release.yaml)
2. Click the **"Run workflow"** dropdown button.
3. Set the parameters:
    * **Use workflow from:** Select **`Tags`** and choose the tag for the release (e.g., **`v1.4.0`**).
4. Click **"Run workflow"**.
5. The **Publish Python Client to PyPI** job waits for approval because it deploys to the `pypi` environment. A reviewer of that environment approves it with **"Review deployments"**.

**Verification:** Once the workflow succeeds, check that the new version is listed on [PyPI](https://pypi.org/project/kuberay-client/#history), then install it in a clean virtual environment:

```bash
pip install kuberay-client==1.4.0
```

---

### Step 7: Publish KubeRay Helm Charts

Helm charts are published via the [ray-project/kuberay-helm](https://github.com/ray-project/kuberay-helm) repository. This repo uses release branches (`release-X.Y`) mirroring the main `kuberay` repo.
See [helm-chart.md](./helm-chart.md) for the end-to-end workflow. Below are steps to cut a new release branch in the kuberay-helm repo and publish new charts.

1. **Create Release Branch (if it doesn't exist):**
    * Clone the `kuberay-helm` repository if you haven't already.
    * Set up an `upstream` remote: `git remote add upstream git@github.com:ray-project/kuberay-helm.git`
    * If the branch `release-1.4` does *not* exist in `kuberay-helm`:

        ```bash
        cd kuberay-helm
        git fetch upstream
        # Create the branch from the latest main branch
        git checkout -b release-1.4 upstream/main
        git push upstream release-1.4
        # Set tracking branch
        git branch --set-upstream-to upstream/release-1.4
        cd ..
        ```

    * *Note:* Creating a new release branch might trigger CI in `kuberay-helm`, but it shouldn't publish new chart versions yet as the chart files haven't changed.

2. **Sync Helm Charts:**
    * Ensure your local `kuberay` repository is on the release branch (`release-1.4`) and up-to-date (including the version bump from Step 2).

        ```bash
        cd kuberay
        git checkout release-1.4
        git pull --rebase upstream release-1.4
        cd ..
        ```

    * Copy the updated Helm charts from the `kuberay` repo to the `kuberay-helm` repo. *This assumes `kuberay` and `kuberay-helm` are in the same parent directory.*

        ```bash
        cd kuberay-helm
        # Ensure you are on the correct release branch
        git checkout release-1.4
        git pull --rebase upstream release-1.4

        # Remove old charts first to handle deleted files
        rm -rf helm-chart/
        # Copy the updated charts from the kuberay repo
        cp -R ../kuberay/helm-chart/ .
        ```

3. **Commit and Create Pull Request:**
    * Stage the changes in the `kuberay-helm` repository:

        ```bash
        git status # Verify changes
        git add helm-chart/
        git commit -m "Update Helm charts to KubeRay v1.4.0"
        ```

    * Push the changes to your fork of `kuberay-helm` (or directly if you have permissions, though PR is safer):

        ```bash
        # Example assuming 'origin' remote points to your fork
        git push origin release-1.4
        ```

    * Open a Pull Request from your fork. See [example PR #54](https://github.com/ray-project/kuberay-helm/pull/54).

4. **Merge and Verify:**
    * Once the PR is reviewed and merged, monitor the GitHub Actions workflows in the `kuberay-helm` repository:
        * [`chart-release`](https://github.com/ray-project/kuberay-helm/actions/workflows/chart-release.yaml)
        * [`pages-build-deployment`](https://github.com/ray-project/kuberay-helm/actions/workflows/pages/pages-build-deployment)
    * Verify that both workflows succeed. This indicates the charts have been packaged and added to the Helm repository index hosted via GitHub Pages.

---

### Step 8: Validate the Release

Perform basic validation to ensure the released artifacts work together.

1. Update your local Helm repository:

    ```bash
    helm repo add kuberay https://ray-project.github.io/kuberay-helm/
    helm repo update kuberay
    ```

2. Search for the new chart versions:

    ```bash
    helm search repo kuberay/kuberay-operator --versions
    helm search repo kuberay/ray-cluster --versions
    # Verify that version 1.4.0 (or your target version) appears
    ```

3. Install KubeRay using the new chart versions on a test cluster (e.g., Kind, Minikube):

    ```bash
    # Example using Kind
    kind create cluster

    # Install the operator using the specific version
    helm install kuberay-operator kuberay/kuberay-operator --version <new-version>

    # Install a sample RayCluster using the specific chart version
    helm install raycluster kuberay/ray-cluster --version <new-version>
    ```

4. Check the pods and basic functionality:

    ```bash
    kubectl get pods
    ```

    You should see the kuberay-operator pod in `Running` state and 1 active RayCluster.

5. Validate Go modules are published:

    ```bash
    go install github.com/ray-project/kuberay/ray-operator@v1.4.0
    ```

6. Open a PR to the master branch to update the version for KubeRay upgrade tests and make sure all tests pass. Example PR: #3825

---

### Step 9: Generate the CHANGELOG

Follow [Generating the changelog for a release](https://github.com/ray-project/kuberay/blob/master/docs/release/changelog.md) to generate the change log for the new release.

---

### Step 10: Publish Release Notes

1. Go to the [KubeRay Releases page](https://github.com/ray-project/kuberay/releases).
2. Find the **draft release** that was created automatically in Step 5 or create a new one if needed, targeting the tag `v1.4.0`.
3. Click **"Edit"** on the draft release.
4. Paste the generated **CHANGELOG** content from Step 9 into the release description.
5. Add any additional release highlights, bug fixes, known issues, etc.
6. Ensure the correct tag (`v1.4.0`) is selected.
7. Verify the attached assets (kubectl plugins) are correct.
8. Mark the release as **"Latest release"** if appropriate (usually for the newest stable release).
9. Click **"Publish release"**.

Announce the new release in the [kuberay Slack channel](https://ray.slack.com/archives/C01CKH05XBN).

---

### Step 11: Update ray.io documentation

Update all references to the KubeRay version in the ray.io documentation.
