# Guide: Pushing Docker Images to Registry

This guide outlines the process for tagging,
and pushing your Docker images to a docker repository.

## Prerequisites

* **Docker:** Installed and running on your local machine.
* **KubeRay:** Minimum version v1.6

## 1. Build History Server and Collector image

Navigate to the `kuberay/` dir and run:

```sh
make -C historyserver localimage-build
```

This will build the following images

1. historyserver:v0.1.0
2. collector:v0.1.0

## 2. Tag Image

Registry tags usually follow the following format:
`<REGISTRY_HOST>/<PATH>/<IMAGE_NAME>:<TAG>`

Run the following to tag the Collector and History Server images

```sh
docker tag historyserver:v0.1.0 <REGISTRY_HOST>/<PATH>/historyserver:v0.1.0
docker tag collector:v0.1.0 <REGISTRY_HOST>/<PATH>/collector:v0.1.0
```

## 3. Push image

Once tagged, upload the image to the registry:

```sh
docker push <REGISTRY_HOST>/<PATH>/historyserver:v0.1.0
docker push <REGISTRY_HOST>/<PATH>/collector:v0.1.0
```

---

## Google Artifact Registry

Additional steps specific for pushing to Google Artifact Registry

### Requirements

* **Cloud Project:** An active cloud project.
* **GCloud SDK:** Installed and initialized (`gcloud init`).
* **Permissions:** Ensure the **Artifact Registry API** is enabled and you have at
least the `roles/artifactregistry.writer` IAM role.

### Additional Configurations

| Variable | Description|
|---|---|
| REGION | The physical location of your repository (e.g., us-east1) |
| PROJECT_ID | Google Cloud Project ID |
| REPO_NAME | AR Repository Name |

#### Set configurable variables

```sh
export REGION=<location>
export PROJECT_ID=<project-id>
export REPO_NAME=<repository-name>
```

### (Optional) Create Artifact Registry Repository

```sh
gcloud artifacts repositories create ${REPO_NAME} \
    --repository-format=docker \
    --location=${REGION} \
    --description="History Server images"
```

### Configure `gcloud`

Configure Docker CLI to authenticate with Google Artifact Registry (AR)

```sh
gcloud auth configure-docker ${REGION}-docker.pkg.dev
```

## Tagging Artifact Registry Images

The image has to follow a specific naming convention for Google Artifact Registry

Format:

`${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${TAG}`

Perform the push on the tagged images. Example for the history server image:

```sh
docker tag historyserver:v0.1.0 ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/historyserver:v0.1.0
```
