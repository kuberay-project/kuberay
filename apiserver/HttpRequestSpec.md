# Full HTTP definition endpoints

## Compute Template

For the purpose to simplify the setting of resources, the Kuberay API server abstracts the resource of the pods
template resource to the `compute template`. You can define the resources in the `compute template` and then choose the
appropriate template for your `head` and `workergroup` when you are creating the objects of `RayCluster`, `RayJobs` or
`RayService`.

The full definition of the compute template resource can be found in [config.proto](../proto/config.proto) or the
Kuberay API server swagger doc.

### Create compute templates in a given namespace

```text
POST {{baseUrl}}/apis/v1/namespaces/<namespace>/compute_templates
```

Examples (please make sure that `ray-system` namespace exists before running this command):

* Request

  ```sh
  curl --silent -X 'POST' \
    'http://localhost:31888/apis/v1/namespaces/ray-system/compute_templates' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
        "name": "default-template",
        "namespace": "ray-system",
        "cpu": 2,
        "memory": 4
      }'
  ```

* Response

  ```json
  {
    "name": "default-template",
    "namespace": "ray-system",
    "cpu": 2,
    "memory": 4
  }
  ```

### List all compute templates in a given namespace

```text
GET {{baseUrl}}/apis/v1/namespaces/<namespace>/compute_templates
```

Examples:

* Request

  ```sh
  curl --silent -X 'GET' \
    'http://localhost:31888/apis/v1/namespaces/ray-system/compute_templates' \
    -H 'accept: application/json'
  ```

* Response

  ```json
  {
    "computeTemplates": [
      {
        "name": "default-template",
        "namespace": "ray-system",
        "cpu": 2,
        "memory": 4
      }
    ]
  }
  ```

#### List all compute templates in all namespaces

```text
GET {{baseUrl}}/apis/v1/compute_templates
```

Examples:

* Request

  ```sh
  curl --silent -X 'GET' \
  'http://localhost:31888/apis/v1/compute_templates' \
  -H 'accept: application/json'
  ```

* Response

    ```json
    {
      "computeTemplates": [
        {
          "name": "default-template",
          "namespace": "ray-system",
          "cpu": 2,
          "memory": 4
        }
      ]
    }
    ```

#### Get compute template by name

```text
GET {{baseUrl}}/apis/v1/namespaces/<namespace>/compute_templates/<compute_template_name>
```

Examples:

* Request

  ```sh
  curl --silent -X 'GET' \
  'http://localhost:31888/apis/v1/namespaces/ray-system/compute_templates/default-template' \
  -H 'accept: application/json'
  ```

* Response:

  ```json
  {
    "name": "default-template",
    "namespace": "ray-system",
    "cpu": 2,
    "memory": 4
  }
  ```

#### Delete compute template by name

```text
DELETE {{baseUrl}}/apis/v1/namespaces/<namespace>/compute_templates/<compute_template_name>
```

Examples:

* Request

  ```sh
  curl --silent -X 'DELETE' \
  'http://localhost:31888/apis/v1/namespaces/ray-system/compute_templates/default-template' \
  -H 'accept: application/json'
  ```

* Response

  ```json
  {}
  ```

### Clusters

#### Create cluster in a given namespace

```text
POST {{baseUrl}}/apis/v1/namespaces/<namespace>/clusters
```

Examples: (please make sure that template `default-template` is created before running this request)

* Request

  ```sh
  curl --silent -X 'POST' \
    'http://localhost:31888/apis/v1/namespaces/ray-system/clusters' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
    "name": "test-cluster",
    "namespace": "ray-system",
    "user": "3cpo",
    "version": "2.46.0",
    "environment": "DEV",
    "clusterSpec": {
      "headGroupSpec": {
        "computeTemplate": "default-template",
        "image": "rayproject/ray:2.46.0",
        "serviceType": "NodePort",
        "rayStartParams": {
          "dashboard-host": "0.0.0.0",
          "metrics-export-port": "8080"
        },
        "volumes": []
      },
      "workerGroupSpec": [
        {
          "groupName": "small-wg",
          "computeTemplate": "default-template",
          "image": "rayproject/ray:2.46.0",
          "replicas": 1,
          "minReplicas": 1,
          "maxReplicas": 1,
          "rayStartParams": {
            "node-ip-address": "$MY_POD_IP"
          }
        }
      ]
    }
  }'
  ```

<details>
  <summary>* Response</summary>

  ```json
  {
    "name": "test-cluster",
    "namespace": "ray-system",
    "user": "3cpo",
    "version": "2.46.0",
    "clusterSpec": {
      "headGroupSpec": {
        "computeTemplate": "default-template",
        "image": "rayproject/ray:2.46.0",
        "serviceType": "NodePort",
        "rayStartParams": {
          "dashboard-host": "0.0.0.0",
          "metrics-export-port": "8080"
        }
      },
      "workerGroupSpec": [
        {
          "groupName": "small-wg",
          "computeTemplate": "default-template",
          "image": "rayproject/ray:2.46.0",
          "replicas": 1,
          "minReplicas": 1,
          "maxReplicas": 1,
          "rayStartParams": {
            "node-ip-address": "$MY_POD_IP"
          }
        }
      ]
    },
    "annotations": {
      "ray.io/creation-timestamp": "2023-09-25 10:48:35.766443417 +0000 UTC"
    },
    "createdAt": "2023-09-25T10:48:35Z",
    "events": [
      {
        "id": "test-cluster.178817bd10374138",
        "name": "test-cluster-test-cluster.178817bd10374138",
        "createdAt": "2023-09-25T08:42:40Z",
        "firstTimestamp": "2023-09-25T08:42:40Z",
        "lastTimestamp": "2023-09-25T08:42:40Z",
        "reason": "Created",
        "message": "Created service test-cluster-head-svc",
        "type": "Normal",
        "count": 1
      },
      {
        "id": "test-cluster.178817bd251e9c7c",
        "name": "test-cluster-test-cluster.178817bd251e9c7c",
        "createdAt": "2023-09-25T08:42:40Z",
        "firstTimestamp": "2023-09-25T08:42:40Z",
        "lastTimestamp": "2023-09-25T08:42:40Z",
        "reason": "Created",
        "message": "Created head pod test-cluster-head-rsbmm",
        "type": "Normal",
        "count": 1
      },
      {
        "id": "test-cluster.178817bd2b74493f",
        "name": "test-cluster-test-cluster.178817bd2b74493f",
        "createdAt": "2023-09-25T08:42:40Z",
        "firstTimestamp": "2023-09-25T08:42:40Z",
        "lastTimestamp": "2023-09-25T08:42:41Z",
        "reason": "Created",
        "message": "Created worker pod ",
        "type": "Normal",
        "count": 2
      }
    ]
  }
  ```

</details>

#### List all clusters in a given namespace

```text
GET {{baseUrl}}/apis/v1/namespaces/<namespace>/clusters
```

Examples:

* Request

  ```sh
  curl --silent -X 'GET' \
    'http://localhost:31888/apis/v1/namespaces/ray-system/clusters' \
    -H 'accept: application/json'
  ```

<details>
  <summary>* Response</summary>

  ```json
  {
    "clusters": [
      {
        "name": "test-cluster",
        "namespace": "ray-system",
        "user": "3cpo",
        "version": "2.46.0",
        "clusterSpec": {
          "headGroupSpec": {
            "computeTemplate": "default-template",
            "image": "rayproject/ray:2.46.0",
            "serviceType": "NodePort",
            "rayStartParams": {
              "dashboard-host": "0.0.0.0",
              "metrics-export-port": "8080"
            }
          },
          "workerGroupSpec": [
            {
              "groupName": "small-wg",
              "computeTemplate": "default-template",
              "image": "rayproject/ray:2.46.0",
              "replicas": 1,
              "minReplicas": 1,
              "maxReplicas": 1,
              "rayStartParams": {
                "node-ip-address": "$MY_POD_IP"
              }
            }
          ]
        },
        "annotations": {
          "ray.io/creation-timestamp": "2023-09-25 10:48:35.766443417 +0000 UTC"
        },
        "createdAt": "2023-09-25T10:48:35Z",
        "clusterState": "ready",
        "events": [
          {
            "id": "test-cluster.178817bd10374138",
            "name": "test-cluster-test-cluster.178817bd10374138",
            "createdAt": "2023-09-25T08:42:40Z",
            "firstTimestamp": "2023-09-25T08:42:40Z",
            "lastTimestamp": "2023-09-25T08:42:40Z",
            "reason": "Created",
            "message": "Created service test-cluster-head-svc",
            "type": "Normal",
            "count": 1
          },
          {
            "id": "test-cluster.178817bd251e9c7c",
            "name": "test-cluster-test-cluster.178817bd251e9c7c",
            "createdAt": "2023-09-25T08:42:40Z",
            "firstTimestamp": "2023-09-25T08:42:40Z",
            "lastTimestamp": "2023-09-25T08:42:40Z",
            "reason": "Created",
            "message": "Created head pod test-cluster-head-rsbmm",
            "type": "Normal",
            "count": 1
          },
          {
            "id": "test-cluster.178817bd2b74493f",
            "name": "test-cluster-test-cluster.178817bd2b74493f",
            "createdAt": "2023-09-25T08:42:40Z",
            "firstTimestamp": "2023-09-25T08:42:40Z",
            "lastTimestamp": "2023-09-25T08:42:41Z",
            "reason": "Created",
            "message": "Created worker pod ",
            "type": "Normal",
            "count": 2
          },
          {
            "id": "test-cluster.17881e9c2b82c449",
            "name": "test-cluster-test-cluster.17881e9c2b82c449",
            "createdAt": "2023-09-25T10:48:35Z",
            "firstTimestamp": "2023-09-25T10:48:35Z",
            "lastTimestamp": "2023-09-25T10:48:35Z",
            "reason": "Created",
            "message": "Created service test-cluster-head-svc",
            "type": "Normal",
            "count": 1
          },
          {
            "id": "test-cluster.17881e9c2e9cd4b8",
            "name": "test-cluster-test-cluster.17881e9c2e9cd4b8",
            "createdAt": "2023-09-25T10:48:35Z",
            "firstTimestamp": "2023-09-25T10:48:35Z",
            "lastTimestamp": "2023-09-25T10:48:35Z",
            "reason": "Created",
            "message": "Created head pod test-cluster-head-nglmx",
            "type": "Normal",
            "count": 1
          },
          {
            "id": "test-cluster.17881e9c34460442",
            "name": "test-cluster-test-cluster.17881e9c34460442",
            "createdAt": "2023-09-25T10:48:35Z",
            "firstTimestamp": "2023-09-25T10:48:35Z",
            "lastTimestamp": "2023-09-25T10:48:35Z",
            "reason": "Created",
            "message": "Created worker pod ",
            "type": "Normal",
            "count": 1
          }
        ],
        "serviceEndpoint": {
          "dashboard": "31476",
          "head": "31850",
          "metrics": "32189",
          "redis": "30736"
        }
      }
    ]
  }
  ```

</details>

#### List all clusters in all namespaces

```text
GET {{baseUrl}}/apis/v1/clusters
```

Examples:

* Request

  ```sh
  curl --silent -X 'GET' \
    'http://localhost:31888/apis/v1/clusters' \
    -H 'accept: application/json'
  ```

<details>
  <summary>* Response</summary>

  ```json
  {
    "clusters": [
      {
        "name": "test-cluster",
        "namespace": "ray-system",
        "user": "3cpo",
        "version": "2.46.0",
        "clusterSpec": {
          "headGroupSpec": {
            "computeTemplate": "default-template",
            "image": "rayproject/ray:2.46.0",
            "serviceType": "NodePort",
            "rayStartParams": {
              "dashboard-host": "0.0.0.0",
              "metrics-export-port": "8080"
            }
          },
          "workerGroupSpec": [
            {
              "groupName": "small-wg",
              "computeTemplate": "default-template",
              "image": "rayproject/ray:2.46.0",
              "replicas": 1,
              "minReplicas": 1,
              "maxReplicas": 1,
              "rayStartParams": {
                "node-ip-address": "$MY_POD_IP"
              }
            }
          ]
        },
        "annotations": {
          "ray.io/creation-timestamp": "2023-09-25 10:48:35.766443417 +0000 UTC"
        },
        "createdAt": "2023-09-25T10:48:35Z",
        "clusterState": "ready",
        "events": [
          {
            "id": "test-cluster.178817bd10374138",
            "name": "test-cluster-test-cluster.178817bd10374138",
            "createdAt": "2023-09-25T08:42:40Z",
            "firstTimestamp": "2023-09-25T08:42:40Z",
            "lastTimestamp": "2023-09-25T08:42:40Z",
            "reason": "Created",
            "message": "Created service test-cluster-head-svc",
            "type": "Normal",
            "count": 1
          },
          {
            "id": "test-cluster.178817bd251e9c7c",
            "name": "test-cluster-test-cluster.178817bd251e9c7c",
            "createdAt": "2023-09-25T08:42:40Z",
            "firstTimestamp": "2023-09-25T08:42:40Z",
            "lastTimestamp": "2023-09-25T08:42:40Z",
            "reason": "Created",
            "message": "Created head pod test-cluster-head-rsbmm",
            "type": "Normal",
            "count": 1
          },
          {
            "id": "test-cluster.178817bd2b74493f",
            "name": "test-cluster-test-cluster.178817bd2b74493f",
            "createdAt": "2023-09-25T08:42:40Z",
            "firstTimestamp": "2023-09-25T08:42:40Z",
            "lastTimestamp": "2023-09-25T08:42:41Z",
            "reason": "Created",
            "message": "Created worker pod ",
            "type": "Normal",
            "count": 2
          },
          {
            "id": "test-cluster.17881e9c2b82c449",
            "name": "test-cluster-test-cluster.17881e9c2b82c449",
            "createdAt": "2023-09-25T10:48:35Z",
            "firstTimestamp": "2023-09-25T10:48:35Z",
            "lastTimestamp": "2023-09-25T10:48:35Z",
            "reason": "Created",
            "message": "Created service test-cluster-head-svc",
            "type": "Normal",
            "count": 1
          },
          {
            "id": "test-cluster.17881e9c2e9cd4b8",
            "name": "test-cluster-test-cluster.17881e9c2e9cd4b8",
            "createdAt": "2023-09-25T10:48:35Z",
            "firstTimestamp": "2023-09-25T10:48:35Z",
            "lastTimestamp": "2023-09-25T10:48:35Z",
            "reason": "Created",
            "message": "Created head pod test-cluster-head-nglmx",
            "type": "Normal",
            "count": 1
          },
          {
            "id": "test-cluster.17881e9c34460442",
            "name": "test-cluster-test-cluster.17881e9c34460442",
            "createdAt": "2023-09-25T10:48:35Z",
            "firstTimestamp": "2023-09-25T10:48:35Z",
            "lastTimestamp": "2023-09-25T10:48:35Z",
            "reason": "Created",
            "message": "Created worker pod ",
            "type": "Normal",
            "count": 1
          }
        ],
        "serviceEndpoint": {
          "dashboard": "31476",
          "head": "31850",
          "metrics": "32189",
          "redis": "30736"
        }
      }
    ]
  }
  ```

</details>

#### Get cluster by its name and namespace

```text
GET {{baseUrl}}/apis/v1/namespaces/<namespace>/clusters/<cluster_name>
```

Examples:

* Request

  ```sh
  curl --silent -X 'GET' \
    'http://localhost:31888/apis/v1/namespaces/ray-system/clusters' \
    -H 'accept: application/json'
  ```

<details>
  <summary>* Response</summary>

  ```json
  {
    "clusters": [
      {
        "name": "test-cluster",
        "namespace": "ray-system",
        "user": "3cpo",
        "version": "2.46.0",
        "clusterSpec": {
          "headGroupSpec": {
            "computeTemplate": "default-template",
            "image": "rayproject/ray:2.46.0",
            "serviceType": "NodePort",
            "rayStartParams": {
              "dashboard-host": "0.0.0.0",
              "metrics-export-port": "8080"
            }
          },
          "workerGroupSpec": [
            {
              "groupName": "small-wg",
              "computeTemplate": "default-template",
              "image": "rayproject/ray:2.46.0",
              "replicas": 1,
              "minReplicas": 1,
              "maxReplicas": 1,
              "rayStartParams": {
                "node-ip-address": "$MY_POD_IP"
              }
            }
          ]
        },
        "annotations": {
          "ray.io/creation-timestamp": "2023-09-25 10:48:35.766443417 +0000 UTC"
        },
        "createdAt": "2023-09-25T10:48:35Z",
        "clusterState": "ready",
        "events": [
          {
            "id": "test-cluster.178817bd10374138",
            "name": "test-cluster-test-cluster.178817bd10374138",
            "createdAt": "2023-09-25T08:42:40Z",
            "firstTimestamp": "2023-09-25T08:42:40Z",
            "lastTimestamp": "2023-09-25T08:42:40Z",
            "reason": "Created",
            "message": "Created service test-cluster-head-svc",
            "type": "Normal",
            "count": 1
          },
          {
            "id": "test-cluster.178817bd251e9c7c",
            "name": "test-cluster-test-cluster.178817bd251e9c7c",
            "createdAt": "2023-09-25T08:42:40Z",
            "firstTimestamp": "2023-09-25T08:42:40Z",
            "lastTimestamp": "2023-09-25T08:42:40Z",
            "reason": "Created",
            "message": "Created head pod test-cluster-head-rsbmm",
            "type": "Normal",
            "count": 1
          },
          {
            "id": "test-cluster.178817bd2b74493f",
            "name": "test-cluster-test-cluster.178817bd2b74493f",
            "createdAt": "2023-09-25T08:42:40Z",
            "firstTimestamp": "2023-09-25T08:42:40Z",
            "lastTimestamp": "2023-09-25T08:42:41Z",
            "reason": "Created",
            "message": "Created worker pod ",
            "type": "Normal",
            "count": 2
          },
          {
            "id": "test-cluster.17881e9c2b82c449",
            "name": "test-cluster-test-cluster.17881e9c2b82c449",
            "createdAt": "2023-09-25T10:48:35Z",
            "firstTimestamp": "2023-09-25T10:48:35Z",
            "lastTimestamp": "2023-09-25T10:48:35Z",
            "reason": "Created",
            "message": "Created service test-cluster-head-svc",
            "type": "Normal",
            "count": 1
          },
          {
            "id": "test-cluster.17881e9c2e9cd4b8",
            "name": "test-cluster-test-cluster.17881e9c2e9cd4b8",
            "createdAt": "2023-09-25T10:48:35Z",
            "firstTimestamp": "2023-09-25T10:48:35Z",
            "lastTimestamp": "2023-09-25T10:48:35Z",
            "reason": "Created",
            "message": "Created head pod test-cluster-head-nglmx",
            "type": "Normal",
            "count": 1
          },
          {
            "id": "test-cluster.17881e9c34460442",
            "name": "test-cluster-test-cluster.17881e9c34460442",
            "createdAt": "2023-09-25T10:48:35Z",
            "firstTimestamp": "2023-09-25T10:48:35Z",
            "lastTimestamp": "2023-09-25T10:48:35Z",
            "reason": "Created",
            "message": "Created worker pod ",
            "type": "Normal",
            "count": 1
          }
        ],
        "serviceEndpoint": {
          "dashboard": "31476",
          "head": "31850",
          "metrics": "32189",
          "redis": "30736"
        }
      }
    ]
  }
  ```

</details>

#### Delete cluster by its name and namespace

```text
DELETE {{baseUrl}}/apis/v1/namespaces/<namespace>/clusters/<cluster_name>
```

Examples:

* Request

  ```sh
  curl --silent -X 'DELETE' \
  'http://localhost:31888/apis/v1/namespaces/ray-system/clusters/test-cluster' \
  -H 'accept: application/json'
  ```

* Response:

  ```json
  {}
  ```

### RayJob

#### Create ray job in a given namespace

```text
POST {{baseUrl}}/apis/v1/namespaces/<namespace>/jobs
```

Examples:

* Request

  ```sh
  curl --silent -X 'POST' \
      'http://localhost:31888/apis/v1/namespaces/ray-system/jobs' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '{
      "name": "rayjob-test",
      "namespace": "ray-system",
      "user": "3cp0",
      "version": "2.46.0",
      "entrypoint": "python -V",
      "clusterSpec": {
        "headGroupSpec": {
          "computeTemplate": "default-template",
          "image": "rayproject/ray:2.46.0",
          "serviceType": "NodePort",
          "rayStartParams": {
            "dashboard-host": "0.0.0.0"
          }
        },
        "workerGroupSpec": [
          {
            "groupName": "small-wg",
            "computeTemplate": "default-template",
            "image": "rayproject/ray:2.46.0",
            "replicas": 1,
            "minReplicas": 0,
            "maxReplicas": 1,
            "rayStartParams": {
              "metrics-export-port": "8080"
            }
          }
        ]
      }
    }'
  ```

<details>
  <summary>* Response</summary>

  ```json
  {
    "name": "rayjob-test",
    "namespace": "ray-system",
    "user": "3cp0",
    "entrypoint": "python -V",
    "clusterSpec": {
      "headGroupSpec": {
        "computeTemplate": "default-template",
        "image": "rayproject/ray:2.46.0",
        "serviceType": "NodePort",
        "rayStartParams": {
          "dashboard-host": "0.0.0.0"
        }
      },
      "workerGroupSpec": [
        {
          "groupName": "small-wg",
          "computeTemplate": "default-template",
          "image": "rayproject/ray:2.46.0",
          "replicas": 1,
          "minReplicas": 1,
          "maxReplicas": 1,
          "rayStartParams": {
            "metrics-export-port": "8080"
          }
        }
      ]
    },
    "createdAt": "2023-09-25T11:36:02Z"
  }
  ```

</details>

The above example creates a new Ray cluster, executes a job on it and optionally deletes a cluster. As an alternative,
the same command allows creating a new job on the existing cluster by referencing it in the payload.

Examples:

Start from creating Ray cluster (We assume here that the [template](test/cluster/template/simple) and
[configmap](test/job/code.yaml) are already created).

* Request

```sh
curl -X POST 'localhost:31888/apis/v1/namespaces/default/clusters' \
--header 'Content-Type: application/json' \
--data '{
  "name": "job-test",
  "namespace": "default",
  "user": "kuberay",
  "version": "2.46.0",
  "environment": "DEV",
  "clusterSpec": {
    "headGroupSpec": {
      "computeTemplate": "default-template",
      "image": "rayproject/ray:2.46.0-py310",
      "serviceType": "NodePort",
      "rayStartParams": {
         "dashboard-host": "0.0.0.0",
         "metrics-export-port": "8080"
       },
       "volumes": [
         {
           "name": "code-sample",
           "mountPath": "/home/ray/samples",
           "volumeType": "CONFIGMAP",
           "source": "ray-job-code-sample",
           "items": {"sample_code.py" : "sample_code.py"}
         }
       ]
    },
    "workerGroupSpec": [
      {
        "groupName": "small-wg",
        "computeTemplate": "default-template",
        "image": "rayproject/ray:2.46.0-py310",
        "replicas": 1,
        "minReplicas": 0,
        "maxReplicas": 5,
        "rayStartParams": {
           "node-ip-address": "$MY_POD_IP"
         },
        "volumes": [
          {
            "name": "code-sample",
            "mountPath": "/home/ray/samples",
            "volumeType": "CONFIGMAP",
            "source": "ray-job-code-sample",
            "items": {"sample_code.py" : "sample_code.py"}
          }
        ]
      }
    ]
  }
}'
```

<details>
  <summary>* Response</summary>

```json
{
   "name":"job-test",
   "namespace":"default",
   "user":"kuberay",
   "version":"2.46.0",
   "clusterSpec":{
      "headGroupSpec":{
         "computeTemplate":"default-template",
         "image":"rayproject/ray:2.46.0-py310",
         "serviceType":"NodePort",
         "rayStartParams":{
            "dashboard-host":"0.0.0.0",
            "metrics-export-port":"8080"
         },
         "volumes":[
            {
               "mountPath":"/home/ray/samples",
               "volumeType":3,
               "name":"code-sample",
               "source":"ray-job-code-sample",
               "items":{
                  "sample_code.py":"sample_code.py"
               }
            }
         ],
         "environment":{

         }
      },
      "workerGroupSpec":[
         {
            "groupName":"small-wg",
            "computeTemplate":"default-template",
            "image":"rayproject/ray:2.46.0-py310",
            "replicas":1,
            "minReplicas":5,
            "maxReplicas":1,
            "rayStartParams":{
               "node-ip-address":"$MY_POD_IP"
            },
            "volumes":[
               {
                  "mountPath":"/home/ray/samples",
                  "volumeType":3,
                  "name":"code-sample",
                  "source":"ray-job-code-sample",
                  "items":{
                     "sample_code.py":"sample_code.py"
                  }
               }
            ],
            "environment":{

            }
         }
      ]
   },
   "annotations":{
      "ray.io/creation-timestamp":"2023-10-18 08:47:48.058576 +0000 UTC"
   },
   "createdAt":"2023-10-18T08:47:48Z"
}
```

</details>

Once the cluster is created, we can create a job to run on it.

* Request

```sh
curl -X POST 'localhost:31888/apis/v1/namespaces/default/jobs' \
--header 'Content-Type: application/json' \
--data '{
  "name": "job-test",
  "namespace": "default",
  "user": "kuberay",
  "version": "2.46.0",
  "entrypoint": "python /home/ray/samples/sample_code.py",
  "runtimeEnv": "pip:\n  - requests==2.26.0\n  - pendulum==2.1.2\nenv_vars:\n  counter_name: test_counter\n",
  "jobSubmitter": {
    "image": "rayproject/ray:2.46.0-py310",
    "cpu": "400m",
    "memory": "150Mi"
  },
  "clusterSelector": {
    "ray.io/cluster": "job-test"
  }
}'
```

* Response

```json
{
   "name":"job-test",
   "namespace":"default",
   "user":"kuberay",
   "entrypoint":"python /home/ray/samples/sample_code.py",
   "runtimeEnv":"pip:\n  - requests==2.26.0\n  - pendulum==2.1.2\nenv_vars:\n  counter_name: test_counter\n",
   "clusterSelector":{
      "ray.io/cluster":"job-test"
   },
   "createdAt":"2023-10-24T11:37:29Z"
}
```

You should also see job submitter job completed, something like:

```text
job-test-2hhmf                   0/1     Completed   0          15s
```

To see job execution results run:

```sh
kubectl logs job-test-2hhmf
```

And you should get something similar to:

```text
2023-10-18 03:19:51,524 INFO cli.py:36 -- Job submission server address: http://job-test-head-svc.default.svc.cluster.local:8265
2023-10-18 03:19:52,197 SUCC cli.py:60 -- -------------------------------------------
2023-10-18 03:19:52,197 SUCC cli.py:61 -- Job 'job-test-bbfqs' submitted successfully
2023-10-18 03:19:52,197 SUCC cli.py:62 -- -------------------------------------------
2023-10-18 03:19:52,197 INFO cli.py:274 -- Next steps
2023-10-18 03:19:52,197 INFO cli.py:275 -- Query the logs of the job:
2023-10-18 03:19:52,198 INFO cli.py:277 -- ray job logs job-test-bbfqs
2023-10-18 03:19:52,198 INFO cli.py:279 -- Query the status of the job:
2023-10-18 03:19:52,198 INFO cli.py:281 -- ray job status job-test-bbfqs
2023-10-18 03:19:52,198 INFO cli.py:283 -- Request the job to be stopped:
2023-10-18 03:19:52,198 INFO cli.py:285 -- ray job stop job-test-bbfqs
2023-10-18 03:19:52,203 INFO cli.py:292 -- Tailing logs until the job exits (disable with --no-wait):
2023-10-18 03:20:00,014 INFO worker.py:1329 -- Using address 10.244.0.10:6379 set in the environment variable RAY_ADDRESS
2023-10-18 03:20:00,014 INFO worker.py:1458 -- Connecting to existing Ray cluster at address: 10.244.0.10:6379...
2023-10-18 03:20:00,032 INFO worker.py:1633 -- Connected to Ray cluster. View the dashboard at 10.244.0.10:8265
test_counter got 1
test_counter got 2
test_counter got 3
test_counter got 4
test_counter got 5
2023-10-18 03:20:03,304 SUCC cli.py:60 -- ------------------------------
2023-10-18 03:20:03,304 SUCC cli.py:61 -- Job 'job-test-bbfqs' succeeded
2023-10-18 03:20:03,304 SUCC cli.py:62 -- ------------------------------
```

Additionally here, we can specify configuration for the job submitter, allowing to specify image, memory and cpu limits
for it.

Make sure that you delete previous job before running this one:

```sh
kubectl delete rayjob job-test
```

* Request

```sh
curl -X POST 'localhost:31888/apis/v1/namespaces/default/jobs' \
--header 'Content-Type: application/json' \
--data '{
  "name": "job-test",
  "namespace": "default",
  "user": "kuberay",
  "version": "2.46.0",
  "entrypoint": "python /home/ray/samples/sample_code.py",
   "runtimeEnv": "pip:\n  - requests==2.26.0\n  - pendulum==2.1.2\nenv_vars:\n  counter_name: test_counter\n",
  "clusterSelector": {
    "ray.io/cluster": "job-test"
  },
  "jobSubmitter": {
    "image": "rayproject/ray:2.46.0-py310"
  }
}'
```

* Response

```json
{
   "name":"job-test",
   "namespace":"default",
   "user":"kuberay",
   "entrypoint":"python /home/ray/samples/sample_code.py",
   "runtimeEnv":"pip:\n  - requests==2.26.0\n  - pendulum==2.1.2\nenv_vars:\n  counter_name: test_counter\n",
   "clusterSelector":{
      "ray.io/cluster":"job-test"
   },
   "jobSubmitter":{
      "image":"rayproject/ray:2.46.0-py310"
   },
   "createdAt":"2023-10-24T11:48:19Z"
}
```

You should beble to see job execution results similar to above

#### List all jobs in a given namespace

```text
GET {{baseUrl}}/apis/v1/namespaces/<namespace>/jobs
```

Examples:

* Request

  ```sh
  curl --silent -X 'GET' \
  'http://localhost:31888/apis/v1/namespaces/ray-system/jobs' \
  -H 'accept: application/json'
  ```

<details>
  <summary>* Response</summary>

  ```json
  {
    "jobs": [
      {
        "name": "rayjob-test",
        "namespace": "ray-system",
        "user": "3cp0",
        "entrypoint": "python -V",
        "jobId": "rayjob-test-drhlq",
        "clusterSpec": {
          "headGroupSpec": {
            "computeTemplate": "default-template",
            "image": "rayproject/ray:2.46.0",
            "serviceType": "NodePort",
            "rayStartParams": {
              "dashboard-host": "0.0.0.0"
            }
          },
          "workerGroupSpec": [
            {
              "groupName": "small-wg",
              "computeTemplate": "default-template",
              "image": "rayproject/ray:2.46.0",
              "replicas": 1,
              "minReplicas": 1,
              "maxReplicas": 1,
              "rayStartParams": {
                "metrics-export-port": "8080"
              }
            }
          ]
        },
        "createdAt": "2023-09-25T11:36:02Z",
        "jobStatus": "SUCCEEDED",
        "jobDeploymentStatus": "Running",
        "message": "Job finished successfully."
      }
    ]
  }
  ```

</details>

#### List all jobs in all namespaces

```text
GET {{baseUrl}}/apis/v1/jobs
```

Examples:

* Request:

  ```sh
  curl --silent -X 'GET' \
  'http://localhost:31888/apis/v1/jobs' \
  -H 'accept: application/json'
  ```

<details>
  <summary>* Response</summary>

  ```json
  {
    "jobs": [
      {
        "name": "rayjob-test",
        "namespace": "ray-system",
        "user": "3cp0",
        "entrypoint": "python -V",
        "jobId": "rayjob-test-drhlq",
        "clusterSpec": {
          "headGroupSpec": {
            "computeTemplate": "default-template",
            "image": "rayproject/ray:2.46.0",
            "serviceType": "NodePort",
            "rayStartParams": {
              "dashboard-host": "0.0.0.0"
            }
          },
          "workerGroupSpec": [
            {
              "groupName": "small-wg",
              "computeTemplate": "default-template",
              "image": "rayproject/ray:2.46.0",
              "replicas": 1,
              "minReplicas": 1,
              "maxReplicas": 1,
              "rayStartParams": {
                "metrics-export-port": "8080"
              }
            }
          ]
        },
        "createdAt": "2023-09-25T11:36:02Z",
        "jobStatus": "SUCCEEDED",
        "jobDeploymentStatus": "Running",
        "message": "Job finished successfully."
      }
    ]
  }
  ```

</details>

#### Get job by its name and namespace

```text
GET {{baseUrl}}/apis/v1/namespaces/<namespace>/jobs/<job_name>
```

Examples:

* Request

  ```sh
  curl --silent -X 'GET' \
  'http://localhost:31888/apis/v1/namespaces/ray-system/jobs/rayjob-test' \
  -H 'accept: application/json'
  ```

<details>
  <summary>* Response</summary>

  ```json
  {
    "name": "rayjob-test",
    "namespace": "ray-system",
    "user": "3cp0",
    "entrypoint": "python -V",
    "jobId": "rayjob-test-drhlq",
    "clusterSpec": {
      "headGroupSpec": {
        "computeTemplate": "default-template",
        "image": "rayproject/ray:2.46.0",
        "serviceType": "NodePort",
        "rayStartParams": {
          "dashboard-host": "0.0.0.0"
        }
      },
      "workerGroupSpec": [
        {
          "groupName": "small-wg",
          "computeTemplate": "default-template",
          "image": "rayproject/ray:2.46.0",
          "replicas": 1,
          "minReplicas": 1,
          "maxReplicas": 1,
          "rayStartParams": {
            "metrics-export-port": "8080"
          }
        }
      ]
    },
    "createdAt": "2023-09-25T11:36:02Z",
    "jobStatus": "SUCCEEDED",
    "jobDeploymentStatus": "Running",
    "message": "Job finished successfully."
  }
  ```

</details>

#### Delete job by its name and namespace

```text
DELETE {{baseUrl}}/apis/v1/namespaces/<namespace>/jobs/<job_name>
```

Examples:

* Request

  ```sh
  curl --silent -X 'DELETE' \
  'http://localhost:31888/apis/v1/namespaces/ray-system/jobs/rayjob-test' \
  -H 'accept: application/json'
  ```

* Response

  ```json
  {}
  ```

### RayService

#### Create ray service in a given namespace

```text
POST {{baseUrl}}/apis/v1/namespaces/<namespace>/services
```

Examples:

* Request

```sh
 curl -X 'POST' 'http://localhost:31888/apis/v1/namespaces/default/services' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '
  {
    "name": "test-v2",
    "namespace": "default",
    "user": "user",
    "version": "2.46.0",
    "serveConfigV2": "applications:\n  - name: fruit_app\n    import_path: fruit.deployment_graph\n    route_prefix: /fruit\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: MangoStand\n        num_replicas: 2\n        max_replicas_per_node: 1\n        user_config:\n          price: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: OrangeStand\n        num_replicas: 1\n        user_config:\n          price: 2\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: PearStand\n        num_replicas: 1\n        user_config:\n          price: 1\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: FruitMarket\n        num_replicas: 1\n        ray_actor_options:\n          num_cpus: 0.1\n  - name: math_app\n    import_path: conditional_dag.serve_dag\n    route_prefix: /calc\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: Adder\n        num_replicas: 1\n        user_config:\n          increment: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Multiplier\n        num_replicas: 1\n        user_config:\n          factor: 5\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Router\n        num_replicas: 1\n",
    "clusterSpec": {
      "headGroupSpec": {
        "computeTemplate": "default-template",
        "image": "rayproject/ray:2.46.0-py310",
        "serviceType": "NodePort",
        "rayStartParams": {
          "dashboard-host": "0.0.0.0",
          "metrics-export-port": "8080"
        },
        "volumes": []
      },
      "workerGroupSpec": [
        {
          "groupName": "small-wg",
          "computeTemplate": "default-template",
          "image": "rayproject/ray:2.46.0-py310",
          "replicas": 1,
          "minReplicas": 0,
          "maxReplicas": 5,
          "rayStartParams": {
            "node-ip-address": "$MY_POD_IP"
          }
        }
      ]
    }
  }'
```

<details>
  <summary>* Response</summary>

```json
{
   "name":"test-v2",
   "namespace":"default",
   "user":"user",
   "serveConfigV2":"applications:\n  - name: fruit_app\n    import_path: fruit.deployment_graph\n    route_prefix: /fruit\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: MangoStand\n        num_replicas: 2\n        max_replicas_per_node: 1\n        user_config:\n          price: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: OrangeStand\n        num_replicas: 1\n        user_config:\n          price: 2\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: PearStand\n        num_replicas: 1\n        user_config:\n          price: 1\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: FruitMarket\n        num_replicas: 1\n        ray_actor_options:\n          num_cpus: 0.1\n  - name: math_app\n    import_path: conditional_dag.serve_dag\n    route_prefix: /calc\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: Adder\n        num_replicas: 1\n        user_config:\n          increment: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Multiplier\n        num_replicas: 1\n        user_config:\n          factor: 5\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Router\n        num_replicas: 1\n",
   "clusterSpec":{
      "headGroupSpec":{
         "computeTemplate":"default-template",
         "image":"rayproject/ray:2.46.0-py310",
         "serviceType":"NodePort",
         "rayStartParams":{
            "dashboard-host":"0.0.0.0",
            "metrics-export-port":"8080"
         },
         "environment":{

         }
      },
      "workerGroupSpec":[
         {
            "groupName":"small-wg",
            "computeTemplate":"default-template",
            "image":"rayproject/ray:2.46.0-py310",
            "replicas":1,
            "minReplicas":1,
            "maxReplicas":5,
            "rayStartParams":{
               "node-ip-address":"$MY_POD_IP"
            },
            "environment":{

            }
         }
      ]
   },
   "rayServiceStatus":{
      "rayServiceEvents":[
         {
            "id":"test-v2.17ab175ec787d26e",
            "name":"test-v2-test-v2.17ab175ec787d26e",
            "createdAt":"2024-01-17T09:09:39Z",
            "firstTimestamp":"2024-01-17T09:09:39Z",
            "lastTimestamp":"2024-01-17T09:10:04Z",
            "reason":"ServiceNotReady",
            "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
            "type":"Normal",
            "count":13
         },
         {
            "id":"test-v2.17ab176292337141",
            "name":"test-v2-test-v2.17ab176292337141",
            "createdAt":"2024-01-17T09:09:56Z",
            "firstTimestamp":"2024-01-17T09:09:56Z",
            "lastTimestamp":"2024-01-17T09:09:56Z",
            "reason":"SubmittedServeDeployment",
            "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-lmk5c",
            "type":"Normal",
            "count":1
         },
         {
            "id":"test-v2.17ab1764fe827fe3",
            "name":"test-v2-test-v2.17ab1764fe827fe3",
            "createdAt":"2024-01-17T09:10:06Z",
            "firstTimestamp":"2024-01-17T09:10:06Z",
            "lastTimestamp":"2024-01-17T09:14:41Z",
            "reason":"Running",
            "message":"The Serve applicaton is now running and healthy.",
            "type":"Normal",
            "count":139
         },
         {
            "id":"test-v2.17ab1815ce6a98da",
            "name":"test-v2-test-v2.17ab1815ce6a98da",
            "createdAt":"2024-01-17T09:22:45Z",
            "firstTimestamp":"2024-01-17T09:22:45Z",
            "lastTimestamp":"2024-01-17T09:23:16Z",
            "reason":"ServiceNotReady",
            "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
            "type":"Normal",
            "count":16
         },
         {
            "id":"test-v2.17ab181a8576812d",
            "name":"test-v2-test-v2.17ab181a8576812d",
            "createdAt":"2024-01-17T09:23:06Z",
            "firstTimestamp":"2024-01-17T09:23:06Z",
            "lastTimestamp":"2024-01-17T09:23:06Z",
            "reason":"SubmittedServeDeployment",
            "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-b85bj",
            "type":"Normal",
            "count":1
         },
         {
            "id":"test-v2.17ab181d64327965",
            "name":"test-v2-test-v2.17ab181d64327965",
            "createdAt":"2024-01-17T09:23:18Z",
            "firstTimestamp":"2024-01-17T09:23:18Z",
            "lastTimestamp":"2024-01-17T09:23:26Z",
            "reason":"Running",
            "message":"The Serve applicaton is now running and healthy.",
            "type":"Normal",
            "count":8
         },
         {
            "id":"test-v2.17ab1857fcf7e3a4",
            "name":"test-v2-test-v2.17ab1857fcf7e3a4",
            "createdAt":"2024-01-17T09:27:30Z",
            "firstTimestamp":"2024-01-17T09:27:30Z",
            "lastTimestamp":"2024-01-17T09:27:58Z",
            "reason":"ServiceNotReady",
            "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
            "type":"Normal",
            "count":15
         },
         {
            "id":"test-v2.17ab185bc523559a",
            "name":"test-v2-test-v2.17ab185bc523559a",
            "createdAt":"2024-01-17T09:27:46Z",
            "firstTimestamp":"2024-01-17T09:27:46Z",
            "lastTimestamp":"2024-01-17T09:27:46Z",
            "reason":"SubmittedServeDeployment",
            "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-lbl9x",
            "type":"Normal",
            "count":1
         },
         {
            "id":"test-v2.17ab185f245800ff",
            "name":"test-v2-test-v2.17ab185f245800ff",
            "createdAt":"2024-01-17T09:28:00Z",
            "firstTimestamp":"2024-01-17T09:28:00Z",
            "lastTimestamp":"2024-01-17T09:28:11Z",
            "reason":"Running",
            "message":"The Serve applicaton is now running and healthy.",
            "type":"Normal",
            "count":9
         }
      ]
   },
   "createdAt":"2024-01-17T09:31:34Z",
   "deleteAt":"1969-12-31T23:59:59Z"
}
```

</details>

#### List all services in a given namespace

```text
GET {{baseUrl}}/apis/v1/namespaces/<namespace>/services
```

Examples

* Request

  ```sh
  curl --silent -X 'GET' \
  'http://localhost:31888/apis/v1/namespaces/default/services' \
  -H 'accept: application/json'
  ```

<details>
  <summary>* Response</summary>

```json
  {
   "services":[
      {
         "name":"test-v2",
         "namespace":"default",
         "user":"user",
         "serveConfigV2":"applications:\n  - name: fruit_app\n    import_path: fruit.deployment_graph\n    route_prefix: /fruit\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: MangoStand\n        num_replicas: 2\n        max_replicas_per_node: 1\n        user_config:\n          price: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: OrangeStand\n        num_replicas: 1\n        user_config:\n          price: 2\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: PearStand\n        num_replicas: 1\n        user_config:\n          price: 1\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: FruitMarket\n        num_replicas: 1\n        ray_actor_options:\n          num_cpus: 0.1\n  - name: math_app\n    import_path: conditional_dag.serve_dag\n    route_prefix: /calc\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: Adder\n        num_replicas: 1\n        user_config:\n          increment: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Multiplier\n        num_replicas: 1\n        user_config:\n          factor: 5\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Router\n        num_replicas: 1\n",
         "clusterSpec":{
            "headGroupSpec":{
               "computeTemplate":"default-template",
               "image":"rayproject/ray:2.46.0-py310",
               "serviceType":"NodePort",
               "rayStartParams":{
                  "dashboard-host":"0.0.0.0",
                  "metrics-export-port":"8080"
               },
               "environment":{

               }
            },
            "workerGroupSpec":[
               {
                  "groupName":"small-wg",
                  "computeTemplate":"default-template",
                  "image":"rayproject/ray:2.46.0-py310",
                  "replicas":1,
                  "minReplicas":1,
                  "maxReplicas":5,
                  "rayStartParams":{
                     "node-ip-address":"$MY_POD_IP"
                  },
                  "environment":{

                  }
               }
            ]
         },
         "rayServiceStatus":{
            "rayServiceEvents":[
               {
                  "id":"test-v2.17ab175ec787d26e",
                  "name":"test-v2-test-v2.17ab175ec787d26e",
                  "createdAt":"2024-01-17T09:09:39Z",
                  "firstTimestamp":"2024-01-17T09:09:39Z",
                  "lastTimestamp":"2024-01-17T09:10:04Z",
                  "reason":"ServiceNotReady",
                  "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
                  "type":"Normal",
                  "count":13
               },
               {
                  "id":"test-v2.17ab176292337141",
                  "name":"test-v2-test-v2.17ab176292337141",
                  "createdAt":"2024-01-17T09:09:56Z",
                  "firstTimestamp":"2024-01-17T09:09:56Z",
                  "lastTimestamp":"2024-01-17T09:09:56Z",
                  "reason":"SubmittedServeDeployment",
                  "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-lmk5c",
                  "type":"Normal",
                  "count":1
               },
               {
                  "id":"test-v2.17ab1764fe827fe3",
                  "name":"test-v2-test-v2.17ab1764fe827fe3",
                  "createdAt":"2024-01-17T09:10:06Z",
                  "firstTimestamp":"2024-01-17T09:10:06Z",
                  "lastTimestamp":"2024-01-17T09:14:41Z",
                  "reason":"Running",
                  "message":"The Serve applicaton is now running and healthy.",
                  "type":"Normal",
                  "count":139
               },
               {
                  "id":"test-v2.17ab1815ce6a98da",
                  "name":"test-v2-test-v2.17ab1815ce6a98da",
                  "createdAt":"2024-01-17T09:22:45Z",
                  "firstTimestamp":"2024-01-17T09:22:45Z",
                  "lastTimestamp":"2024-01-17T09:23:16Z",
                  "reason":"ServiceNotReady",
                  "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
                  "type":"Normal",
                  "count":16
               },
               {
                  "id":"test-v2.17ab181a8576812d",
                  "name":"test-v2-test-v2.17ab181a8576812d",
                  "createdAt":"2024-01-17T09:23:06Z",
                  "firstTimestamp":"2024-01-17T09:23:06Z",
                  "lastTimestamp":"2024-01-17T09:23:06Z",
                  "reason":"SubmittedServeDeployment",
                  "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-b85bj",
                  "type":"Normal",
                  "count":1
               },
               {
                  "id":"test-v2.17ab181d64327965",
                  "name":"test-v2-test-v2.17ab181d64327965",
                  "createdAt":"2024-01-17T09:23:18Z",
                  "firstTimestamp":"2024-01-17T09:23:18Z",
                  "lastTimestamp":"2024-01-17T09:23:26Z",
                  "reason":"Running",
                  "message":"The Serve applicaton is now running and healthy.",
                  "type":"Normal",
                  "count":8
               },
               {
                  "id":"test-v2.17ab1857fcf7e3a4",
                  "name":"test-v2-test-v2.17ab1857fcf7e3a4",
                  "createdAt":"2024-01-17T09:27:30Z",
                  "firstTimestamp":"2024-01-17T09:27:30Z",
                  "lastTimestamp":"2024-01-17T09:27:58Z",
                  "reason":"ServiceNotReady",
                  "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
                  "type":"Normal",
                  "count":15
               },
               {
                  "id":"test-v2.17ab185bc523559a",
                  "name":"test-v2-test-v2.17ab185bc523559a",
                  "createdAt":"2024-01-17T09:27:46Z",
                  "firstTimestamp":"2024-01-17T09:27:46Z",
                  "lastTimestamp":"2024-01-17T09:27:46Z",
                  "reason":"SubmittedServeDeployment",
                  "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-lbl9x",
                  "type":"Normal",
                  "count":1
               },
               {
                  "id":"test-v2.17ab185f245800ff",
                  "name":"test-v2-test-v2.17ab185f245800ff",
                  "createdAt":"2024-01-17T09:28:00Z",
                  "firstTimestamp":"2024-01-17T09:28:00Z",
                  "lastTimestamp":"2024-01-17T09:28:11Z",
                  "reason":"Running",
                  "message":"The Serve applicaton is now running and healthy.",
                  "type":"Normal",
                  "count":9
               },
               {
                  "id":"test-v2.17ab189170a9c462",
                  "name":"test-v2-test-v2.17ab189170a9c462",
                  "createdAt":"2024-01-17T09:31:36Z",
                  "firstTimestamp":"2024-01-17T09:31:36Z",
                  "lastTimestamp":"2024-01-17T09:32:08Z",
                  "reason":"ServiceNotReady",
                  "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
                  "type":"Normal",
                  "count":16
               },
               {
                  "id":"test-v2.17ab18965aab5e92",
                  "name":"test-v2-test-v2.17ab18965aab5e92",
                  "createdAt":"2024-01-17T09:31:57Z",
                  "firstTimestamp":"2024-01-17T09:31:57Z",
                  "lastTimestamp":"2024-01-17T09:31:57Z",
                  "reason":"SubmittedServeDeployment",
                  "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-qhrmk",
                  "type":"Normal",
                  "count":1
               },
               {
                  "id":"test-v2.17ab18993b7ef94e",
                  "name":"test-v2-test-v2.17ab18993b7ef94e",
                  "createdAt":"2024-01-17T09:32:10Z",
                  "firstTimestamp":"2024-01-17T09:32:10Z",
                  "lastTimestamp":"2024-01-17T09:36:36Z",
                  "reason":"Running",
                  "message":"The Serve applicaton is now running and healthy.",
                  "type":"Normal",
                  "count":135
               },
               {
                  "id":"test-v2-raycluster-qhrmk.17ab1891669f9337",
                  "name":"test-v2-test-v2-raycluster-qhrmk.17ab1891669f9337",
                  "createdAt":"2024-01-17T09:31:36Z",
                  "firstTimestamp":"2024-01-17T09:31:36Z",
                  "lastTimestamp":"2024-01-17T09:31:36Z",
                  "reason":"Created",
                  "message":"Created service test-v2-raycluster-qhrmk-head-svc",
                  "type":"Normal",
                  "count":1
               },
               {
                  "id":"test-v2-raycluster-qhrmk.17ab1891683f5579",
                  "name":"test-v2-test-v2-raycluster-qhrmk.17ab1891683f5579",
                  "createdAt":"2024-01-17T09:31:36Z",
                  "firstTimestamp":"2024-01-17T09:31:36Z",
                  "lastTimestamp":"2024-01-17T09:31:36Z",
                  "reason":"Created",
                  "message":"Created head pod test-v2-raycluster-qhrmk-head-8kptm",
                  "type":"Normal",
                  "count":1
               },
               {
                  "id":"test-v2-raycluster-qhrmk.17ab18916aa9fca3",
                  "name":"test-v2-test-v2-raycluster-qhrmk.17ab18916aa9fca3",
                  "createdAt":"2024-01-17T09:31:36Z",
                  "firstTimestamp":"2024-01-17T09:31:36Z",
                  "lastTimestamp":"2024-01-17T09:31:36Z",
                  "reason":"Created",
                  "message":"Created worker pod ",
                  "type":"Normal",
                  "count":1
               }
            ],
            "rayClusterName":"test-v2-raycluster-qhrmk",
            "serveApplicationStatus":[
               {
                  "name":"fruit_app",
                  "status":"RUNNING",
                  "serveDeploymentStatus":[
                     {
                        "deploymentName":"PearStand",
                        "status":"HEALTHY"
                     },
                     {
                        "deploymentName":"FruitMarket",
                        "status":"HEALTHY"
                     },
                     {
                        "deploymentName":"MangoStand",
                        "status":"HEALTHY"
                     },
                     {
                        "deploymentName":"OrangeStand",
                        "status":"HEALTHY"
                     }
                  ]
               },
               {
                  "name":"math_app",
                  "status":"RUNNING",
                  "serveDeploymentStatus":[
                     {
                        "deploymentName":"Adder",
                        "status":"HEALTHY"
                     },
                     {
                        "deploymentName":"Multiplier",
                        "status":"HEALTHY"
                     },
                     {
                        "deploymentName":"Router",
                        "status":"HEALTHY"
                     }
                  ]
               }
            ]
         },
         "createdAt":"2024-01-17T09:31:34Z",
         "deleteAt":"1969-12-31T23:59:59Z"
      }
   ]
}
```

</details>

#### List all services in all namespaces

```text
GET {{baseUrl}}/apis/v1/services
```

Examples:

* Request

  ```sh
  curl --silent -X 'GET' \
  'http://localhost:31888/apis/v1/services' \
  -H 'accept: application/json'
  ```

<details>
  <summary>* Response</summary>

```json
{
   "services":[
      {
         "name":"test-v2",
         "namespace":"default",
         "user":"user",
         "serveConfigV2":"applications:\n  - name: fruit_app\n    import_path: fruit.deployment_graph\n    route_prefix: /fruit\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: MangoStand\n        num_replicas: 2\n        max_replicas_per_node: 1\n        user_config:\n          price: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: OrangeStand\n        num_replicas: 1\n        user_config:\n          price: 2\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: PearStand\n        num_replicas: 1\n        user_config:\n          price: 1\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: FruitMarket\n        num_replicas: 1\n        ray_actor_options:\n          num_cpus: 0.1\n  - name: math_app\n    import_path: conditional_dag.serve_dag\n    route_prefix: /calc\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: Adder\n        num_replicas: 1\n        user_config:\n          increment: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Multiplier\n        num_replicas: 1\n        user_config:\n          factor: 5\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Router\n        num_replicas: 1\n",
         "clusterSpec":{
            "headGroupSpec":{
               "computeTemplate":"default-template",
               "image":"rayproject/ray:2.46.0-py310",
               "serviceType":"NodePort",
               "rayStartParams":{
                  "dashboard-host":"0.0.0.0",
                  "metrics-export-port":"8080"
               },
               "environment":{

               }
            },
            "workerGroupSpec":[
               {
                  "groupName":"small-wg",
                  "computeTemplate":"default-template",
                  "image":"rayproject/ray:2.46.0-py310",
                  "replicas":1,
                  "minReplicas":1,
                  "maxReplicas":5,
                  "rayStartParams":{
                     "node-ip-address":"$MY_POD_IP"
                  },
                  "environment":{

                  }
               }
            ]
         },
         "rayServiceStatus":{
            "rayServiceEvents":[
               {
                  "id":"test-v2.17ab175ec787d26e",
                  "name":"test-v2-test-v2.17ab175ec787d26e",
                  "createdAt":"2024-01-17T09:09:39Z",
                  "firstTimestamp":"2024-01-17T09:09:39Z",
                  "lastTimestamp":"2024-01-17T09:10:04Z",
                  "reason":"ServiceNotReady",
                  "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
                  "type":"Normal",
                  "count":13
               },
               {
                  "id":"test-v2.17ab176292337141",
                  "name":"test-v2-test-v2.17ab176292337141",
                  "createdAt":"2024-01-17T09:09:56Z",
                  "firstTimestamp":"2024-01-17T09:09:56Z",
                  "lastTimestamp":"2024-01-17T09:09:56Z",
                  "reason":"SubmittedServeDeployment",
                  "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-lmk5c",
                  "type":"Normal",
                  "count":1
               },
               {
                  "id":"test-v2.17ab1764fe827fe3",
                  "name":"test-v2-test-v2.17ab1764fe827fe3",
                  "createdAt":"2024-01-17T09:10:06Z",
                  "firstTimestamp":"2024-01-17T09:10:06Z",
                  "lastTimestamp":"2024-01-17T09:14:41Z",
                  "reason":"Running",
                  "message":"The Serve applicaton is now running and healthy.",
                  "type":"Normal",
                  "count":139
               },
               {
                  "id":"test-v2.17ab1815ce6a98da",
                  "name":"test-v2-test-v2.17ab1815ce6a98da",
                  "createdAt":"2024-01-17T09:22:45Z",
                  "firstTimestamp":"2024-01-17T09:22:45Z",
                  "lastTimestamp":"2024-01-17T09:23:16Z",
                  "reason":"ServiceNotReady",
                  "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
                  "type":"Normal",
                  "count":16
               },
               {
                  "id":"test-v2.17ab181a8576812d",
                  "name":"test-v2-test-v2.17ab181a8576812d",
                  "createdAt":"2024-01-17T09:23:06Z",
                  "firstTimestamp":"2024-01-17T09:23:06Z",
                  "lastTimestamp":"2024-01-17T09:23:06Z",
                  "reason":"SubmittedServeDeployment",
                  "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-b85bj",
                  "type":"Normal",
                  "count":1
               },
               {
                  "id":"test-v2.17ab181d64327965",
                  "name":"test-v2-test-v2.17ab181d64327965",
                  "createdAt":"2024-01-17T09:23:18Z",
                  "firstTimestamp":"2024-01-17T09:23:18Z",
                  "lastTimestamp":"2024-01-17T09:23:26Z",
                  "reason":"Running",
                  "message":"The Serve applicaton is now running and healthy.",
                  "type":"Normal",
                  "count":8
               },
               {
                  "id":"test-v2.17ab1857fcf7e3a4",
                  "name":"test-v2-test-v2.17ab1857fcf7e3a4",
                  "createdAt":"2024-01-17T09:27:30Z",
                  "firstTimestamp":"2024-01-17T09:27:30Z",
                  "lastTimestamp":"2024-01-17T09:27:58Z",
                  "reason":"ServiceNotReady",
                  "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
                  "type":"Normal",
                  "count":15
               },
               {
                  "id":"test-v2.17ab185bc523559a",
                  "name":"test-v2-test-v2.17ab185bc523559a",
                  "createdAt":"2024-01-17T09:27:46Z",
                  "firstTimestamp":"2024-01-17T09:27:46Z",
                  "lastTimestamp":"2024-01-17T09:27:46Z",
                  "reason":"SubmittedServeDeployment",
                  "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-lbl9x",
                  "type":"Normal",
                  "count":1
               },
               {
                  "id":"test-v2.17ab185f245800ff",
                  "name":"test-v2-test-v2.17ab185f245800ff",
                  "createdAt":"2024-01-17T09:28:00Z",
                  "firstTimestamp":"2024-01-17T09:28:00Z",
                  "lastTimestamp":"2024-01-17T09:28:11Z",
                  "reason":"Running",
                  "message":"The Serve applicaton is now running and healthy.",
                  "type":"Normal",
                  "count":9
               },
               {
                  "id":"test-v2.17ab189170a9c462",
                  "name":"test-v2-test-v2.17ab189170a9c462",
                  "createdAt":"2024-01-17T09:31:36Z",
                  "firstTimestamp":"2024-01-17T09:31:36Z",
                  "lastTimestamp":"2024-01-17T09:32:08Z",
                  "reason":"ServiceNotReady",
                  "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
                  "type":"Normal",
                  "count":16
               },
               {
                  "id":"test-v2.17ab18965aab5e92",
                  "name":"test-v2-test-v2.17ab18965aab5e92",
                  "createdAt":"2024-01-17T09:31:57Z",
                  "firstTimestamp":"2024-01-17T09:31:57Z",
                  "lastTimestamp":"2024-01-17T09:31:57Z",
                  "reason":"SubmittedServeDeployment",
                  "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-qhrmk",
                  "type":"Normal",
                  "count":1
               },
               {
                  "id":"test-v2.17ab18993b7ef94e",
                  "name":"test-v2-test-v2.17ab18993b7ef94e",
                  "createdAt":"2024-01-17T09:32:10Z",
                  "firstTimestamp":"2024-01-17T09:32:10Z",
                  "lastTimestamp":"2024-01-17T09:41:37Z",
                  "reason":"Running",
                  "message":"The Serve applicaton is now running and healthy.",
                  "type":"Normal",
                  "count":284
               },
               {
                  "id":"test-v2-raycluster-qhrmk.17ab1891669f9337",
                  "name":"test-v2-test-v2-raycluster-qhrmk.17ab1891669f9337",
                  "createdAt":"2024-01-17T09:31:36Z",
                  "firstTimestamp":"2024-01-17T09:31:36Z",
                  "lastTimestamp":"2024-01-17T09:31:36Z",
                  "reason":"Created",
                  "message":"Created service test-v2-raycluster-qhrmk-head-svc",
                  "type":"Normal",
                  "count":1
               },
               {
                  "id":"test-v2-raycluster-qhrmk.17ab1891683f5579",
                  "name":"test-v2-test-v2-raycluster-qhrmk.17ab1891683f5579",
                  "createdAt":"2024-01-17T09:31:36Z",
                  "firstTimestamp":"2024-01-17T09:31:36Z",
                  "lastTimestamp":"2024-01-17T09:31:36Z",
                  "reason":"Created",
                  "message":"Created head pod test-v2-raycluster-qhrmk-head-8kptm",
                  "type":"Normal",
                  "count":1
               },
               {
                  "id":"test-v2-raycluster-qhrmk.17ab18916aa9fca3",
                  "name":"test-v2-test-v2-raycluster-qhrmk.17ab18916aa9fca3",
                  "createdAt":"2024-01-17T09:31:36Z",
                  "firstTimestamp":"2024-01-17T09:31:36Z",
                  "lastTimestamp":"2024-01-17T09:31:36Z",
                  "reason":"Created",
                  "message":"Created worker pod ",
                  "type":"Normal",
                  "count":1
               }
            ],
            "rayClusterName":"test-v2-raycluster-qhrmk",
            "serveApplicationStatus":[
               {
                  "name":"fruit_app",
                  "status":"RUNNING",
                  "serveDeploymentStatus":[
                     {
                        "deploymentName":"FruitMarket",
                        "status":"HEALTHY"
                     },
                     {
                        "deploymentName":"MangoStand",
                        "status":"HEALTHY"
                     },
                     {
                        "deploymentName":"OrangeStand",
                        "status":"HEALTHY"
                     },
                     {
                        "deploymentName":"PearStand",
                        "status":"HEALTHY"
                     }
                  ]
               },
               {
                  "name":"math_app",
                  "status":"RUNNING",
                  "serveDeploymentStatus":[
                     {
                        "deploymentName":"Adder",
                        "status":"HEALTHY"
                     },
                     {
                        "deploymentName":"Multiplier",
                        "status":"HEALTHY"
                     },
                     {
                        "deploymentName":"Router",
                        "status":"HEALTHY"
                     }
                  ]
               }
            ]
         },
         "createdAt":"2024-01-17T09:31:34Z",
         "deleteAt":"1969-12-31T23:59:59Z"
      }
   ]
}
```

</details>

#### Get service by its name and namespace

```text
GET {{baseUrl}}/apis/v1/namespaces/<namespace>/services/<service_name>
```

Examples:

* Request:

  ```sh
  curl --silent -X 'GET' \
    'http://localhost:31888/apis/v1/namespaces/default/services/test-v2' \
    -H 'accept: application/json'
  ```

<details>
  <summary>* Response</summary>

```json
{
   "name":"test-v2",
   "namespace":"default",
   "user":"user",
   "serveConfigV2":"applications:\n  - name: fruit_app\n    import_path: fruit.deployment_graph\n    route_prefix: /fruit\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: MangoStand\n        num_replicas: 2\n        max_replicas_per_node: 1\n        user_config:\n          price: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: OrangeStand\n        num_replicas: 1\n        user_config:\n          price: 2\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: PearStand\n        num_replicas: 1\n        user_config:\n          price: 1\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: FruitMarket\n        num_replicas: 1\n        ray_actor_options:\n          num_cpus: 0.1\n  - name: math_app\n    import_path: conditional_dag.serve_dag\n    route_prefix: /calc\n    runtime_env:\n      working_dir: \"https://github.com/ray-project/test_dag/archive/78b4a5da38796123d9f9ffff59bab2792a043e95.zip\"\n    deployments:\n      - name: Adder\n        num_replicas: 1\n        user_config:\n          increment: 3\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Multiplier\n        num_replicas: 1\n        user_config:\n          factor: 5\n        ray_actor_options:\n          num_cpus: 0.1\n      - name: Router\n        num_replicas: 1\n",
   "clusterSpec":{
      "headGroupSpec":{
         "computeTemplate":"default-template",
         "image":"rayproject/ray:2.46.0-py310",
         "serviceType":"NodePort",
         "rayStartParams":{
            "dashboard-host":"0.0.0.0",
            "metrics-export-port":"8080"
         },
         "environment":{

         }
      },
      "workerGroupSpec":[
         {
            "groupName":"small-wg",
            "computeTemplate":"default-template",
            "image":"rayproject/ray:2.46.0-py310",
            "replicas":1,
            "minReplicas":1,
            "maxReplicas":5,
            "rayStartParams":{
               "node-ip-address":"$MY_POD_IP"
            },
            "environment":{

            }
         }
      ]
   },
   "rayServiceStatus":{
      "rayServiceEvents":[
         {
            "id":"test-v2.17ab175ec787d26e",
            "name":"test-v2-test-v2.17ab175ec787d26e",
            "createdAt":"2024-01-17T09:09:39Z",
            "firstTimestamp":"2024-01-17T09:09:39Z",
            "lastTimestamp":"2024-01-17T09:10:04Z",
            "reason":"ServiceNotReady",
            "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
            "type":"Normal",
            "count":13
         },
         {
            "id":"test-v2.17ab176292337141",
            "name":"test-v2-test-v2.17ab176292337141",
            "createdAt":"2024-01-17T09:09:56Z",
            "firstTimestamp":"2024-01-17T09:09:56Z",
            "lastTimestamp":"2024-01-17T09:09:56Z",
            "reason":"SubmittedServeDeployment",
            "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-lmk5c",
            "type":"Normal",
            "count":1
         },
         {
            "id":"test-v2.17ab1764fe827fe3",
            "name":"test-v2-test-v2.17ab1764fe827fe3",
            "createdAt":"2024-01-17T09:10:06Z",
            "firstTimestamp":"2024-01-17T09:10:06Z",
            "lastTimestamp":"2024-01-17T09:14:41Z",
            "reason":"Running",
            "message":"The Serve applicaton is now running and healthy.",
            "type":"Normal",
            "count":139
         },
         {
            "id":"test-v2.17ab1815ce6a98da",
            "name":"test-v2-test-v2.17ab1815ce6a98da",
            "createdAt":"2024-01-17T09:22:45Z",
            "firstTimestamp":"2024-01-17T09:22:45Z",
            "lastTimestamp":"2024-01-17T09:23:16Z",
            "reason":"ServiceNotReady",
            "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
            "type":"Normal",
            "count":16
         },
         {
            "id":"test-v2.17ab181a8576812d",
            "name":"test-v2-test-v2.17ab181a8576812d",
            "createdAt":"2024-01-17T09:23:06Z",
            "firstTimestamp":"2024-01-17T09:23:06Z",
            "lastTimestamp":"2024-01-17T09:23:06Z",
            "reason":"SubmittedServeDeployment",
            "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-b85bj",
            "type":"Normal",
            "count":1
         },
         {
            "id":"test-v2.17ab181d64327965",
            "name":"test-v2-test-v2.17ab181d64327965",
            "createdAt":"2024-01-17T09:23:18Z",
            "firstTimestamp":"2024-01-17T09:23:18Z",
            "lastTimestamp":"2024-01-17T09:23:26Z",
            "reason":"Running",
            "message":"The Serve applicaton is now running and healthy.",
            "type":"Normal",
            "count":8
         },
         {
            "id":"test-v2.17ab1857fcf7e3a4",
            "name":"test-v2-test-v2.17ab1857fcf7e3a4",
            "createdAt":"2024-01-17T09:27:30Z",
            "firstTimestamp":"2024-01-17T09:27:30Z",
            "lastTimestamp":"2024-01-17T09:27:58Z",
            "reason":"ServiceNotReady",
            "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
            "type":"Normal",
            "count":15
         },
         {
            "id":"test-v2.17ab185bc523559a",
            "name":"test-v2-test-v2.17ab185bc523559a",
            "createdAt":"2024-01-17T09:27:46Z",
            "firstTimestamp":"2024-01-17T09:27:46Z",
            "lastTimestamp":"2024-01-17T09:27:46Z",
            "reason":"SubmittedServeDeployment",
            "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-lbl9x",
            "type":"Normal",
            "count":1
         },
         {
            "id":"test-v2.17ab185f245800ff",
            "name":"test-v2-test-v2.17ab185f245800ff",
            "createdAt":"2024-01-17T09:28:00Z",
            "firstTimestamp":"2024-01-17T09:28:00Z",
            "lastTimestamp":"2024-01-17T09:28:11Z",
            "reason":"Running",
            "message":"The Serve applicaton is now running and healthy.",
            "type":"Normal",
            "count":9
         },
         {
            "id":"test-v2.17ab189170a9c462",
            "name":"test-v2-test-v2.17ab189170a9c462",
            "createdAt":"2024-01-17T09:31:36Z",
            "firstTimestamp":"2024-01-17T09:31:36Z",
            "lastTimestamp":"2024-01-17T09:32:08Z",
            "reason":"ServiceNotReady",
            "message":"The service is not ready yet. Controller will perform a round of actions in 2s.",
            "type":"Normal",
            "count":16
         },
         {
            "id":"test-v2.17ab18965aab5e92",
            "name":"test-v2-test-v2.17ab18965aab5e92",
            "createdAt":"2024-01-17T09:31:57Z",
            "firstTimestamp":"2024-01-17T09:31:57Z",
            "lastTimestamp":"2024-01-17T09:31:57Z",
            "reason":"SubmittedServeDeployment",
            "message":"Controller sent API request to update Serve deployments on cluster test-v2-raycluster-qhrmk",
            "type":"Normal",
            "count":1
         },
         {
            "id":"test-v2.17ab18993b7ef94e",
            "name":"test-v2-test-v2.17ab18993b7ef94e",
            "createdAt":"2024-01-17T09:32:10Z",
            "firstTimestamp":"2024-01-17T09:32:10Z",
            "lastTimestamp":"2024-01-17T09:46:38Z",
            "reason":"Running",
            "message":"The Serve applicaton is now running and healthy.",
            "type":"Normal",
            "count":433
         },
         {
            "id":"test-v2-raycluster-qhrmk.17ab1891669f9337",
            "name":"test-v2-test-v2-raycluster-qhrmk.17ab1891669f9337",
            "createdAt":"2024-01-17T09:31:36Z",
            "firstTimestamp":"2024-01-17T09:31:36Z",
            "lastTimestamp":"2024-01-17T09:31:36Z",
            "reason":"Created",
            "message":"Created service test-v2-raycluster-qhrmk-head-svc",
            "type":"Normal",
            "count":1
         },
         {
            "id":"test-v2-raycluster-qhrmk.17ab1891683f5579",
            "name":"test-v2-test-v2-raycluster-qhrmk.17ab1891683f5579",
            "createdAt":"2024-01-17T09:31:36Z",
            "firstTimestamp":"2024-01-17T09:31:36Z",
            "lastTimestamp":"2024-01-17T09:31:36Z",
            "reason":"Created",
            "message":"Created head pod test-v2-raycluster-qhrmk-head-8kptm",
            "type":"Normal",
            "count":1
         },
         {
            "id":"test-v2-raycluster-qhrmk.17ab18916aa9fca3",
            "name":"test-v2-test-v2-raycluster-qhrmk.17ab18916aa9fca3",
            "createdAt":"2024-01-17T09:31:36Z",
            "firstTimestamp":"2024-01-17T09:31:36Z",
            "lastTimestamp":"2024-01-17T09:31:36Z",
            "reason":"Created",
            "message":"Created worker pod ",
            "type":"Normal",
            "count":1
         }
      ],
      "rayClusterName":"test-v2-raycluster-qhrmk",
      "serveApplicationStatus":[
         {
            "name":"fruit_app",
            "status":"RUNNING",
            "serveDeploymentStatus":[
               {
                  "deploymentName":"PearStand",
                  "status":"HEALTHY"
               },
               {
                  "deploymentName":"FruitMarket",
                  "status":"HEALTHY"
               },
               {
                  "deploymentName":"MangoStand",
                  "status":"HEALTHY"
               },
               {
                  "deploymentName":"OrangeStand",
                  "status":"HEALTHY"
               }
            ]
         },
         {
            "name":"math_app",
            "status":"RUNNING",
            "serveDeploymentStatus":[
               {
                  "deploymentName":"Adder",
                  "status":"HEALTHY"
               },
               {
                  "deploymentName":"Multiplier",
                  "status":"HEALTHY"
               },
               {
                  "deploymentName":"Router",
                  "status":"HEALTHY"
               }
            ]
         }
      ]
   },
   "createdAt":"2024-01-17T09:31:34Z",
   "deleteAt":"1969-12-31T23:59:59Z"
}
```

</details>

#### Delete service by its name and namespace

```text
DELETE {{baseUrl}}/apis/v1/namespaces/<namespace>/services/<service_name>
```

Examples:

* Request

  ```sh
  curl --silent -X 'DELETE' \
  'http://localhost:31888/apis/v1/namespaces/default/services/test-v2' \
  -H 'accept: application/json'
  ```

* Response

  ```json
  {}
  ```
