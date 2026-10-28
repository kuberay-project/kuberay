# Job Submission using APIServer

Ray provides convenient and powerful [Job Submission APIs]. However, it requires access to
the cluster's dashboard Ingress, which currently lacks security implementations.
Alternatively, you can use the Ray cluster head service for this, but in that case, your
Ray job management code must run in the same Kubernetes cluster as Ray. The job submission
implementation in the APIServer leverages the already exposed URL of the APIServer to
locate the cluster and use its head service to implement job management functionality.
Since you can access the APIServer running on a remote Kubernetes cluster, you can use
these APIs to manage remote Ray clusters without exposing them via Ingress.

## Using Job Submission APIs

Note that job submission APIs will only work if you are running the APIServer within a
Kubernetes cluster. The local development option of the APIServer will not work.

Before proceeding with the example, remove any running RayClusters to ensure the successful
execution of the steps below.

```sh
kubectl delete raycluster --all
```

### Deploy KubeRay operator and APIServer

Refer to the [Install with Helm](README.md#install-with-helm) section in the README for
setting up the KubeRay operator and APIServer, and port-forward the HTTP endpoint to local
port 31888.

### Install ConfigMap

Note that this cluster is mounting a volume from a ConfigMap. This ConfigMap should be
created prior to cluster creation using [this YAML].

We will use this [ConfigMap], which contains the code for our example. Please download the
ConfigMap and deploy it with the following command:

```sh
kubectl apply -f code_configmap.yaml
```

### Deploy RayCluster

Execute the following commands to deploy the RayCluster:

```sh
curl -X POST 'localhost:31888/apis/v1/namespaces/default/compute_templates' \
    --header 'Content-Type: application/json' \
    --data @docs/api-example/compute_template.json

curl -X POST 'localhost:31888/apis/v1/namespaces/default/clusters' \
  --header 'Content-Type: application/json' \
  --data @docs/api-example/jobsubmission_clusters.json
```

To verify if the RayCluster is set up correctly, list all pods with the following command.
You should see a head and worker node for `test-cluster` running:

```sh
kubectl get pods
# NAME                                 READY   STATUS    RESTARTS   AGE
# test-cluster-head                    1/1     Running   0          4m9s
# test-cluster-small-wg-worker-c8t2w   1/1     Running   0          4m9s
```

### Submit Ray Job

Once the cluster is up and running, submit a job to the cluster using the following command:

```sh
curl -X POST 'localhost:31888/apis/v1/namespaces/default/jobsubmissions/test-cluster' \
    --header 'Content-Type: application/json' \
    --data '{
    "entrypoint": "python /home/ray/samples/sample_code.py",
    "runtimeEnv": "pip:\n  - requests==2.26.0\n  - pendulum==2.1.2\nenv_vars:\n  counter_name: test_counter\n",
    "numCpus": ".5"
    }'
```

This should return the following:

```json
{
  "submissionId":"raysubmit_KWZLwme56esG3Wcr"
}
```

Note that the `submissionId` value you receive will be different.

### Get job details

Once the job is submitted, use the following command to get the job's details. Please
replace the `submissionId` with the one returned during job creation.

```shell
curl -X GET 'localhost:31888/apis/v1/namespaces/default/jobsubmissions/test-cluster/<submissionID>' \
  --header 'Content-Type: application/json'
```

This should return JSON similar to the example below:

```json
{
   "entrypoint":"python /home/ray/samples/sample_code.py",
   "jobId":"02000000",
   "submissionId":"raysubmit_KWZLwme56esG3Wcr",
   "status":"SUCCEEDED",
   "message":"Job finished successfully.",
   "startTime":"1699442662879",
   "endTime":"1699442682405",
   "runtimeEnv":{
      "env_vars":"map[counter_name:test_counter]",
      "pip":"[requests==2.26.0 pendulum==2.1.2]"
   }
}
```

### Get Job log

You can also retrieve the job execution log using the following command. Please replace
the `<submissionID>` with the one returned during job creation.

```shell
curl -X GET 'localhost:31888/apis/v1/namespaces/default/jobsubmissions/test-cluster/log/<submissionID>' \
  --header 'Content-Type: application/json'
```

This will return the execution log, which will look something like the following:

```text
2023-11-08 03:24:31,904\tINFO worker.py:1329 -- Using address 10.244.2.2:6379 set in the environment variable RAY_ADDRESS
2023-11-08 03:24:31,905\tINFO worker.py:1458 -- Connecting to existing Ray cluster at address: 10.244.2.2:6379...
2023-11-08 03:24:31,921\tINFO worker.py:1633 -- Connected to Ray cluster. View the dashboard at 10.244.2.2:8265
test_counter got 1
test_counter got 2
test_counter got 3
test_counter got 4
test_counter got 5
```

Note that this command always returns the execution log from the beginning (no streaming
support) up to the current moment.

### List jobs

You can also list all jobs (in any state) in the Ray cluster using the following command:

```shell
curl -X GET 'localhost:31888/apis/v1/namespaces/default/jobsubmissions/test-cluster' \
  --header 'Content-Type: application/json'
```

This should return a list of submissions, which looks as follows:

```json
{
   "submissions":[
      {
         "entrypoint":"python /home/ray/samples/sample_code.py",
         "jobId":"02000000",
         "submissionId":"raysubmit_KWZLwme56esG3Wcr",
         "status":"SUCCEEDED",
         "message":"Job finished successfully.",
         "startTime":"1699442662879",
         "endTime":"1699442682405",
         "runtimeEnv":{
            "env_vars":"map[counter_name:test_counter]",
            "pip":"[requests==2.26.0 pendulum==2.1.2]"
         }
      }
   ]
}
```

### Stop Job

A job can be stopped using the following command. Please replace the `<submissionID>` with
the one returned during job creation.

```sh
curl -X POST 'localhost:31888/apis/v1/namespaces/default/jobsubmissions/test-cluster/<submissionID>' \
  --header 'Content-Type: application/json'
```

### Delete Job

Finally, you can delete a job using the following command. Please replace the
`<submissionID>` with the one returned during job creation.

```sh
curl -X DELETE 'localhost:31888/apis/v1/namespaces/default/jobsubmissions/test-cluster/<submissionID>' \
  --header 'Content-Type: application/json'
```

You can confirm the job deletion by listing jobs again. You should see an empty list.

### Clean up

```sh
make clean-cluster
# Remove APIServer from helm
helm uninstall kuberay-apiserver
```

[Job Submission APIs]: https://docs.ray.io/en/latest/cluster/running-applications/job-submission/rest.html
[ConfigMap]: test/job/code.yaml
