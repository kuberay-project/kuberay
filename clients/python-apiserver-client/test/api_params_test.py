import json

from python_apiserver_client.params import (
    DEFAULT_HEAD_START_PARAMS,
    DEFAULT_WORKER_START_PARAMS,
    AccessMode,
    AutoscalerOptions,
    Cluster,
    ClusterEvent,
    ClusterSpec,
    ConfigMapVolume,
    EmptyDirVolume,
    Environment,
    EnvironmentVariables,
    EnvVarFrom,
    EnvVarSource,
    EphemeralVolume,
    HeadNodeSpec,
    HostPath,
    HostPathVolume,
    MountPropagationMode,
    PVCVolume,
    RayJobInfo,
    RayJobRequest,
    SecretVolume,
    ServiceType,
    Template,
    Toleration,
    TolerationEffect,
    TolerationOperation,
    WorkerNodeSpec,
    autoscaling_decoder,
    cluster_decoder,
    cluster_spec_decoder,
    env_var_from_decoder,
    environment_variables_decoder,
    head_node_spec_decoder,
    template_decoder,
    toleration_decoder,
    volume_decoder,
    worker_node_spec_decoder,
)


def test_toleration():

    tol1 = Toleration(key="blah1", operator=TolerationOperation.Exists, effect=TolerationEffect.NoExecute)
    print(f"\ntoleration 1: {tol1.to_string()}")
    t1_json = json.dumps(tol1.to_dict())
    print(f"toleration 1 JSON: {t1_json}")

    tol2 = Toleration(
        key="blah2", operator=TolerationOperation.Exists, effect=TolerationEffect.NoExecute, value="value"
    )
    print(f"toleration 2: {tol2.to_string()}")
    t2_json = json.dumps(tol2.to_dict())
    print(f"toleration 2 JSON: {t2_json}")

    assert tol1.to_string() == toleration_decoder(json.loads(t1_json)).to_string()
    assert tol2.to_string() == toleration_decoder(json.loads(t2_json)).to_string()


def test_templates():

    tol1 = Toleration(key="blah1", operator=TolerationOperation.Exists, effect=TolerationEffect.NoExecute)
    tol2 = Toleration(
        key="blah2", operator=TolerationOperation.Exists, effect=TolerationEffect.NoExecute, value="value"
    )

    temp1 = Template(name="template1", namespace="namespace", cpu=1, memory=4, tolerations=[tol1, tol2])
    print(f"\ntemplate 1: {temp1.to_string()}")
    tm1_json = json.dumps(temp1.to_dict())
    print(f"template 1 JSON: {tm1_json}")

    temp2 = Template(name="template2", namespace="namespace", cpu=2, memory=8, gpu=1)
    print(f"template 2: {temp2.to_string()}")
    tm2_json = json.dumps(temp2.to_dict())
    print(f"template 2 JSON: {tm2_json}")

    temp3 = Template(name="template3", namespace="namespace", cpu=2, memory=8, gpu=1, extended_resources={"vpc.amazonaws.com/efa": 32})
    print(f"template 3: {temp3.to_string()}")
    tm3_json = json.dumps(temp3.to_dict())
    print(f"template 3 JSON: {tm3_json}")

    assert temp1.to_string() == template_decoder(json.loads(tm1_json)).to_string()
    assert temp2.to_string() == template_decoder(json.loads(tm2_json)).to_string()
    assert temp3.to_string() == template_decoder(json.loads(tm3_json)).to_string()


def test_volumes():

    # hostPath
    vol = HostPathVolume(
        name="hostPath",
        mount_path="tmp/hostPath",
        source="source",
        host_path_type=HostPath.FILE,
        mount_propagation=MountPropagationMode.NONE,
    )
    print(f"\nhostPath volume: {vol.to_string()}")
    vol_json = json.dumps(vol.to_dict())
    print(f"host path volume json: {vol_json}")
    assert volume_decoder(json.loads(vol_json)).to_string() == vol.to_string()

    vol = PVCVolume(
        name="pvc",
        mount_path="tmp/pvc",
        source="claim",
        read_only=True,
        mount_propagation=MountPropagationMode.BIDIRECTIONAL,
    )
    print(f"PVC volume: {vol.to_string()}")
    vol_json = json.dumps(vol.to_dict())
    print(f"PVC volume json: {vol_json}")
    assert volume_decoder(json.loads(vol_json)).to_string() == vol.to_string()

    vol = EphemeralVolume(
        name="ephemeral", mount_path="tmp/ephemeral", storage="5Gi", storage_class="blah", access_mode=AccessMode.RWX
    )
    print(f"Ephemeral volume: {vol.to_string()}")
    vol_json = json.dumps(vol.to_dict())
    print(f"Ephemeral volume json: {vol_json}")
    assert volume_decoder(json.loads(vol_json)).to_string() == vol.to_string()

    vol = EmptyDirVolume(name="emptyDir", mount_path="tmp/emptyDir")
    print(f"Empty dir volume: {vol.to_string()}")
    vol_json = json.dumps(vol.to_dict())
    print(f"Empty dir volume json: {vol_json}")
    assert volume_decoder(json.loads(vol_json)).to_string() == vol.to_string()

    vol = ConfigMapVolume(
        name="confmap", mount_path="tmp/confmap", source="my-map", items={"sample_code.py": "sample_code.py"}
    )
    print(f"config map volume: {vol.to_string()}")
    vol_json = json.dumps(vol.to_dict())
    print(f"config map volume json: {vol_json}")
    assert volume_decoder(json.loads(vol_json)).to_string() == vol.to_string()

    vol = SecretVolume(name="secret", mount_path="tmp/secret", source="my-secret")
    print(f"secret volume: {vol.to_string()}")
    vol_json = json.dumps(vol.to_dict())
    print(f"secret volume json: {vol_json}")
    assert volume_decoder(json.loads(vol_json)).to_string() == vol.to_string()


def test_environment():

    env_v = EnvVarFrom(source=EnvVarSource.SECRET, name="my-secret", key="key")
    print(f"\nEnv variable from: {env_v.to_string()}")
    env_v_json = json.dumps(env_v.to_dict())
    print(f"Env variable from JSON: {env_v_json}")
    assert env_var_from_decoder(json.loads(env_v_json)).to_string() == env_v.to_string()

    envs = EnvironmentVariables(key_value={"key": "val"}, from_ref={"key_ref": env_v})
    print(f"Env variables: {envs.to_string()}")
    envs_json = json.dumps(envs.to_dict())
    print(f"Env variables JSON: {envs_json}")
    assert environment_variables_decoder(json.loads(envs_json)).to_string() == envs.to_string()

    envs = EnvironmentVariables(from_ref={"key_ref": env_v})
    print(f"Env variables: {envs.to_string()}")
    envs_json = json.dumps(envs.to_dict())
    print(f"Env variables JSON: {envs_json}")
    assert environment_variables_decoder(json.loads(envs_json)).to_string() == envs.to_string()

    envs = EnvironmentVariables(key_value={"key": "val"})
    print(f"Env variables: {envs.to_string()}")
    envs_json = json.dumps(envs.to_dict())
    print(f"Env variables JSON: {envs_json}")
    assert environment_variables_decoder(json.loads(envs_json)).to_string() == envs.to_string()


def test_head_node_spec():

    env_v = EnvVarFrom(source=EnvVarSource.SECRET, name="my-secret", key="key")
    env_s = EnvironmentVariables(key_value={"key": "val"}, from_ref={"key_ref": env_v})
    volumes = [
        PVCVolume(
            name="pvc",
            mount_path="tmp/pvc",
            source="claim",
            read_only=True,
            mount_propagation=MountPropagationMode.BIDIRECTIONAL,
        ),
        EmptyDirVolume(name="emptyDir", mount_path="tmp/emptyDir"),
    ]

    head = HeadNodeSpec(
        compute_template="template",
        image="rayproject/ray:2.46.0-py310",
        ray_start_params=DEFAULT_HEAD_START_PARAMS,
        enable_ingress=True,
        service_type=ServiceType.ClusterIP,
        volumes=volumes,
        environment=env_s,
        image_pull_policy="Always",
    )
    print(f"\nhead node: {head.to_string()}")
    head_json = json.dumps(head.to_dict())
    print(f"head node JSON: {head_json}")
    assert head_node_spec_decoder(json.loads(head_json)).to_string() == head.to_string()


def test_worker_node_spec():

    env_v = EnvVarFrom(source=EnvVarSource.SECRET, name="my-secret", key="key")
    env_s = EnvironmentVariables(key_value={"key": "val"}, from_ref={"key_ref": env_v})
    volumes = [
        PVCVolume(
            name="pvc",
            mount_path="tmp/pvc",
            source="claim",
            read_only=True,
            mount_propagation=MountPropagationMode.BIDIRECTIONAL,
        ),
        EmptyDirVolume(name="emptyDir", mount_path="tmp/emptyDir"),
    ]

    worker = WorkerNodeSpec(
        group_name="group",
        compute_template="template",
        image="rayproject/ray:2.46.0-py310",
        replicas=2,
        min_replicas=2,
        max_replicas=2,
        volumes=volumes,
        ray_start_params=DEFAULT_WORKER_START_PARAMS,
        environment=env_s,
        labels={"key": "value"},
        image_pull_policy="IfNotPresent",
    )
    print(f"\nworker node: {worker.to_string()}")
    worker_json = json.dumps(worker.to_dict())
    print(f"worker node JSON: {worker_json}")
    assert worker_node_spec_decoder(json.loads(worker_json)).to_string() == worker.to_string()


def test_autoscaler_options():
    options = AutoscalerOptions()
    print(f"\nautoscaler options: {options.to_string()}")
    options_json = json.dumps(options.to_dict())
    print(f"autoscaler options JSON: {options_json}")
    assert autoscaling_decoder(json.loads(options_json)).to_string() == options.to_string()

    options = AutoscalerOptions(cpus="1.0", memory="64GB")
    print(f"\nautoscaler options: {options.to_string()}")
    options_json = json.dumps(options.to_dict())
    print(f"autoscaler options JSON: {options_json}")
    assert autoscaling_decoder(json.loads(options_json)).to_string() == options.to_string()


def test_cluster_spec():
    env_s = EnvironmentVariables(
        key_value={"key": "val"},
        from_ref={"key_ref": EnvVarFrom(source=EnvVarSource.SECRET, name="my-secret", key="key")},
    )
    volumes = [
        PVCVolume(
            name="pvc",
            mount_path="tmp/pvc",
            source="claim",
            read_only=True,
            mount_propagation=MountPropagationMode.BIDIRECTIONAL,
        ),
        EmptyDirVolume(name="emptyDir", mount_path="tmp/emptyDir"),
    ]
    spec = ClusterSpec(
        head_node=HeadNodeSpec(
            compute_template="template",
            image="rayproject/ray:2.46.0-py310",
            ray_start_params=DEFAULT_HEAD_START_PARAMS,
            volumes=volumes,
            enable_ingress=True,
            service_type=ServiceType.ClusterIP,
            environment=env_s,
        ),
        worker_groups=[
            WorkerNodeSpec(
                group_name="group",
                compute_template="template",
                replicas=2,
                min_replicas=2,
                max_replicas=2,
                image="rayproject/ray:2.46.0-py310",
                ray_start_params=DEFAULT_WORKER_START_PARAMS,
                volumes=volumes,
                environment=env_s,
                labels={"key": "value"},
            ),
            WorkerNodeSpec(
                group_name="group1",
                compute_template="template1",
                replicas=2,
                min_replicas=2,
                max_replicas=2,
                image="rayproject/ray:2.46.0-py310",
                ray_start_params=DEFAULT_WORKER_START_PARAMS,
                volumes=volumes,
                environment=env_s,
                labels={"key": "value"},
            ),
        ],
        autoscaling_options=AutoscalerOptions(),
    )
    print(f"\ncluster spec: {spec.to_string()}")
    spec_json = json.dumps(spec.to_dict())
    print(f"cluster spec JSON: {spec_json}")
    assert cluster_spec_decoder(json.loads(spec_json)).to_string() == spec.to_string()


def test_cluster():

    event = {
        "id": "id",
        "name": "name",
        "created_at": "ts",
        "first_timestamp": "ts",
        "last_timestamp": "ts",
        "reason": "reason",
        "message": "message",
        "type": "warning",
        "count": "1",
    }
    print(f"\ncluster event: {ClusterEvent(event).to_string()}")
    env_s = EnvironmentVariables(
        key_value={"key": "val"},
        from_ref={"key_ref": EnvVarFrom(source=EnvVarSource.SECRET, name="my-secret", key="key")},
    )
    volumes = [
        PVCVolume(
            name="pvc",
            mount_path="tmp/pvc",
            source="claim",
            read_only=True,
            mount_propagation=MountPropagationMode.BIDIRECTIONAL,
        ),
        EmptyDirVolume(name="emptyDir", mount_path="tmp/emptyDir"),
    ]
    spec = ClusterSpec(
        head_node=HeadNodeSpec(
            compute_template="template",
            ray_start_params=DEFAULT_HEAD_START_PARAMS,
            enable_ingress=True,
            service_type=ServiceType.ClusterIP,
            volumes=volumes,
            environment=env_s,
            annotations={"a_key": "a_val"},
            image="rayproject/ray:2.46.0-py310",
        ),
        worker_groups=[
            WorkerNodeSpec(
                group_name="group",
                compute_template="template",
                replicas=2,
                min_replicas=2,
                max_replicas=2,
                image="rayproject/ray:2.46.0-py310",
                ray_start_params=DEFAULT_WORKER_START_PARAMS,
                volumes=volumes,
                environment=env_s,
                labels={"key": "value"},
            ),
            WorkerNodeSpec(
                group_name="group1",
                compute_template="template1",
                replicas=2,
                min_replicas=2,
                max_replicas=2,
                image="rayproject/ray:2.46.0-py310",
                ray_start_params=DEFAULT_WORKER_START_PARAMS,
                volumes=volumes,
                environment=env_s,
                labels={"key": "value"},
            ),
        ],
    )
    cluster = Cluster(
        name="test",
        namespace="default",
        user="kuberay",
        version="2.46.0",
        cluster_spec=spec,
        deployment_environment=Environment.DEV,
        cluster_environment=env_s,
    )
    print(f"cluster: {cluster.to_string()}")
    cluster_json = json.dumps(cluster.to_dict())
    print(f"cluster JSON: {cluster_json}")
    assert cluster_decoder(json.loads(cluster_json)).to_string() == cluster.to_string()

    cluster_dict = cluster.to_dict()
    cluster_dict["created_at"] = "created"
    cluster_dict["created_status"] = "status"
    cluster_dict["events"] = [event]
    print(f"cluster with output: {cluster_decoder(cluster_dict).to_string()}")


def test_submission():
    yaml = """
    pip:
      - requests==2.26.0
      - pendulum==2.1.2
    env_vars:
      counter_name: test_counter
    """
    request = RayJobRequest(entrypoint="python /home/ray/samples/sample_code.py", runtime_env=yaml, num_cpu=0.5)
    print(f"job request: {request.to_string()}")
    request_json = json.dumps(request.to_dict())
    print(f"request JSON: {request_json}")

    info_json = """
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
    """
    job_info = RayJobInfo(json.loads(info_json))
    print(job_info.to_string())
