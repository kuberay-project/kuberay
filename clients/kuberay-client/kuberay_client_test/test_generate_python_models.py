import os
import sys
from pathlib import Path

import yaml

CLIENT_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = CLIENT_DIR.parent.parent
sys.path.insert(0, str(CLIENT_DIR / "scripts"))

from generate_python_models import (
    property_name_to_class_name,
    normalize_schema,
    load_crd_schema,
    generate_model_for_crd,
)


def test_camel_to_pascal():
    assert property_name_to_class_name("headGroupSpec") == "HeadGroupSpec"


def test_already_pascal():
    assert property_name_to_class_name("RayCluster") == "RayCluster"


def test_snake_case():
    assert property_name_to_class_name("worker_group_spec") == "WorkerGroupSpec"


def test_single_word():
    assert property_name_to_class_name("containers") == "Container"


def test_plural_ies():
    assert property_name_to_class_name("policies") == "Policy"


def test_items_suffix_stripped():
    # Array items should use singular of parent
    assert property_name_to_class_name("envFrom") == "EnvFrom"


def test_inline_object_extracted():
    schema = {
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "headGroupSpec": {
                        "type": "object",
                        "properties": {
                            "rayStartParams": {
                                "type": "object",
                                "additionalProperties": {"type": "string"}
                            }
                        }
                    }
                }
            }
        }
    }
    result = normalize_schema(schema)
    # headGroupSpec should be extracted to definitions
    assert "definitions" in result
    assert "HeadGroupSpec" in result["definitions"]
    # spec is also extracted, and references HeadGroupSpec
    assert "Spec" in result["definitions"]
    assert result["definitions"]["Spec"]["properties"]["headGroupSpec"] == {
        "$ref": "#/definitions/HeadGroupSpec"
    }


def test_duplicate_schemas_deduplicated():
    shared = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "value": {"type": "string"}
        }
    }
    schema = {
        "type": "object",
        "properties": {
            "spec": {
                "type": "object",
                "properties": {
                    "env": {
                        "type": "array",
                        "items": dict(shared)
                    },
                    "workerEnv": {
                        "type": "array",
                        "items": dict(shared)
                    }
                }
            }
        }
    }
    result = normalize_schema(schema)
    defs = result["definitions"]
    # spec is extracted, check its properties
    spec_def = result["definitions"]["Spec"]
    env_ref = spec_def["properties"]["env"]["items"]
    worker_ref = spec_def["properties"]["workerEnv"]["items"]
    assert "$ref" in env_ref
    assert "$ref" in worker_ref
    # Both should reference the same definition (deduplication)
    assert env_ref["$ref"] == worker_ref["$ref"]


def test_int_or_string_converted():
    schema = {
        "type": "object",
        "properties": {
            "port": {
                "x-kubernetes-int-or-string": True
            }
        }
    }
    result = normalize_schema(schema)
    port = result["properties"]["port"]
    assert "oneOf" in port or "$ref" in port


def test_array_items_named_from_parent():
    schema = {
        "type": "object",
        "properties": {
            "containers": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "image": {"type": "string"}
                    }
                }
            }
        }
    }
    result = normalize_schema(schema)
    assert "Container" in result["definitions"]
    assert result["properties"]["containers"]["items"] == {
        "$ref": "#/definitions/Container"
    }


def test_name_collision_different_schemas():
    """Two properties named 'config' with different schemas get distinct class names."""
    schema = {
        "type": "object",
        "properties": {
            "head": {
                "type": "object",
                "properties": {
                    "config": {
                        "type": "object",
                        "properties": {"replicas": {"type": "integer"}}
                    }
                }
            },
            "worker": {
                "type": "object",
                "properties": {
                    "config": {
                        "type": "object",
                        "properties": {"minReplicas": {"type": "integer"}, "maxReplicas": {"type": "integer"}}
                    }
                }
            }
        }
    }
    result = normalize_schema(schema)
    defs = result["definitions"]
    # Path-based naming gives HeadConfig and WorkerConfig
    config_names = [k for k in defs if "Config" in k]
    assert len(config_names) == 2
    assert "HeadConfig" in defs
    assert "WorkerConfig" in defs


def test_load_crd_schema(tmp_path):
    crd = {
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "spec": {
            "group": "ray.io",
            "names": {"kind": "RayCluster"},
            "versions": [{
                "name": "v1",
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "apiVersion": {"type": "string"},
                            "kind": {"type": "string"},
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "rayVersion": {"type": "string"}
                                }
                            }
                        }
                    }
                }
            }]
        }
    }
    crd_path = tmp_path / "ray.io_rayclusters.yaml"
    crd_path.write_text(yaml.safe_dump(crd))
    kind, schema = load_crd_schema(str(crd_path))
    assert kind == "RayCluster"
    assert schema["type"] == "object"
    assert "spec" in schema["properties"]


def test_generate_model_produces_stable_names(tmp_path):
    """Generate from a small CRD schema and verify class names are stable."""
    crd = {
        "apiVersion": "apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "spec": {
            "group": "ray.io",
            "names": {"kind": "RayCluster"},
            "versions": [{
                "name": "v1",
                "schema": {
                    "openAPIV3Schema": {
                        "type": "object",
                        "properties": {
                            "apiVersion": {"type": "string"},
                            "kind": {"type": "string"},
                            "spec": {
                                "type": "object",
                                "properties": {
                                    "rayVersion": {"type": "string"},
                                    "headGroupSpec": {
                                        "type": "object",
                                        "properties": {
                                            "rayStartParams": {
                                                "type": "object",
                                                "additionalProperties": {"type": "string"}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }]
        }
    }
    crd_file = tmp_path / "ray.io_rayclusters.yaml"
    with open(crd_file, 'w') as f:
        yaml.dump(crd, f)

    output_file = tmp_path / "raycluster.py"
    generate_model_for_crd(str(crd_file), str(output_file))

    content = output_file.read_text()
    assert "class HeadGroupSpec" in content
    assert "class Spec" in content or "class RayClusterSpec" in content
    # No numbered classes
    import re
    numbered = re.findall(r'class \w+\d+', content)
    assert numbered == [], f"Found numbered classes: {numbered}"


REAL_CRD_DIR = os.path.join(REPO_ROOT, "ray-operator", "config", "crd", "bases")


def test_real_crd_normalization_no_numbered_classes():
    """Normalize real RayCluster CRD and verify no numbered classes would be generated."""
    crd_path = os.path.join(REAL_CRD_DIR, "ray.io_rayclusters.yaml")
    if not os.path.exists(crd_path):
        import pytest
        pytest.skip("Real CRD files not available")

    kind, schema = load_crd_schema(crd_path)
    normalized = normalize_schema(schema)

    # Should have definitions
    assert "definitions" in normalized
    defs = normalized["definitions"]
    assert len(defs) > 50  # RayCluster has ~142 unique schemas

    # No numbered definition names (except legitimate ones)
    import re
    numbered = [name for name in defs
                if re.match(r'^[A-Za-z]+\d+$', name) and not re.match(r'^V\d+$', name)]
    assert len(numbered) <= 5, f"Too many collision-suffixed names: {numbered}"

    # Key names should be present
    assert "HeadGroupSpec" in defs
    assert "AutoscalerOptions" in defs


def test_raycluster_roundtrip():
    """Construct a RayCluster, serialize to dict, deserialize back."""
    crd_path = os.path.join(REAL_CRD_DIR, "ray.io_rayclusters.yaml")
    if not os.path.exists(crd_path):
        import pytest
        pytest.skip("Real CRD files not available")

    from kuberay_client.models import RayCluster

    cluster = RayCluster(
        apiVersion="ray.io/v1",
        kind="RayCluster",
    )
    data = cluster.model_dump(exclude_none=True)
    assert data["apiVersion"] == "ray.io/v1"
    assert data["kind"] == "RayCluster"

    restored = RayCluster.model_validate(data)
    assert restored.apiVersion == "ray.io/v1"


def test_rayjob_roundtrip():
    """Construct a RayJob, serialize to dict, deserialize back."""
    crd_path = os.path.join(REAL_CRD_DIR, "ray.io_rayjobs.yaml")
    if not os.path.exists(crd_path):
        import pytest
        pytest.skip("Real CRD files not available")

    from kuberay_client.models import RayJob

    job = RayJob(
        apiVersion="ray.io/v1",
        kind="RayJob",
    )
    data = job.model_dump(exclude_none=True)
    assert data["apiVersion"] == "ray.io/v1"
    assert data["kind"] == "RayJob"

    restored = RayJob.model_validate(data)
    assert restored.apiVersion == "ray.io/v1"


def test_quantity_pattern_on_string_only():
    """Verify int-or-string with pattern moves pattern to string branch."""
    schema = {
        "type": "object",
        "properties": {
            "resource": {
                "x-kubernetes-int-or-string": True,
                "pattern": "^(\\+|-)?(([0-9]+(\\.[0-9]*)?)|(\\.[0-9]+))(([KMGTPE]i)|[numkMGTPE]|([eE](\\+|-)?(([0-9]+(\\.[0-9]*)?)|(\\.[0-9]+))))?$",
            }
        }
    }
    result = normalize_schema(schema)
    resource = result["properties"]["resource"]
    assert "oneOf" in resource
    int_branch = next(b for b in resource["oneOf"] if b.get("type") == "integer")
    str_branch = next(b for b in resource["oneOf"] if b.get("type") == "string")
    assert "pattern" not in int_branch
    assert "pattern" in str_branch


def test_generation_idempotent(tmp_path):
    """Running the generator twice produces identical output."""
    crd_path = os.path.join(REAL_CRD_DIR, "ray.io_rayclusters.yaml")
    if not os.path.exists(crd_path):
        import pytest
        pytest.skip("Real CRD files not available")

    out1 = tmp_path / "run1.py"
    out2 = tmp_path / "run2.py"

    generate_model_for_crd(crd_path, str(out1))
    generate_model_for_crd(crd_path, str(out2))

    assert out1.read_text() == out2.read_text(), "Generation is not idempotent"
