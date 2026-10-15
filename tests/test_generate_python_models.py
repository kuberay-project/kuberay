import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from generate_python_models import property_name_to_class_name, normalize_schema


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
    # Both should exist with distinct names
    config_names = [k for k in defs if k.startswith("Config")]
    assert len(config_names) == 2
