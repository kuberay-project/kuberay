#!/usr/bin/env python3
"""Generate Pydantic v2 models from KubeRay CRD YAML schemas.

Normalizes inline CRD schemas into named definitions, then runs
datamodel-code-generator to produce stable, human-readable class names.
"""

import json
import hashlib
import re
from typing import Any


# Names that should not be singularized
_NO_SINGULARIZE = {
    "options", "Options", "status", "Status", "address", "Address",
    "class", "Class", "alias", "Alias", "bus", "Bus",
    "atlas", "Atlas", "kubernetes", "Kubernetes",
}


def property_name_to_class_name(name: str) -> str:
    """Convert a JSON property name to a PascalCase class name.

    headGroupSpec -> HeadGroupSpec
    worker_group_spec -> WorkerGroupSpec
    containers -> Container
    """
    # Handle snake_case
    if '_' in name:
        parts = name.split('_')
        name = ''.join(p.capitalize() for p in parts)
    else:
        # Handle camelCase — capitalize first letter
        name = name[0].upper() + name[1:]

    # Singularize common plural suffixes, unless in _NO_SINGULARIZE
    if name not in _NO_SINGULARIZE:
        if name.endswith('ies') and len(name) > 4:
            name = name[:-3] + 'y'
        elif name.endswith('ses'):
            name = name[:-2]
        elif name.endswith('s') and not name.endswith('ss') and not name.endswith('us') and len(name) > 3:
            name = name[:-1]

    return name


def schema_hash(schema: dict) -> str:
    canonical = json.dumps(schema, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def normalize_schema(schema: dict) -> dict:
    """Normalize a CRD openAPIV3Schema into JSON Schema with named definitions.

    1. Convert x-kubernetes-int-or-string to oneOf
    2. Strip x-kubernetes-* extensions
    3. Extract inline objects into definitions with $ref
    4. Deduplicate identical schemas
    """
    schema = _convert_int_or_string(schema)
    schema = _strip_k8s_extensions(schema)

    definitions: dict[str, dict] = {}
    hash_to_name: dict[str, str] = {}

    schema = _extract_definitions(schema, "", definitions, hash_to_name, is_root=True)
    if definitions:
        schema["definitions"] = definitions
    return schema


def _convert_int_or_string(obj: Any) -> Any:
    if isinstance(obj, dict):
        if obj.get("x-kubernetes-int-or-string"):
            new = {k: v for k, v in obj.items() if k != "x-kubernetes-int-or-string" and k != "type" and k != "anyOf"}
            new["oneOf"] = [{"type": "integer"}, {"type": "string"}]
            return {k: _convert_int_or_string(v) if k != "oneOf" else v for k, v in new.items()}
        return {k: _convert_int_or_string(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_int_or_string(i) for i in obj]
    return obj


def _strip_k8s_extensions(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _strip_k8s_extensions(v) for k, v in obj.items()
                if not k.startswith("x-kubernetes-")}
    elif isinstance(obj, list):
        return [_strip_k8s_extensions(i) for i in obj]
    return obj


def _extract_definitions(
    schema: Any,
    prop_name: str,
    definitions: dict[str, dict],
    hash_to_name: dict[str, str],
    is_root: bool = False,
) -> Any:
    if not isinstance(schema, dict):
        return schema

    # Process array items first
    if "items" in schema and isinstance(schema["items"], dict):
        schema["items"] = _extract_definitions(
            schema["items"], prop_name, definitions, hash_to_name
        )

    # Process properties recursively
    if "properties" in schema:
        new_props = {}
        for key, val in schema["properties"].items():
            new_props[key] = _extract_definitions(
                val, key, definitions, hash_to_name
            )
        schema["properties"] = new_props

    # Process additionalProperties
    if "additionalProperties" in schema and isinstance(schema["additionalProperties"], dict):
        schema["additionalProperties"] = _extract_definitions(
            schema["additionalProperties"], prop_name, definitions, hash_to_name
        )

    # Extract this schema into definitions if it's an inline object (not root)
    if (
        not is_root
        and schema.get("type") == "object"
        and "properties" in schema
        and prop_name
    ):
        class_name = property_name_to_class_name(prop_name)
        h = schema_hash(schema)

        if h in hash_to_name:
            # Deduplicate — reuse existing definition
            return {"$ref": f"#/definitions/{hash_to_name[h]}"}

        # Handle name collisions (different schemas, same derived name)
        final_name = class_name
        counter = 1
        while final_name in definitions and schema_hash(definitions[final_name]) != h:
            counter += 1
            final_name = f"{class_name}{counter}"

        definitions[final_name] = schema
        hash_to_name[h] = final_name
        return {"$ref": f"#/definitions/{final_name}"}

    return schema
