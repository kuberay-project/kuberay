#!/usr/bin/env python3
"""Generate Pydantic v2 models from KubeRay CRD YAML schemas.

Normalizes inline CRD schemas into named definitions, then runs
datamodel-code-generator to produce stable, human-readable class names.
"""

import json
import hashlib
import re
from typing import Any
from pathlib import Path
import yaml
import subprocess
import sys
import tempfile
import argparse
import glob


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


def _path_to_class_name(path: list[str]) -> str:
    """Generate a class name from a property path.

    Uses minimal context from the path to avoid numbered collisions.
    Examples:
        ['spec', 'autoscalerOptions'] -> AutoscalerOptions
        ['spec', 'template', 'spec', 'containers'] -> Container
        ['spec', 'headGroupSpec', 'template', 'spec', 'containers', 'ports'] -> ContainerPort
        ['spec', 'template', 'spec', 'initContainers', 'ports'] -> InitContainerPort
        ['status', 'head', 'ports'] -> HeadPort
    """
    if not path:
        return "Model"

    final_segment = path[-1]
    final_lower = final_segment.lower()

    # Well-known property names that should get simple, standalone class names
    standalone_names = {
        'autoscaleroptions': 'AutoscalerOptions',
        'containers': 'Container',
    }

    # Check if the final segment is a standalone name
    if final_lower in standalone_names:
        return standalone_names[final_lower]

    # Segments to skip (too generic)
    skip_segments = {'spec', 'status', 'metadata', 'template'}

    # Strong context keywords that provide sufficient differentiation
    strong_context = {'container', 'init', 'ephemeral'}

    # Weaker context keywords (only include if no strong context present)
    weak_context = {'head', 'worker', 'autoscaler', 'service', 'ingress',
                   'volume', 'env', 'resource', 'security', 'affinity',
                   'toleration', 'config', 'secret', 'downward', 'projected'}

    # Build class name from relevant parts of the path
    parts = []
    has_strong_context = False

    # First pass: identify if we have strong context
    for segment in path[:-1]:  # Exclude final segment
        segment_lower = segment.lower()
        if segment_lower in strong_context or any(sc in segment_lower for sc in strong_context):
            has_strong_context = True
            break

    # Second pass: build parts list
    for i, segment in enumerate(path):
        segment_lower = segment.lower()
        is_last = (i == len(path) - 1)

        # Always include the last segment
        if is_last:
            parts.append(segment)
        # Include strong context keywords
        elif segment_lower in strong_context or any(sc in segment_lower for sc in strong_context):
            parts.append(segment)
        # Include weak context only if we don't have strong context
        elif not has_strong_context and (segment_lower in weak_context or any(wc in segment_lower for wc in weak_context)):
            parts.append(segment)
        # Skip generic segments
        elif segment_lower in skip_segments:
            continue

    # Convert each part to PascalCase and combine
    class_parts = [property_name_to_class_name(p) for p in parts]
    return ''.join(class_parts)


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

    schema = _extract_definitions(schema, [], definitions, hash_to_name, is_root=True)
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
    path: list[str],
    definitions: dict[str, dict],
    hash_to_name: dict[str, str],
    is_root: bool = False,
) -> Any:
    if not isinstance(schema, dict):
        return schema

    # Process oneOf/anyOf schemas
    for key in ["oneOf", "anyOf"]:
        if key in schema and isinstance(schema[key], list):
            schema[key] = [
                _extract_definitions(item, path, definitions, hash_to_name)
                if isinstance(item, dict) else item
                for item in schema[key]
            ]

    # Process array items first
    if "items" in schema and isinstance(schema["items"], dict):
        schema["items"] = _extract_definitions(
            schema["items"], path, definitions, hash_to_name
        )

    # Process properties recursively
    if "properties" in schema:
        new_props = {}
        for key, val in schema["properties"].items():
            new_props[key] = _extract_definitions(
                val, path + [key], definitions, hash_to_name
            )
        schema["properties"] = new_props

    # Process additionalProperties
    if "additionalProperties" in schema and isinstance(schema["additionalProperties"], dict):
        # additionalProperties are dict-like, don't add to path
        schema["additionalProperties"] = _extract_definitions(
            schema["additionalProperties"], path, definitions, hash_to_name
        )

    # Extract this schema into definitions if it's an inline object (not root)
    # or if it has oneOf/anyOf with complex schemas
    should_extract = False
    if not is_root and path:
        if schema.get("type") == "object" and "properties" in schema:
            should_extract = True
        # Also extract oneOf/anyOf if they contain objects
        elif "oneOf" in schema or "anyOf" in schema:
            key = "oneOf" if "oneOf" in schema else "anyOf"
            if any(isinstance(item, dict) and item.get("type") == "object" for item in schema[key]):
                should_extract = True

    if should_extract:
        class_name = _path_to_class_name(path)
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


def load_crd_schema(crd_path: str) -> tuple[str, dict]:
    """Load a CRD YAML and return (kind, openAPIV3Schema)."""
    with open(crd_path) as f:
        crd = yaml.safe_load(f)

    kind = crd["spec"]["names"]["kind"]
    versions = crd["spec"]["versions"]
    schema = versions[0]["schema"]["openAPIV3Schema"]
    return kind, schema


def generate_model_for_crd(crd_path: str, output_path: str) -> None:
    """Generate a Pydantic v2 model file from a CRD YAML."""
    kind, schema = load_crd_schema(crd_path)
    normalized = normalize_schema(schema)

    import os
    tmp_dir = tempfile.gettempdir()
    tmp_schema = os.path.join(tmp_dir, f"{kind.lower()}_schema.json")
    with open(tmp_schema, 'w') as f:
        json.dump(normalized, f, indent=2)

    try:
        subprocess.run(
            [
                sys.executable, "-m", "datamodel_code_generator",
                "--input", tmp_schema,
                "--input-file-type", "jsonschema",
                "--output-model-type", "pydantic_v2.BaseModel",
                "--output", output_path,
                "--use-union-operator",
                "--target-python-version", "3.11",
                "--collapse-root-models",
                "--field-constraints",
                "--use-default-kwarg",
                "--formatters", "builtin",
                "--class-name", kind,  # Use CRD kind as root class name
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"datamodel-code-generator failed for {kind}:", file=sys.stderr)
        print(e.stderr, file=sys.stderr)
        raise
    finally:
        Path(tmp_schema).unlink(missing_ok=True)


CRD_DIR = "ray-operator/config/crd/bases"
OUTPUT_DIR = "clients/python-api/kuberay_api/models"
CRD_PATTERN = "ray.io_*.yaml"


def generate_models(crd_dir: str, output_dir: str) -> None:
    """Generate Pydantic models for all CRDs in the directory."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    crd_files = sorted(glob.glob(str(Path(crd_dir) / CRD_PATTERN)))
    if not crd_files:
        print(f"No CRD files matching {CRD_PATTERN} in {crd_dir}", file=sys.stderr)
        sys.exit(1)

    generated = []
    for crd_file in crd_files:
        kind, _ = load_crd_schema(crd_file)
        module_name = kind.lower()  # RayCluster -> raycluster
        out_file = output_path / f"{module_name}.py"
        print(f"Generating {kind} -> {out_file}")
        generate_model_for_crd(crd_file, str(out_file))
        generated.append((kind, module_name))

    # Generate __init__.py that re-exports only top-level CRD types
    init_lines = [
        "# Generated by scripts/generate_python_models.py — do not edit",
        "",
    ]
    for kind, module in generated:
        init_lines.append(f"from .{module} import {kind}  # noqa: F401")
    init_lines.append("")

    (output_path / "__init__.py").write_text("\n".join(init_lines))
    print(f"Generated models for {len(generated)} CRDs in {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate Pydantic v2 models from KubeRay CRD schemas"
    )
    parser.add_argument(
        "--crd-dir", default=CRD_DIR,
        help=f"Directory containing CRD YAML files (default: {CRD_DIR})"
    )
    parser.add_argument(
        "--output-dir", default=OUTPUT_DIR,
        help=f"Output directory for generated models (default: {OUTPUT_DIR})"
    )
    args = parser.parse_args()
    generate_models(args.crd_dir, args.output_dir)


if __name__ == "__main__":
    main()
