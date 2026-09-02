#!/usr/bin/env python3
"""
Generate Pydantic models from KubeRay CRD OpenAPI schemas.

This script extracts the v1 OpenAPI schema from each CRD YAML file and generates
typed Pydantic models using datamodel-code-generator, one module per custom
resource.

Each CRD is generated into its own module on purpose. CRD YAML inlines every
schema (no $refs), so the generator mints a class per occurrence and names the
anonymous ones positionally (Spec2, Port2, ...). Generating all CRDs into one
module numbers those classes across all of them, which means a RayService schema
change can rename the classes the RayJob builders import. Separate modules keep
each CR's numbering scoped to its own schema.

Usage:
    python scripts/generate_models.py

Requirements:
    pip install -r scripts/requirements.txt
"""

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, NamedTuple

# Paths relative to this script
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parent.parent.parent
CRD_DIR = REPO_ROOT / "ray-operator" / "config" / "crd" / "bases"
OUTPUT_DIR = SCRIPT_DIR.parent / "python_client" / "models" / "generated"
REQUIREMENTS_PATH = SCRIPT_DIR / "requirements.txt"
CODEGEN_PACKAGE = "datamodel-code-generator"
CRD_VERSION = "v1"


class CustomResource(NamedTuple):
    """A custom resource to generate models for."""

    crd_file: str
    kind: str
    module: str


CUSTOM_RESOURCES = [
    CustomResource("ray.io_rayclusters.yaml", "RayCluster", "raycluster.py"),
    CustomResource("ray.io_rayjobs.yaml", "RayJob", "rayjob.py"),
    CustomResource("ray.io_rayservices.yaml", "RayService", "rayservice.py"),
    CustomResource("ray.io_raycronjobs.yaml", "RayCronJob", "raycronjob.py"),
]


def extract_schema(crd_path: Path) -> dict:
    """Extract the CRD_VERSION OpenAPI schema from a CRD YAML."""
    try:
        import yaml
    except ImportError:
        print(f"ERROR: pyyaml is required. Install with: pip install -r {REQUIREMENTS_PATH}")
        sys.exit(1)

    with open(crd_path) as f:
        crd = yaml.safe_load(f)

    versions = crd["spec"]["versions"]
    version = next((v for v in versions if v["name"] == CRD_VERSION), None)
    if version is None:
        available = [v["name"] for v in versions]
        print(f"ERROR: {crd_path.name} has no {CRD_VERSION} version (found {available})")
        sys.exit(1)

    schema = version["schema"]["openAPIV3Schema"]
    scope_int_or_string_patterns(schema)
    return schema


def scope_int_or_string_patterns(node: Any) -> None:
    """Move a Kubernetes int-or-string 'pattern' onto the string branch only.

    Quantity fields (resource requests/limits, divisors, ...) are serialized as
    {"anyOf": [{"type": "integer"}, {"type": "string"}], "pattern": ...}. The
    pattern is a sibling of anyOf, so the generator applies it to both branches
    and emits RootModel[int] with a pattern constraint - which pydantic rejects
    at validation time with "Unable to apply constraint 'pattern' ... for schema
    of type 'int'". A pattern only ever constrains the string form, so attach it
    there.
    """
    if isinstance(node, dict):
        if node.get("x-kubernetes-int-or-string") and "pattern" in node:
            pattern = node.pop("pattern")
            for branch in node.get("anyOf", []):
                if branch.get("type") == "string":
                    branch["pattern"] = pattern

        for value in node.values():
            scope_int_or_string_patterns(value)
    elif isinstance(node, list):
        for value in node:
            scope_int_or_string_patterns(value)


def pinned_codegen_version() -> str:
    """Read the datamodel-code-generator version pinned in scripts/requirements.txt."""
    for line in REQUIREMENTS_PATH.read_text().splitlines():
        match = re.match(rf"^{re.escape(CODEGEN_PACKAGE)}==(\S+)$", line.strip())
        if match:
            return match.group(1)

    print(f"ERROR: no '{CODEGEN_PACKAGE}==<version>' pin found in {REQUIREMENTS_PATH}")
    sys.exit(1)


def check_codegen_version(expected: str) -> None:
    """Ensure the installed datamodel-codegen matches the pinned version.

    Different versions generate different class names, so an unpinned generator
    silently produces files that no longer match the committed ones.
    """
    try:
        result = subprocess.run(
            ["datamodel-codegen", "--version"],
            capture_output=True,
            check=True,
            text=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        print(f"ERROR: {CODEGEN_PACKAGE} is required.")
        print(f"Install with: pip install -r {REQUIREMENTS_PATH}")
        sys.exit(1)

    # Output looks like "datamodel-codegen 0.76.0"
    installed = result.stdout.strip().split()[-1]
    if installed != expected:
        print(f"ERROR: datamodel-codegen {installed} is installed, but {expected} is pinned.")
        print(f"Install the pinned version with: pip install -r {REQUIREMENTS_PATH}")
        sys.exit(1)


def generate_models(
    schema: dict,
    output_path: Path,
    crd_path: Path,
    kind: str,
    codegen_version: str,
) -> None:
    """Generate Pydantic models from schema using datamodel-codegen."""

    result = subprocess.run(
        [
            "datamodel-codegen",
            "--input-file-type", "jsonschema",
            "--output", str(output_path),
            "--output-model-type", "pydantic_v2.BaseModel",
            "--use-standard-collections",
            "--use-union-operator",
            "--field-constraints",
            "--reuse-model",
            "--collapse-reuse-models",
            "--class-name", kind,
        ],
        input=json.dumps(schema),
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"ERROR: datamodel-codegen failed for {kind}:\n{result.stderr}")
        sys.exit(1)

    # Read generated content and add proper header
    with open(output_path) as f:
        content = f.read()

    # Remove the default header (first 3 lines)
    lines = content.split("\n")
    if lines[0].startswith("# generated by datamodel-codegen"):
        lines = lines[3:]  # Skip the 3 header lines
        content = "\n".join(lines)

    # Create proper header (no timestamp to avoid CI churn)
    crd_relative = crd_path.relative_to(REPO_ROOT)

    header = f'''"""
Auto-generated Pydantic models for the {kind} CRD OpenAPI schema.

DO NOT EDIT THIS FILE MANUALLY!

Generated by: clients/python-client/scripts/generate_models.py
Source CRD:   {crd_relative} ({CRD_VERSION})
Generator:    {CODEGEN_PACKAGE} {codegen_version}

To regenerate (from repo root):
    pip install -r clients/python-client/scripts/requirements.txt
    python clients/python-client/scripts/generate_models.py
"""

'''

    # Write with new header
    with open(output_path, "w") as f:
        f.write(header + content)


def main():
    codegen_version = pinned_codegen_version()
    check_codegen_version(codegen_version)

    missing = [cr.crd_file for cr in CUSTOM_RESOURCES if not (CRD_DIR / cr.crd_file).exists()]
    if missing:
        print(f"ERROR: CRD files not found in {CRD_DIR}: {missing}")
        print("Make sure you're running from the correct directory.")
        sys.exit(1)

    print(f"Generating Pydantic models with {CODEGEN_PACKAGE} {codegen_version}...")
    for cr in CUSTOM_RESOURCES:
        crd_path = CRD_DIR / cr.crd_file
        output_path = OUTPUT_DIR / cr.module

        schema = extract_schema(crd_path)
        generate_models(schema, output_path, crd_path, cr.kind, codegen_version)

        print(f"  {cr.kind:11} {cr.crd_file:24} -> models/generated/{cr.module}")

    print("Done!")


if __name__ == "__main__":
    main()
