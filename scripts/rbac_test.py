"""
Test `crNamespacedRbacEnable`, `singleNamespaceInstall`, and `watchNamespace` of the Helm chart.
"""
from pathlib import Path
import subprocess
import tempfile
import yaml
import jsonpatch


REPO_ROOT = Path(__file__).absolute().parent.parent


def generate_config_patch(config):
    """
    Set `crNamespacedRbacEnable`, `singleNamespaceInstall`, and `watchNamespace` of the Helm chart.
    """
    actions = [
        {
            "op": "replace",
            "path": "/crNamespacedRbacEnable",
            "value": config["crNamespacedRbacEnable"],
        },
        {
            "op": "replace",
            "path": "/singleNamespaceInstall",
            "value": config["singleNamespaceInstall"],
        },
    ]
    if "watchNamespace" in config:
        actions.append(
            {
                "op": "add",
                "path": "/watchNamespace",
                "value": config["watchNamespace"],
            },
        )
    return jsonpatch.JsonPatch(actions)


def helm_template_render(values_yaml):
    """
    Render the Helm chart with the given values.yaml.
    """
    chart_path = REPO_ROOT.joinpath("helm-chart/kuberay-operator/")
    with tempfile.NamedTemporaryFile("w", suffix="_values.yaml") as values_fd:
        yaml.safe_dump(values_yaml, values_fd)
        render_string = subprocess.run(
            f"helm template kuberay-operator {chart_path} -f {values_fd.name}",
            shell=True,
            check=True,
            capture_output=True,
        ).stdout
        return yaml.safe_load_all(render_string)


class TestRbac:
    base_values_yaml_path = REPO_ROOT.joinpath(
        "helm-chart/kuberay-operator/values.yaml"
    )
    with open(base_values_yaml_path, encoding="utf-8") as fd:
        base_values_yaml = yaml.safe_load(fd)

    def test_rbac_1(self):
        """
        When singleNamespaceInstall is set to True, no cluster-scoped
        RBAC resources should be created.
        """
        patch = generate_config_patch(
            {
                "singleNamespaceInstall": True,
                "crNamespacedRbacEnable": False,
                "watchNamespace": ["n1"],
            }
        )
        render_output = helm_template_render(patch.apply(self.base_values_yaml))
        for k8s_object in render_output:
            if k8s_object is None:
                continue
            assert (
                k8s_object["kind"] != "ClusterRole"
                and k8s_object["kind"] != "ClusterRoleBinding"
            )
