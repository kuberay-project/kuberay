import enum
from typing import Any


class TolerationOperation(enum.Enum):
    """
    Toleration operation types
    """

    Exists = "Exists"  # exists
    Equal = "Equal"  # equal


class TolerationEffect(enum.Enum):
    """
    Toleration effect
    """

    NoSchedule = "NoSchedule"  # not schedule
    PreferNoSchedule = "PreferNoSchedule"  # prefer not schedule
    NoExecute = "NoExecute"  # not execute


class Toleration:
    """
    Toleration is used by compute template to pick specific nodes for placing pods.
    It provides APIs to create, stringify and convert to dict.

    Methods:
    - Create toleration: gets the following parameters:
        key - required, key created by the node's taint
        operator - required, operator to apply, supported operators are "Exists" and "Equal"
        effect - required, toleration effect supported effects are "NoSchedule", "PreferNoSchedule", "NoExecute"
        value - optional, value
    - to_string() -> str: convert toleration to string for printing
    - to_dict() -> dict[str, Any] convert to dict
    """

    def __init__(self, key: str, operator: TolerationOperation, effect: TolerationEffect, value: str = None):
        """
        Initialization
        :param key: key
        :param operator: operator
        :param effect: effect
        :param value: value
        """
        self.key = key
        self.operator = operator
        self.value = value
        self.effect = effect

    def to_string(self) -> str:
        """
        Convert to string
        :return: string representation of toleration
        """
        val = f"key = {self.key}, operator = {self.operator.name}, effect = {self.effect.name}"
        if self.value is None:
            return val
        else:
            return val + f", value = {self.value}"

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to string
        :return: string representation of toleration
        """
        dct = {"key": self.key, "operator": self.operator.value, "effect": self.effect.value}
        if self.value is not None:
            dct["value"] = self.value
        return dct


# Here the default gpu-accelerator is "nvidia.com/gpu", that is used for generating limits.
# If it is specified, it has to be in the format that is understood by kubernetes as a valid
# The following devices are currently supported by kubernetes:
# AMD - gpu accelerator amd.com/gpu
# Intel - gpu accelerator gpu.intel.com/i915
# NVIDIA - gpu accelerator nvidia.com/gpu


class Template:
    """
    Template is used to define specific nodes configuration.
    It provides APIs to create, stringify and convert to dict.

    Methods:
    - Create templates: gets the following parameters:
        name - required, template name
        namespace - required, template namespace
        cpus - required, template number of cpus
        memory - required, template memory (GB)
        gpus - optional, number of GPUs, default 0
        gpu_accelerator - optional, if not defined nvidia.com/gpu is assumed
        extended_resources - optional, name and number of the extended resources
        tolerations - optional, tolerations for pod placing, default none
    - to_string() -> str: convert toleration to string for printing
    - to_dict() -> dict[str, Any] convert to dict
    - to_json() -> str convert to json string
    """

    def __init__(
            self,
            name: str,
            namespace: str,
            cpu: int,
            memory: int,
            gpu: int = 0,
            gpu_accelerator: str = None,
            extended_resources: dict[str, int] = None,
            tolerations: list[Toleration] = None,
    ):
        """
        Initialization
        :param name: name
        :param namespace: namespace
        :param cpu: cpu
        :param memory: memory
        :param gpu: gpu
        :param gpu_accelerator: accelerator type
        :param extended_resources: extended resources
        :param tolerations: tolerations
        """
        self.name = name
        self.namespace = namespace
        self.cpu = cpu
        self.memory = memory
        self.gpu = gpu
        self.gpu_accelerator = gpu_accelerator
        self.extended_resources = extended_resources
        self.tolerations = tolerations

    def to_string(self) -> str:
        """
        Convert to string
        :return: string representation of template
        """
        val = f"name = {self.name}, namespace = {self.namespace}, cpu = {self.cpu}, memory = {self.memory}"
        if self.gpu > 0:
            val = val + f", gpu {self.gpu}"
        if self.gpu_accelerator is not None:
            val = val + f", gpu accelerator {self.gpu_accelerator}"
        if self.extended_resources is not None:
            val = val + f", extended resources {self.extended_resources}"
        if self.tolerations is None:
            return val
        val = val + ", tolerations ["
        first = True
        for tol in self.tolerations:
            if first:
                first = False
                val = val + "{" + tol.to_string() + "}"
            else:
                val = val + ", {" + tol.to_string() + "}"
        return val + "]"

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary
        :return: dictionary representation of template
        """
        dct = {"name": self.name, "namespace": self.namespace, "cpu": self.cpu, "memory": self.memory}
        if self.gpu > 0:
            dct["gpu"] = self.gpu
        if self.gpu_accelerator is not None:
            dct["gpu accelerator"] = self.gpu_accelerator
        if self.extended_resources is not None:
            dct["extended resources"] = self.extended_resources
        if self.tolerations is not None:
            dct["tolerations"] = [tl.to_dict() for tl in self.tolerations]
        return dct


"""
    Creates new toleration from dictionary, used for unmarshalling json. Python does not
    support multiple constructors, so do it this way
"""


def toleration_decoder(dct: dict[str, Any]) -> Toleration:
    """
    Create toleration from dictionary
    :param dct: dictionary representation of toleration
    :return: toleration
    """
    return Toleration(
        key=dct.get("key"),
        operator=TolerationOperation(dct.get("operator", "Exists")),
        effect=TolerationEffect(dct.get("effect", "NoSchedule")),
        value=dct.get("value"),
    )


def template_decoder(dct: dict[str, Any]) -> Template:
    """
    Create template from dictionary
    :param dct: dictionary representation of template
    :return: template
    """
    tolerations = None
    if "tolerations" in dct:
        tolerations = [toleration_decoder(d) for d in dct["tolerations"]]
    return Template(
        name=dct.get("name"),
        namespace=dct.get("namespace"),
        cpu=int(dct.get("cpu", "0")),
        memory=int(dct.get("memory", "0")),
        gpu=int(dct.get("gpu", "0")),
        gpu_accelerator=dct.get("gpu_accelerator"),
        extended_resources=dct.get("extended_resources"),
        tolerations=tolerations,
    )


def templates_decoder(dct: dict[str, Any]) -> list[Template]:
    """
    Create list of template from dictionary
    :param dct: dictionary representation of list of template
    :return: list of template
    """
    return [template_decoder(tmp) for tmp in dct["computeTemplates"]]
