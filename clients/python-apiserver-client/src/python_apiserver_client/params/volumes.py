import enum
from typing import Any


class HostPath(enum.Enum):
    """
    Host path enumeration
    """

    DIRECTORY = 0  # directory
    FILE = 1  # files


class MountPropagationMode(enum.Enum):
    """
    Mount propagation enumeration
    """

    NONE = 0  # None
    HOSTTOCONTAINER = 1  # host to container
    BIDIRECTIONAL = 2  # bi directional


class AccessMode(enum.Enum):
    """
    Access mode enumeration
    """

    RWO = 0  # read write once
    ROX = 1  # read only many
    RWX = 2  # read write many


class BaseVolume:
    """
    KubeRay currently support several types of volumes, including hostPat, PVC,
    ephemeral volumes, config maps, secrets and empty dir. All of them use slightly
    different parameters. Base Volume is a base class for all different volume types.
    """

    def to_string(self) -> str:
        """
        Convert to string
        :return: string representation of base volume
        """
        raise Exception(f"Base volume cannot be used directly. Pls use one of the derived classes")

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary
        :return: dictionary representation of base volume
        """
        raise Exception(f"Base volume cannot be used directly. Pls use one of the derived classes")


class HostPathVolume(BaseVolume):
    """
    This class implements HostPath volume. In addition to name and mount path it requires host
    path volume specific parameters:
        source - data location on host
        hostPathType - host path type: directory (0) or file (1)
        mountPropagationMode - mount propagation: None (0), host to container (1) or bidirectional (2)

    """

    def __init__(
            self,
            name: str,
            mount_path: str,
            source: str,
            host_path_type: HostPath = None,
            mount_propagation: MountPropagationMode = None,
    ):
        """
        Initialization
        :param name: name
        :param mount_path: mount path
        :param source: source
        :param host_path_type: host path type
        :param mount_propagation: mount propagation
        """
        self.name = name
        self.mount_path = mount_path
        self.source = source
        self.host_path_type = host_path_type
        self.volume_type = 1
        self.mount_propagation = mount_propagation

    def to_string(self) -> str:
        """
        Convert to string
        :return: HostPathVolume string representation
        """
        val = f"name = {self.name}, mount_path = {self.mount_path}, source = {self.source}, " f"volume type = hostPath"
        if self.mount_propagation is not None:
            val += f", mount propagation = {self.mount_propagation.name}"
        if self.host_path_type is not None:
            val += f", host path type = {self.host_path_type.name}"
        return val

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary
        :return: HostPathVolume dictionary representation
        """
        dst = {"name": self.name, "mountPath": self.mount_path, "source": self.source, "volumeType": self.volume_type}
        if self.mount_propagation is not None:
            dst["mountPropagationMode"] = self.mount_propagation.value
        if self.host_path_type is not None:
            dst["hostPathType"] = self.host_path_type.value
        return dst


class PVCVolume(BaseVolume):
    """
    This class implements PVC volume. In addition to name and mount path it requires
    PVC volume specific parameters:
       source - PVC claim name
       read_only - read only flag
       mountPropagationMode - mount propagation: None (0), host to container (1) or bidirectional (2)
    """

    def __init__(
            self,
            name: str,
            mount_path: str,
            source: str,
            read_only: bool = False,
            mount_propagation: MountPropagationMode = None,
    ):
        """
        Initialization
        :param name: name
        :param mount_path: mount path
        :param source: source
        :param read_only: read only
        :param mount_propagation: mount propagation
        """
        self.name = name
        self.mount_path = mount_path
        self.source = source
        self.volume_type = 0
        self.mount_propagation = mount_propagation
        self.readonly = read_only

    def to_string(self) -> str:
        """
        Convert to string
        :return: PVCVolume string representation
        """
        val = f"name = {self.name}, mount_path = {self.mount_path}, source = {self.source}, " f"volume type = PVC"
        if self.readonly:
            val += ", read only = True"
        if self.mount_propagation is not None:
            val += f", mount propagation = {self.mount_propagation.name}"
        return val

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary
        :return: PVCVolume dictionary representation
        """
        dst = {"name": self.name, "mountPath": self.mount_path, "source": self.source, "volumeType": self.volume_type}
        if self.readonly:
            dst["readOnly"] = True
        if self.mount_propagation is not None:
            dst["mountPropagationMode"] = self.mount_propagation.value
        return dst


class EphemeralVolume(BaseVolume):
    """
    This class implements Ephemeral volume. In addition to name and mount path it requires
    Ephemeral volume specific parameters:
        storage - disk size (valid k8 value, for example 5Gi)
        storageClass - storage class - optional, if not specified, use default
        accessMode - access mode RWO - optional ReadWriteOnce (0), ReadOnlyMAny (1), ReadWriteMany (2)
        mountPropagationMode - optional mount propagation: None (0), host to container (1) or bidirectional (2)
    """

    def __init__(
            self,
            name: str,
            mount_path: str,
            storage: str,
            storage_class: str = None,
            access_mode: AccessMode = None,
            mount_propagation: MountPropagationMode = None,
    ):
        """
        Initialization
        :param name: name
        :param mount_path: mount path
        :param storage: storage
        :param storage_class: storage class
        :param access_mode: access mode
        :param mount_propagation: mount propagation
        """
        self.name = name
        self.mount_path = mount_path
        self.storage = storage
        self.volume_type = 2
        self.mount_propagation = mount_propagation
        self.storage_class = storage_class
        self.access_mode = access_mode

    def to_string(self) -> str:
        """
        Convert to string
        :return: EphemeralVolume string representation
        """
        val = (
            f"name = {self.name}, mount_path = {self.mount_path}, storage = {self.storage} " f"volume type = ephemeral"
        )
        if self.storage_class is not None:
            val += f", storage class = {self.storage_class}"
        if self.access_mode is not None:
            val += f", access mode = {self.access_mode.name}"
        if self.mount_propagation is not None:
            val += f", mount propagation = {self.mount_propagation.name}"
        return val

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary
        :return: EphemeralVolume dictionary representation
        """
        dct = {
            "name": self.name,
            "mountPath": self.mount_path,
            "storage": self.storage,
            "volumeType": self.volume_type,
        }
        if self.storage_class is not None:
            dct["storageClassName"] = self.storage_class
        if self.access_mode is not None:
            dct["accessMode"] = self.access_mode.value
        if self.mount_propagation is not None:
            dct["mountPropagationMode"] = self.mount_propagation.value
        return dct


class EmptyDirVolume(BaseVolume):
    """
    This class implements EmptyDir volume. In addition to name and mount path it requires
    Empty Dir specific parameters:
        storage - optional max storage size (valid k8 value, for example 5Gi)
    """

    def __init__(self, name: str, mount_path: str, storage: str = None):
        """
        Initialization
        :param name: name
        :param mount_path: mount_path
        :param storage: storage
        """
        self.name = name
        self.mount_path = mount_path
        self.storage = storage
        self.volume_type = 5

    def to_string(self) -> str:
        """
        Convert to string
        :return: EmptyDirVolume string representation
        """
        val = f"name = {self.name}, mount_path = {self.mount_path}, volume type = emptyDir"
        if self.storage is not None:
            val += f", storage = {self.storage}"
        return val

    def to_dict(self) -> dict[str, Any]:
        dct = {"name": self.name, "mountPath": self.mount_path, "volumeType": self.volume_type}
        if self.storage is not None:
            dct["storage"] = self.storage
        return dct


class ConfigMapVolume(BaseVolume):
    """
    This class implements ConfigMap volume. In addition to name and mount path it requires
    configMap volume specific parameters:
        source - required, config map name
        items - optional, key/path items (optional)
    """

    def __init__(
            self,
            name: str,
            mount_path: str,
            source: str,
            items: dict[str, str] = None,
    ):
        """
        Initialization
        :param name: name
        :param mount_path: mount path
        :param source: source
        :param items: items
        """
        self.name = name
        self.mount_path = mount_path
        self.source = source
        self.items = items
        self.volume_type = 3

    def to_string(self) -> str:
        """
        Convert to string
        :return: ConfigMapVolume string representation
        """
        val = (
            f"name = {self.name}, mount_path = {self.mount_path}, source = {self.source}, " f"volume type = configmap"
        )
        if self.items is not None:
            val = val + f", items = {str(self.items)}"
        return val

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary
        :return: ConfigMapVolume dictionary representation
        """
        dct = {"name": self.name, "mountPath": self.mount_path, "source": self.source, "volumeType": self.volume_type}
        if self.items is not None:
            dct["items"] = self.items
        return dct


class SecretVolume(BaseVolume):
    """
    This class implements Secret volume. In addition to name and mount path it requires
    Secret volume specific parameters:
        source - required, secret name
        items - optional, key/path items (optional)
    """

    def __init__(
            self,
            name: str,
            mount_path: str,
            source: str,
            items: dict[str, str] = None,
    ):
        self.name = name
        self.mount_path = mount_path
        self.source = source
        self.items = items
        self.volume_type = 4

    def to_string(self) -> str:
        val = f"name = {self.name}, mount_path = {self.mount_path}, source = {self.source}, " f"volume type = secret"
        if self.items is not None:
            val = val + f", items = {str(self.items)}"
        return val

    def to_dict(self) -> dict[str, Any]:
        dct = {"name": self.name, "mountPath": self.mount_path, "source": self.source, "volumeType": self.volume_type}
        if self.items is not None:
            dct["items"] = self.items
        return dct


"""
    Creates new Volume from dictionary, used for unmarshalling json. Python does not
    support multiple constructors, so do it this way
"""


def volume_decoder(dst: dict[str, Any]) -> BaseVolume:
    def _get_mount_propagation() -> MountPropagationMode:
        if "mountPropagationMode" in dst:
            return MountPropagationMode(int(dst.get("mountPropagationMode", "0")))
        return None

    def _get_host_path() -> HostPath:
        if "hostPathType" in dst:
            return HostPath(int(dst.get("hostPathType", "0")))
        return None

    def _get_access_mode() -> AccessMode:
        if "accessMode" in dst:
            return AccessMode(int(dst.get("accessMode", "0")))
        return None

    match dst["volumeType"]:
        case 0:
            # PVC
            return PVCVolume(
                name=dst.get("name", ""),
                mount_path=dst.get("mountPath", ""),
                source=dst.get("source", ""),
                read_only=dst.get("readOnly", False),
                mount_propagation=_get_mount_propagation(),
            )
        case 1:
            # host path
            return HostPathVolume(
                name=dst.get("name", ""),
                mount_path=dst.get("mountPath", ""),
                source=dst.get("source", ""),
                host_path_type=_get_host_path(),
                mount_propagation=_get_mount_propagation(),
            )
        case 2:
            # Ephemeral volume
            return EphemeralVolume(
                name=dst.get("name", ""),
                mount_path=dst.get("mountPath", ""),
                storage=dst.get("storage", ""),
                storage_class=dst.get("storageClassName"),
                access_mode=_get_access_mode(),
                mount_propagation=_get_mount_propagation(),
            )
        case 3:
            # ConfigMap Volume
            return ConfigMapVolume(
                name=dst.get("name", ""),
                mount_path=dst.get("mountPath", ""),
                source=dst.get("source", ""),
                items=dst.get("items"),
            )
        case 4:
            # Secret Volume
            return SecretVolume(
                name=dst.get("name", ""),
                mount_path=dst.get("mountPath", ""),
                source=dst.get("source", ""),
                items=dst.get("items"),
            )
        case 5:
            # Empty dir volume
            return EmptyDirVolume(
                name=dst.get("name", ""), mount_path=dst.get("mountPath", ""), storage=dst.get("storage")
            )
        case _:
            raise Exception(f"Unknown volume type in {dst}")
