import enum
from typing import Any


class EnvVarSource(enum.Enum):
    """
    Enumeration of environment sources
    """

    CONFIGMAP = 0  # config map
    SECRET = 1  # secret
    RESOURCE_FIELD = 2  # resource field
    FIELD = 3  # field


class EnvVarFrom:
    """
    EnvVarFrom is used to define an environment variable from one of the sources (EnvarSource).
    It provides APIs to create, stringify, convert to dict and json.

    Methods:
    - Create env variable from: gets the following parameters:
        Source required - source of environment variable
        name required name for config map or secret, container name for resource, path for field
        key required Key for config map or secret, resource name for resource
    - to_string() -> str: convert toleration to string for printing
    - to_dict() -> dict[str, Any] convert to dict
    """

    def __init__(self, source: EnvVarSource, name: str, key: str):
        """
        Initialize
        :param source - source
        :param name source name
        :param key source key
        """
        self.source = source
        self.name = name
        self.key = key

    def to_string(self) -> str:
        """
        Convert to string
        :return: string representation of environment from
        """
        return f"source = {self.source.name}, name = {self.name}, key = {self.key}"

    def to_dict(self) -> dict[str, Any]:
        """
        convert to dictionary
        :return: dictionary representation of environment from
        """
        return {"source": self.source.value, "name": self.name, "key": self.key}


class EnvironmentVariables:
    """
    EnvironmentVariables is used to define environment variables.
    It provides APIs to create, stringify, convert to dict and json.

    Methods:
    - Create env variable from: gets the following parameters:
        key_value - optional, dictionary of key/value environment variables
        from_ref - optional, dictionary of reference environment variables
    - to_string() -> str: convert toleration to string for printing
    - to_dict() -> dict[str, Any] convert to dict
    """

    def __init__(self, key_value: dict[str, str] = None, from_ref: dict[str, EnvVarFrom] = None):
        """
        Initialization
        :param key_value: dictionary of key/value pairs for environment variables
        :param from_ref: dictionary of key/value pairs for environment from variables
        """
        self.key_val = key_value
        self.from_ref = from_ref

    def to_string(self) -> str:
        """
        convert to string
        :return: string representation of environment variables
        """
        val = ""
        if self.key_val is not None:
            val = f"values = {str(self.key_val)}"
        if self.from_ref is not None:
            if val != "":
                val += " , "
            val += "valuesFrom = {"
            first = True
            for k, v in self.from_ref.items():
                if not first:
                    val += ", "
                else:
                    first = False
                val += f"{k} = [{v.to_string()}]"
            val += "}"
        return val

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary
        :return: dictionary representation of environment variables
        """
        dst = {}
        if self.key_val is not None:
            dst["values"] = self.key_val
        if self.from_ref is not None:
            fr = {}
            for k, v in self.from_ref.items():
                fr[k] = v.to_dict()
            dst["valuesFrom"] = fr
        return dst


"""
    Creates new environment variable from from dictionary, used for unmarshalling json. Python does not
    support multiple constructors, so do it this way
"""


def env_var_from_decoder(dct: dict[str, Any]) -> EnvVarFrom:
    """
    Create environment from from dictionary
    :param dct: dictionary representations of environment from
    :return: environment from
    """
    return EnvVarFrom(name=dct.get("name", ""), source=EnvVarSource(int(dct.get("source", 0))), key=dct.get("key", ""))


def environment_variables_decoder(dct: dict[str, Any]) -> EnvironmentVariables:
    """
    Create environment variables from from dictionary
    :param dct: dictionary representations of environment variables
    :return: environment variables
    """
    keyvalues = None
    fr = None
    if "values" in dct:
        keyvalues = dct.get("values")
    if "valuesFrom" in dct:
        from_ref = dct.get("valuesFrom")
        fr = {}
        for k, v in from_ref.items():
            fr[k] = env_var_from_decoder(v)
    return EnvironmentVariables(key_value=keyvalues, from_ref=fr)
