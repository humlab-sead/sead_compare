import io
import os
from inspect import isclass
from pathlib import Path
from typing import Any, Type

import dotenv
import yaml

from ..utility import dget, dotexists, dotset

dotenv.load_dotenv()


class Config(dict):
    class SafeLoaderIgnoreUnknown(yaml.SafeLoader):  # pylint: disable=too-many-ancestors
        def let_unknown_through(self, node):  # pylint: disable=unused-argument
            return None

    def __init__(self, __map: dict = None, filename: str | None = None):
        if __map is None and filename is not None:
            __map = Config.load(source=filename)

        super().__init__(__map or {})

    def get(self, *keys: str, default: Any | Type[Any] = None, mandatory: bool = False) -> Any:
        if mandatory and not self.exists(*keys):
            raise ValueError(f"Missing mandatory key: {keys}")

        value: Any = dget(self, *keys)

        if value is not None:
            return value

        return default() if isclass(default) else default

    def exists(self, *keys) -> bool:
        return dotexists(self, *keys)

    @staticmethod
    def load(source: str | dict, *, env_prefix: str = None) -> "Config":
        if isinstance(source, Config):
            return source
        data = source
        data: dict = (
            (
                yaml.load(
                    Path(source).read_text(encoding="utf-8"),
                    Loader=Config.SafeLoaderIgnoreUnknown,
                )
                if Config.is_config_path(source)
                else yaml.load(io.StringIO(source), Loader=Config.SafeLoaderIgnoreUnknown)
            )
            if isinstance(source, str)
            else source
        )
        if not isinstance(data, dict):
            raise TypeError(f"expected dict, found {type(data)}")

        return Config(data, filename=source if Config.is_config_path(source) else None).load_environment(env_prefix)

    def load_environment(self, prefix: str = None) -> "Config":
        if not (prefix or "").strip():
            return self

        prefix = prefix.upper()

        for key, value in os.environ.items():
            if key.lower().startswith(prefix.lower()):
                dotpath: str = key[len(prefix) :].lower().replace("_", ".").replace(":", ".")
                dotset(self, dotpath, value)

        return self

    @staticmethod
    def is_config_path(source) -> bool:
        try:
            if not isinstance(source, str):
                return False
            return source.endswith(".yaml") or source.endswith(".yml") or os.path.isfile(source)
        except (TypeError, ValueError):
            return False
