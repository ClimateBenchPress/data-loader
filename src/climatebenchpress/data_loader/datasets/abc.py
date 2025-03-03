__all__ = ["Dataset"]

from abc import ABC, abstractmethod
from collections.abc import Mapping
from inspect import isabstract
from pathlib import Path
from types import MappingProxyType

import xarray as xr
from typed_classproperties import classproperty


class Dataset(ABC):
    # Abstract interface, must be implemented by sublcasses
    name: str

    @staticmethod
    @abstractmethod
    def download(download_path: Path, progress: bool = True):
        pass

    @staticmethod
    @abstractmethod
    def open(download_path: Path) -> xr.Dataset:
        pass

    # Class interface
    @classproperty
    def registry(cls) -> Mapping:
        return MappingProxyType(Dataset._registry)

    # Implementation details
    _registry: dict[str, type["Dataset"]] = dict()

    @classmethod
    def __init_subclass__(cls: type["Dataset"]) -> None:
        name = getattr(cls, "name", None)

        if isabstract(cls) and name is not None:
            raise TypeError(f"abstract Dataset {cls} must not have a name")
        if not isabstract(cls) and name is None:
            raise TypeError(f"Dataset {cls} must have a name")

        if name is None:
            return

        # Ignore datasets registered in the main module
        if cls.__module__ == "__main__":
            return

        if name in Dataset._registry:
            raise TypeError(
                f"duplicate Dataset name {name} for {cls} vs {Dataset._registry[name]}"
            )

        Dataset._registry[name] = cls

        return super().__init_subclass__()
