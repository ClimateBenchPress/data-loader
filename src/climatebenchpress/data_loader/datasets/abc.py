__all__ = ["Dataset"]

from abc import ABC, abstractmethod

import xarray as xr


class Dataset(ABC):
    name: str

    @staticmethod
    @abstractmethod
    def open() -> xr.Dataset:
        pass
