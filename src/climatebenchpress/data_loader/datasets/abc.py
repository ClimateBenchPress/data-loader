__all__ = ["Dataset"]

from abc import ABC, abstractmethod
from collections.abc import Mapping
from inspect import isabstract
from pathlib import Path
from types import MappingProxyType

import xarray as xr
from typed_classproperties import classproperty


class Dataset(ABC):
    """Abstract base class for datasets.

    Each dataset has a unique name associated
    with it that will be used to name the directory where the dataset is stored.

    The dataset should implement the `download` and `open` methods to handle downloading
    the dataset (in whatever data format the original data comes) and opening it as an xarray
    Dataset, respectively.
    """

    name: str

    @staticmethod
    @abstractmethod
    def download(download_path: Path, progress: bool = True):
        """Download the dataset to the specified path. The download function is responsible
        for checking whether the download is complete or not. If the previous download was
        interrupted, it will resume the download. If the download is complete, it will skip
        the download.

        Parameters
        ----------
        download_path : Path
            The path where the dataset should be downloaded to
        progress : bool, optional
            Whether to show a progress bar during the download, by default True
        """
        pass

    @staticmethod
    def chunks(ds: xr.Dataset) -> int | Mapping[str, int]:
        """The chunking that the canonicalized dataset should be stored with.

        The return value is passed to `xarray.Dataset.chunk`. By default, each
        array is stored as a single chunk.

        This is the single source of truth for the chunking of a dataset: it is
        applied by `open` and is also used to check whether a pre-processed
        dataset that was downloaded from the object store is still up to date.

        Parameters
        ----------
        ds : xr.Dataset
            The dataset that is about to be chunked

        Returns
        -------
        int | Mapping[str, int]
            The chunk sizes, either for all dimensions or per dimension
        """
        return -1

    @staticmethod
    @abstractmethod
    def open(download_path: Path) -> xr.Dataset:
        """Open the dataset from the specified path as an xarray Dataset.

        Parameters
        ----------
        download_path : Path
            The path where the dataset is stored

        Returns
        -------
        xr.Dataset
            The dataset as an xarray Dataset
        """
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
