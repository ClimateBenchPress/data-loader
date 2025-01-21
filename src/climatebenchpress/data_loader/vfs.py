__all__ = ["path", "mapping", "xr_store", "zarr_store"]

from collections.abc import MutableMapping
from os import PathLike
from pathlib import Path
from typing import Optional

import xarray.backends.common
import zarr.storage
from fsspec import AbstractFileSystem
from upath import UPath


def path(path: str, fs: Optional[AbstractFileSystem] = None) -> Path:
    if fs is None:
        return Path(path)

    return UPath(fs.unstrip_protocol(str(path)), fs=fs)


def mapping(path: Path) -> PathLike[str] | MutableMapping:
    if not isinstance(path, UPath):
        return path

    return path.fs.get_mapper(str(path))


def xr_store(path: Path) -> PathLike[str] | xarray.backends.common.AbstractDataStore:
    return mapping(path)  # type: ignore


def zarr_store(path: Path) -> PathLike[str] | zarr.storage.FSStore:
    if not isinstance(path, UPath):
        return path

    return zarr.storage.FSStore(path, fs=path.fs)
