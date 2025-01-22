__all__ = [
    "canon",
    "datasets",
    "open_downloaded_canonicalized_dataset",
    "open_downloaded_tiny_canonicalized_dataset",
]

from pathlib import Path

import xarray as xr
from dask.diagnostics.progress import ProgressBar

from . import canon, datasets
from .datasets.abc import Dataset


def open_downloaded_canonicalized_dataset(
    cls: type[Dataset], basepath: Path = Path()
) -> xr.Dataset:
    datasets = basepath / "datasets"

    download = datasets / cls.name / "download.zarr"
    if not download.exists():
        ds = cls.open()

        with ProgressBar():
            ds.to_zarr(download, encoding=dict(), compute=False).compute()

    standardized = datasets / cls.name / "standardized.zarr"
    if not standardized.exists():
        ds = xr.open_dataset(download, chunks=dict(), engine="zarr")
        ds = canon.canonicalize_dataset(ds)

        with ProgressBar():
            ds.to_zarr(standardized, encoding=dict(), compute=False).compute()

    return xr.open_dataset(standardized, chunks=dict(), engine="zarr")


def open_downloaded_tiny_canonicalized_dataset(
    cls: type[Dataset], basepath: Path = Path()
) -> xr.Dataset:
    datasets = basepath / "datasets"

    download = datasets / f"{cls.name}-tiny" / "download.zarr"
    if not download.exists():
        ds = cls.open()
        ds = canon.canonical_tiny_dataset(ds)

        with ProgressBar():
            ds.to_zarr(download, encoding=dict(), compute=False).compute()

    standardized = datasets / f"{cls.name}-tiny" / "standardized.zarr"
    if not standardized.exists():
        ds = xr.open_dataset(download, chunks=dict(), engine="zarr")
        ds = canon.canonicalize_dataset(ds)

        with ProgressBar():
            ds.to_zarr(standardized, encoding=dict(), compute=False).compute()

    return xr.open_dataset(standardized, chunks=dict(), engine="zarr")
