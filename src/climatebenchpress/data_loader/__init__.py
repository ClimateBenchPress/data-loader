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
        with ProgressBar():
            cls.open().to_zarr(download, encoding=dict(), compute=False).compute()

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

    huge_download = datasets / cls.name / "download.zarr"
    tiny_standardized = datasets / f"{cls.name}-tiny" / "standardized.zarr"

    if not tiny_standardized.exists():
        if huge_download.exists():
            ds = xr.open_dataset(huge_download, chunks=dict(), engine="zarr")
        else:
            ds = cls.open()

        ds = canon.canonicalize_dataset(ds)

        ds = ds.isel(
            {
                ds.cf["realization"].name: slice(0, 1),
                ds.cf["time"].name: slice(0, 4),
                ds.cf["vertical"].name: slice(0, 4),
                ds.cf["latitude"].name: slice(None),
                ds.cf["longitude"].name: slice(None),
            }
        )

        with ProgressBar():
            ds.to_zarr(tiny_standardized, encoding=dict(), compute=False).compute()

    return xr.open_dataset(tiny_standardized, chunks=dict(), engine="zarr")
