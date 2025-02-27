__all__ = [
    "canon",
    "datasets",
    "open_downloaded_canonicalized_dataset",
    "open_downloaded_tiny_canonicalized_dataset",
]

from pathlib import Path
from typing import Optional

import xarray as xr

from . import canon, datasets, monitor
from .datasets.abc import Dataset


def open_downloaded_canonicalized_dataset(
    cls: type[Dataset],
    basepath: Path = Path(),
    progress: bool = True,
) -> xr.Dataset:
    datasets = basepath / "datasets"

    download = datasets / cls.name / "download"
    if not download.exists():
        download.mkdir(parents=True, exist_ok=True)
    # The download function is responsible for checking whether the download is
    # complete or not. If the previous download was interrupt it will resume the download.
    # If the download is complete it will skip the download.
    cls.download(download)

    standardized = datasets / cls.name / "standardized.zarr"
    if not standardized.exists():
        ds = cls.open(download)
        ds = canon.canonicalize_dataset(ds)

        with monitor.progress_bar(progress):
            ds.to_zarr(standardized, encoding=dict(), compute=False).compute()

    return xr.open_dataset(standardized, chunks=dict(), engine="zarr")


def open_downloaded_tiny_canonicalized_dataset(
    cls: type[Dataset],
    basepath: Path = Path(),
    progress: bool = True,
    slices: Optional[dict[str, slice]] = None,
) -> xr.Dataset:
    datasets = basepath / "datasets"

    download = datasets / f"{cls.name}" / "download"
    if not download.exists():
        download.mkdir(parents=True, exist_ok=True)
    cls.download(download)

    standardized = datasets / f"{cls.name}-tiny" / "standardized.zarr"
    if not standardized.exists():
        ds = cls.open(download)
        ds = canon.canonicalize_dataset(ds)
        ds = canon.canonical_tiny_dataset(ds, slices=slices)

        with monitor.progress_bar(progress):
            ds.to_zarr(standardized, encoding=dict(), compute=False).compute()

    return xr.open_dataset(standardized, chunks=dict(), engine="zarr")
