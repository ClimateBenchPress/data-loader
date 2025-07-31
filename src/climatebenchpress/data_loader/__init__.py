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
    """Download a given dataset and canonicalize it, i.e. ensure that all the axes names are consistent between different datasets.

    Parameters
    ----------
    cls : type[Dataset]
        The dataset class to download and open
    basepath : Path, optional
        The base path where the dataset should be stored, by default Path()
    progress : bool, optional
        Whether to show a progress bar during the download, by default True

    Returns
    -------
    xr.Dataset
        The canonicalized dataset as an xarray Dataset
    """
    datasets = basepath / "datasets"

    download = datasets / cls.name / "download"
    if not download.exists():
        download.mkdir(parents=True, exist_ok=True)
    # The download function is responsible for checking whether the download is
    # complete or not. If the previous download was interrupt it will resume the download.
    # If the download is complete it will skip the download.
    cls.download(download, progress)

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
    """Same as `open_downloaded_canonicalized_dataset`, but returns a subset of the dataset.

    These tiny datasets are mainly used for testing purposes.

    Parameters
    ----------
    cls : type[Dataset]
        The dataset class to download and open
    basepath : Path, optional
        The base path where the dataset should be stored, by default Path()
    progress : bool, optional
        Whether to show a progress bar during the download, by default True
    slices : Optional[dict[str, slice]], optional
        A dictionary of slices to apply to the dataset, by default None

    Returns
    -------
    xr.Dataset
        The canonicalized tiny dataset as an xarray Dataset
    """
    datasets = basepath / "datasets"

    download = datasets / f"{cls.name}" / "download"
    if not download.exists():
        download.mkdir(parents=True, exist_ok=True)
    cls.download(download, progress)

    standardized = datasets / f"{cls.name}-tiny" / "standardized.zarr"
    if not standardized.exists():
        ds = cls.open(download)
        ds = canon.canonicalize_dataset(ds)
        ds = canon.canonical_tiny_dataset(ds, slices=slices)
        # Rechunk the data because "tiny-fication" can lead to inconsistent or
        # suboptimal chunking.
        ds = _rechunk_dataset(ds)

        with monitor.progress_bar(progress):
            ds.to_zarr(
                standardized, encoding=dict(), compute=False, consolidated=True
            ).compute()

    return xr.open_dataset(standardized, chunks=dict(), engine="zarr")


def _rechunk_dataset(ds: xr.Dataset) -> xr.Dataset:
    rechunked = ds.copy()
    for var_name in ds.data_vars:
        if hasattr(ds[var_name].data, "chunks"):
            rechunked[var_name] = ds[var_name].chunk("auto")

    return rechunked
