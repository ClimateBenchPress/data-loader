__all__ = [
    "canon",
    "datasets",
    "s3",
    "open_downloaded_canonicalized_dataset",
    "open_downloaded_tiny_canonicalized_dataset",
]

import logging
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Optional, Union

import xarray as xr
import zarr

from . import canon, datasets, monitor, s3
from .datasets.abc import Dataset

Chunks = Union[int, Mapping[str, int]]


def open_downloaded_canonicalized_dataset(
    cls: type[Dataset],
    basepath: Path = Path(),
    progress: bool = True,
    reprocess: bool = False,
) -> xr.Dataset:
    """Download a given dataset and canonicalize it, i.e. ensure that all the axes names are consistent between different datasets.

    By default, the pre-processed dataset is downloaded from the S3 ESIWACE
    object store. If it is not published there, or if `reprocess` is set, it is
    instead reprocessed from the original data source.

    Parameters
    ----------
    cls : type[Dataset]
        The dataset class to download and open
    basepath : Path, optional
        The base path where the dataset should be stored, by default Path()
    progress : bool, optional
        Whether to show a progress bar during the download, by default True
    reprocess : bool, optional
        Whether to reprocess the dataset from the original data source instead of
        downloading the pre-processed dataset, by default False

    Returns
    -------
    xr.Dataset
        The canonicalized dataset as an xarray Dataset
    """
    datasets = basepath / "datasets"

    standardized = datasets / cls.name / "standardized.zarr"
    if not standardized.exists() and not _download_standardized(
        cls.name,
        standardized,
        chunks=cls.chunks,
        consolidated=None,
        progress=progress,
        reprocess=reprocess,
    ):
        download = datasets / cls.name / "download"
        if not download.exists():
            download.mkdir(parents=True, exist_ok=True)
        # The download function is responsible for checking whether the download is
        # complete or not. If the previous download was interrupt it will resume the download.
        # If the download is complete it will skip the download.
        cls.download(download, progress)

        ds = cls.open(download)
        ds = canon.canonicalize_dataset(ds)

        with monitor.progress_bar(progress):
            ds.to_zarr(standardized, compute=False).compute()

    return xr.open_dataset(standardized, chunks=dict(), engine="zarr")


def open_downloaded_tiny_canonicalized_dataset(
    cls: type[Dataset],
    basepath: Path = Path(),
    progress: bool = True,
    slices: Optional[dict[str, slice]] = None,
    reprocess: bool = False,
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
    reprocess : bool, optional
        Whether to reprocess the dataset from the original data source instead of
        downloading the pre-processed dataset, by default False

    Returns
    -------
    xr.Dataset
        The canonicalized tiny dataset as an xarray Dataset
    """
    datasets = basepath / "datasets"

    standardized = datasets / f"{cls.name}-tiny" / "standardized.zarr"
    if not standardized.exists() and not _download_standardized(
        f"{cls.name}-tiny",
        standardized,
        # "Tiny-fication" can lead to inconsistent or suboptimal chunking, so the
        # tiny datasets are always stored as a single chunk per array.
        chunks=lambda ds: -1,
        consolidated=True,
        progress=progress,
        reprocess=reprocess,
    ):
        download = datasets / f"{cls.name}" / "download"
        if not download.exists():
            download.mkdir(parents=True, exist_ok=True)
        cls.download(download, progress)

        ds = cls.open(download)
        ds = canon.canonicalize_dataset(ds)
        ds = canon.canonical_tiny_dataset(ds, slices=slices)
        # Rechunk the data because "tiny-fication" can lead to inconsistent or
        # suboptimal chunking.
        ds = ds.chunk(-1)

        with monitor.progress_bar(progress):
            ds.to_zarr(standardized, compute=False, consolidated=True).compute()

    return xr.open_dataset(standardized, chunks=dict(), engine="zarr")


def _download_standardized(
    name: str,
    standardized: Path,
    chunks: Callable[[xr.Dataset], Chunks],
    consolidated: Optional[bool],
    progress: bool,
    reprocess: bool,
) -> bool:
    """Download the pre-processed dataset from the S3 ESIWACE object store.

    If the published dataset was written by an older version of the pipeline, and
    thus uses an outdated chunking or compressor, it is reformatted locally. Note
    that only the encoding of the dataset is checked, not its contents.

    Returns
    -------
    bool
        Whether the pre-processed dataset is now available at `standardized`
    """
    if reprocess:
        return False

    if not s3.fetch_standardized(name, standardized, progress):
        logging.warning(
            f"{name} is not published in the S3 ESIWACE object store, "
            f"reprocessing it from the original data source instead"
        )
        return False

    ds = xr.open_zarr(standardized)
    spec = chunks(ds)

    problems = _verify_encoding(ds, spec)
    if problems:
        logging.warning(
            f"the published {name} uses an outdated encoding "
            f"({'; '.join(problems)}), reformatting it locally"
        )
        _reformat(standardized, ds, spec, consolidated, progress)

    return True


def _verify_encoding(ds: xr.Dataset, chunks: Chunks) -> list[str]:
    """Check that a canonicalized dataset is stored the way the pipeline writes it.

    Returns
    -------
    list[str]
        The encoding problems that were found, empty if the dataset is up to date
    """
    problems = []

    for name, var in ds.variables.items():
        # The pipeline drops the encoding of the original data before writing,
        # so every array uses Zarr's default compressor and no filters.
        compressor = var.encoding.get("compressor")
        if compressor != zarr.storage.default_compressor:
            problems.append(f"{name} is compressed with {compressor}")

        filters = var.encoding.get("filters")
        if filters:
            problems.append(f"{name} is filtered with {filters}")

        # Only the data variables are chunked, the coordinates are always stored
        # as a single chunk.
        expected = _expected_chunks(var, chunks if name in ds.data_vars else -1)
        actual = var.encoding.get("chunks")

        if actual is not None:
            # A chunk that is larger than the array still stores the array in a
            # single chunk, so only the effective chunk grid is compared. The
            # published CMIP6 datasets, for instance, inherited the time
            # chunking of the much longer upstream time series.
            effective = tuple(
                min(chunk, size) for chunk, size in zip(actual, var.shape)
            )
            if effective != expected:
                problems.append(f"{name} is chunked {effective} instead of {expected}")

    return problems


def _expected_chunks(var: xr.Variable, chunks: Chunks) -> tuple[int, ...]:
    if not isinstance(chunks, Mapping):
        chunks = {str(dim): chunks for dim in var.dims}

    return tuple(
        size if (chunk := chunks.get(str(dim), -1)) in (-1, None) else min(chunk, size)
        for dim, size in zip(var.dims, var.shape)
    )


def _reformat(
    standardized: Path,
    ds: xr.Dataset,
    chunks: Chunks,
    consolidated: Optional[bool],
    progress: bool,
) -> None:
    """Rewrite a dataset with the chunking and compressor that the pipeline uses.

    The dataset is written to a sibling directory first, so that it stays readable
    while it is being reformatted.
    """
    reformatted = standardized.parent / (standardized.name + ".reformatted")
    if reformatted.exists():
        s3.remove_tree(reformatted)

    ds = ds.drop_encoding().chunk(chunks)

    with monitor.progress_bar(progress):
        ds.to_zarr(reformatted, compute=False, consolidated=consolidated).compute()

    s3.remove_tree(standardized)
    s3.move_tree(reformatted, standardized)
