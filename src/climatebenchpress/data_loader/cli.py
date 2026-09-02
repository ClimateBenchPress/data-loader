__all__ = ["main"]

import argparse
from collections.abc import Callable
from pathlib import Path
from typing import Optional

import xarray as xr

from . import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .datasets.abc import Dataset


def main(
    cls: type[Dataset],
    tiny_slices: Optional[Callable[[xr.Dataset], dict[str, slice]]] = None,
) -> None:
    """Download a dataset from the command line.

    This is the shared entry point for all `python -m climatebenchpress.data_loader.datasets.*`
    scripts. By default the pre-processed dataset is downloaded from the
    ClimateBenchPress object store, unless `--reprocess-data` is passed.

    Parameters
    ----------
    cls : type[Dataset]
        The dataset class to download and open
    tiny_slices : Optional[Callable[[xr.Dataset], dict[str, slice]]], optional
        Computes the slices that are used to build the tiny variant of the dataset
        from the full dataset, by default None, i.e. the default slices are used
    """
    parser = argparse.ArgumentParser(description=cls.__doc__)
    parser.add_argument(
        "--basepath",
        type=Path,
        default=Path(),
        help="the directory that the `datasets` directory is created in",
    )
    parser.add_argument(
        "--reprocess-data",
        action="store_true",
        help=(
            "reprocess the dataset from its original data source instead of "
            "downloading the pre-processed dataset from the ClimateBenchPress "
            "object store; note that an already-processed dataset is never "
            "rebuilt, delete it first to force a rebuild"
        ),
    )
    args = parser.parse_args()

    ds = open_downloaded_canonicalized_dataset(
        cls, basepath=args.basepath, reprocess=args.reprocess_data
    )
    open_downloaded_tiny_canonicalized_dataset(
        cls,
        basepath=args.basepath,
        slices=None if tiny_slices is None else tiny_slices(ds),
        reprocess=args.reprocess_data,
    )

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
