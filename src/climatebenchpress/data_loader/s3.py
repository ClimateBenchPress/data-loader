__all__ = [
    "fetch_standardized",
    "is_published",
    "list_published",
    "move_tree",
    "remove_tree",
]

import logging
import shutil
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from pathlib import Path
from typing import Any

import fsspec
from tqdm import tqdm

# The object store that hosts the pre-processed ClimateBenchPress datasets.
ENDPOINT_URL = "https://object-store.os-api.cci1.ecmwf.int"
BUCKET_PREFIX = "esiwacebucket/ClimateBenchPress"

STANDARDIZED = "standardized.zarr"

# Number of objects that are copied in parallel. The Zarr stores consist of
# many small-ish chunk files, for which the round-trip latency dominates.
_NUM_WORKERS = 8

_COPY_BUFFER_SIZE = 4 * 1024 * 1024


@lru_cache
def _filesystem() -> Any:
    # The bucket is publicly readable, so no credentials are required.
    return fsspec.filesystem("s3", anon=True, endpoint_url=ENDPOINT_URL)


def _source_root(name: str) -> str:
    return f"{BUCKET_PREFIX}/{name}/{STANDARDIZED}"


def list_published() -> list[str]:
    """List the names of all datasets that are published in the object store.

    Returns
    -------
    list[str]
        The sorted dataset names, e.g. ``["cams-nitrogen-dioxide", ...]``.
    """
    fs = _filesystem()

    return sorted(
        entry.rstrip("/").rsplit("/", 1)[-1] for entry in fs.ls(BUCKET_PREFIX)
    )


def is_published(name: str) -> bool:
    """Check whether a dataset is published in the object store.

    Parameters
    ----------
    name : str
        The name of the dataset, e.g. ``"cams-nitrogen-dioxide"``.

    Returns
    -------
    bool
        Whether the pre-processed dataset is available for download.
    """
    return bool(_filesystem().exists(_source_root(name)))


def fetch_standardized(name: str, standardized: Path, progress: bool = True) -> bool:
    """Download the pre-processed dataset from the ClimateBenchPress object store.

    The Zarr store is copied byte-for-byte so that the chunking, the compressors
    and the encoding are exactly as published.

    The store is first assembled in a sibling `.partial` directory and only moved
    into place once every object has been copied, such that an interrupted
    download never leaves behind a store that looks complete.

    Parameters
    ----------
    name : str
        The name of the dataset, e.g. ``"cams-nitrogen-dioxide"``. Note that the
        tiny variants are published under their own name, e.g.
        ``"cams-nitrogen-dioxide-tiny"``.
    standardized : Path
        The path that the `standardized.zarr` store is downloaded to.
    progress : bool, optional
        Whether to show a progress bar during the download, by default True

    Returns
    -------
    bool
        True if the dataset was downloaded, False if it is not published, in
        which case nothing has been written.
    """
    fs = _filesystem()

    source = _source_root(name)
    if not fs.exists(source):
        return False

    # Fetch the sizes alongside the listing so that the progress bar does not
    # need a HEAD request per object.
    sizes = {
        key: info.get("size", 0)
        for key, info in fs.find(source, detail=True).items()
        if not key.endswith("/")
    }

    partial = standardized.parent / (standardized.name + ".partial")
    if partial.exists():
        remove_tree(partial)
    partial.mkdir(parents=True, exist_ok=True)

    logging.debug(f"Downloading {len(sizes)} objects from {source} to {standardized}")

    with tqdm(
        total=sum(sizes.values()),
        unit="B",
        unit_scale=True,
        desc=f"{name}/{STANDARDIZED}",
        ascii=True,
        disable=not progress,
    ) as pbar:

        def copy(key: str) -> None:
            target = partial
            for part in key[len(source) + 1 :].split("/"):
                target = target / part
            target.parent.mkdir(parents=True, exist_ok=True)

            with fs.open(key, "rb") as fsrc, target.open("wb") as fdst:
                shutil.copyfileobj(fsrc, fdst, _COPY_BUFFER_SIZE)

            pbar.update(sizes[key])

        with ThreadPoolExecutor(max_workers=_NUM_WORKERS) as pool:
            # Consume the iterator so that any exception is re-raised here.
            for _ in pool.map(copy, sizes):
                pass

    move_tree(partial, standardized)

    return True


def move_tree(src: Path, dst: Path) -> None:
    """Move a directory and everything inside it.

    Falls back to a recursive copy for the filesystems that `basepath` may point
    at which cannot rename a non-empty directory, e.g. fsspec's in-memory
    filesystem. Such a rename fails without moving anything, so the fallback
    always starts from an intact `src`.

    Parameters
    ----------
    src : Path
        The directory to move
    dst : Path
        The path to move it to, which must not exist yet
    """
    try:
        src.rename(dst)
        return
    except OSError:
        pass

    dst.mkdir(parents=True, exist_ok=True)

    for child in src.iterdir():
        if child.is_dir():
            move_tree(child, dst / child.name)
        else:
            with child.open("rb") as fsrc, (dst / child.name).open("wb") as fdst:
                shutil.copyfileobj(fsrc, fdst, _COPY_BUFFER_SIZE)
            child.unlink()

    src.rmdir()


def remove_tree(path: Path) -> None:
    """Recursively remove a directory and everything inside it.

    Unlike `shutil.rmtree`, this only uses the `pathlib.Path` interface and thus
    also works for the non-local paths that `basepath` may point at.

    Parameters
    ----------
    path : Path
        The directory to remove
    """
    for child in path.iterdir():
        if child.is_dir():
            remove_tree(child)
        else:
            child.unlink()
    path.rmdir()
