import logging
from pathlib import Path

import climatebenchpress.data_loader
import climatebenchpress.data_loader.datasets.abc
import climatebenchpress.data_loader.monitor
import fsspec
import numpy as np
import pytest
import xarray as xr
import zarr
from upath import UPath


@pytest.fixture
def basepath(request):
    fs = fsspec.filesystem("memory")
    # Each test needs its own root, since the memory filesystem is global.
    return UPath(fs.unstrip_protocol(request.node.name), fs=fs)


def test_virtual_download(basepath):
    ds = climatebenchpress.data_loader.open_downloaded_canonicalized_dataset(
        VirtualDataset,
        basepath=basepath,
        reprocess=True,
    )
    assert ds.t.shape == (1, 1, 1, 2, 2)

    assert (basepath / "datasets" / "test" / "download" / "download.zarr").exists()
    assert (basepath / "datasets" / "test" / "standardized.zarr").exists()


def test_falls_back_to_reprocessing_when_unpublished(basepath, monkeypatch, caplog):
    monkeypatch.setattr(
        climatebenchpress.data_loader.s3,
        "fetch_standardized",
        lambda name, standardized, progress=True: False,
    )

    with caplog.at_level(logging.WARNING):
        ds = climatebenchpress.data_loader.open_downloaded_canonicalized_dataset(
            VirtualDataset, basepath=basepath
        )

    assert ds.t.shape == (1, 1, 1, 2, 2)
    assert "not published" in caplog.text
    # The dataset had to be reprocessed from the original data source.
    assert (basepath / "datasets" / "test" / "download" / "download.zarr").exists()


def test_skips_reprocessing_when_published(basepath, monkeypatch):
    monkeypatch.setattr(
        climatebenchpress.data_loader.s3,
        "fetch_standardized",
        _publish(chunks=-1, compressor=zarr.storage.default_compressor),
    )

    ds = climatebenchpress.data_loader.open_downloaded_canonicalized_dataset(
        VirtualDataset, basepath=basepath
    )

    assert ds.t.shape == (1, 1, 1, 2, 2)
    # No raw data was downloaded, the pre-processed dataset was used as-is.
    assert not (basepath / "datasets" / "test" / "download").exists()


def test_reformats_outdated_encoding(basepath, monkeypatch, caplog):
    # Mimics the published IFS datasets, which were uploaded before the
    # chunking of the pipeline was fixed.
    monkeypatch.setattr(
        climatebenchpress.data_loader.s3,
        "fetch_standardized",
        _publish(chunks=1, compressor=zarr.Blosc(cname="zstd")),
    )

    with caplog.at_level(logging.WARNING):
        ds = climatebenchpress.data_loader.open_downloaded_canonicalized_dataset(
            VirtualDataset, basepath=basepath
        )

    assert "outdated encoding" in caplog.text
    # The raw data was still not needed to repair the dataset.
    assert not (basepath / "datasets" / "test" / "download").exists()

    xr.testing.assert_identical(ds, _canonical_dataset())
    for var in ds.variables.values():
        assert var.encoding["compressor"] == zarr.storage.default_compressor
    assert ds.t.encoding["chunks"] == (1, 1, 1, 2, 2)


def test_reformats_to_the_declared_chunking(basepath, monkeypatch):
    monkeypatch.setattr(
        climatebenchpress.data_loader.s3,
        "fetch_standardized",
        _publish(chunks=1, compressor=zarr.storage.default_compressor),
    )

    ds = climatebenchpress.data_loader.open_downloaded_canonicalized_dataset(
        ChunkedVirtualDataset, basepath=basepath
    )

    # The lat dimension is split across two chunks, the others are not.
    assert ds.t.encoding["chunks"] == (1, 1, 1, 1, 2)


def test_verify_encoding():
    ds = _canonical_dataset()
    ds.t.encoding.update(
        chunks=(1, 1, 1, 2, 2), compressor=zarr.storage.default_compressor
    )
    for coord in ds.coords.values():
        coord.encoding.update(compressor=zarr.storage.default_compressor)

    assert climatebenchpress.data_loader._verify_encoding(ds, -1) == []

    # A wrong chunking of a data variable is reported ...
    (problem,) = climatebenchpress.data_loader._verify_encoding(ds, 1)
    assert problem == "t is chunked (1, 1, 1, 2, 2) instead of (1, 1, 1, 1, 1)"

    # ... and so is a wrong compressor on a coordinate alone, which is how the
    # published ifs-humidity dataset is out of date.
    ds.lat.encoding.update(compressor=zarr.Blosc(cname="zstd"))
    (problem,) = climatebenchpress.data_loader._verify_encoding(ds, -1)
    assert problem.startswith("lat is compressed with Blosc(cname='zstd'")


def test_verify_encoding_allows_oversized_chunks():
    # The published CMIP6 datasets inherited the chunking of the much longer
    # upstream time series. Such a chunk still stores the array as a single
    # chunk, so it must not trigger a needless reformat.
    ds = _canonical_dataset()
    ds.t.encoding.update(
        chunks=(1, 1, 1, 3432, 3432), compressor=zarr.storage.default_compressor
    )
    for coord in ds.coords.values():
        coord.encoding.update(
            chunks=(3432,), compressor=zarr.storage.default_compressor
        )

    assert climatebenchpress.data_loader._verify_encoding(ds, -1) == []


def _dataset() -> xr.Dataset:
    return xr.Dataset(
        {
            "t": (("lat", "lon"), np.asarray([[1, 2], [3, 4]])),
        },
        coords={
            "lat": ("lat", [-45, 45], {"standard_name": "latitude", "axis": "Y"}),
            "lon": ("lon", [0, 180], {"standard_name": "longitude", "axis": "X"}),
        },
    )


def _canonical_dataset() -> xr.Dataset:
    return climatebenchpress.data_loader.canon.canonicalize_dataset(_dataset())


def _publish(chunks, compressor):
    """Build a `fetch_standardized` that publishes the dataset with a given encoding."""

    def fetch_standardized(
        name: str, standardized: Path, progress: bool = True
    ) -> bool:
        ds = _canonical_dataset().chunk(chunks)
        ds.to_zarr(
            standardized,
            encoding={v: dict(compressor=compressor) for v in ds.variables},
        )
        return True

    return fetch_standardized


class VirtualDataset(climatebenchpress.data_loader.datasets.abc.Dataset):
    name = "test"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        with climatebenchpress.data_loader.monitor.progress_bar(progress):
            _dataset().to_zarr(
                download_path / "download.zarr",
                mode="w",
                compute=False,
            ).compute()

    @staticmethod
    def open(download_path: Path) -> xr.Dataset:
        return xr.open_zarr(download_path / "download.zarr")


class ChunkedVirtualDataset(VirtualDataset):
    name = "test-chunked"

    @staticmethod
    def chunks(ds: xr.Dataset) -> int | dict[str, int]:
        return {"lat": 1, "lon": -1}
