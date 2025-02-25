import climatebenchpress.data_loader
import climatebenchpress.data_loader.datasets.abc
import fsspec
import xarray as xr
from upath import UPath


def test_virtual_download():
    fs = fsspec.filesystem("memory")
    basepath = UPath(fs.unstrip_protocol("root"), fs=fs)

    ds = climatebenchpress.data_loader.open_downloaded_canonicalized_dataset(
        TestDataset,
        basepath=basepath,
    )
    assert ds.t.shape == (1, 1, 1, 2, 2)

    assert (basepath / "datasets" / "test" / "download.zarr").exists()
    assert (basepath / "datasets" / "test" / "standardized.zarr").exists()


class TestDataset(climatebenchpress.data_loader.datasets.abc.Dataset):
    name = "test"

    @staticmethod
    def open() -> xr.Dataset:
        return xr.Dataset(
            {
                "t": (("lat", "lon"), [[1, 2], [3, 4]]),
            },
            coords={
                "lat": ("lat", [-45, 45], {"standard_name": "latitude", "axis": "Y"}),
                "lon": ("lon", [0, 180], {"standard_name": "longitude", "axis": "X"}),
            },
        )
