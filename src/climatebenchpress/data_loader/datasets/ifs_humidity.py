__all__ = ["IFSHumidityDataset"]

from collections.abc import Mapping
from pathlib import Path

import xarray as xr

from .. import monitor
from ..cli import main
from .abc import Dataset
from .ifs_uncompressed import load_hplp_data, regrid_to_regular


class IFSHumidityDataset(Dataset):
    """Dataset for the humidity field of the uncompressed IFS data.

    Contains data from the [hplp](https://apps.ecmwf.int/ifs-experiments/rd/hplp/)
    experiment from the Integrated Forecasting System (IFS) model. Crucially,
    this dataset contains uncompressed 64-bit floating point data.
    """

    name = "ifs-humidity"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        donefile = download_path / "download.done"
        if donefile.exists():
            return

        ds = load_hplp_data(leveltype="ml", gridtype="reduced_gg", step=0)
        ds = ds[["q"]]
        ds_regridded = regrid_to_regular(
            ds,
            in_grid={"grid": "O400"},
            out_grid={"grid": [0.25, 0.25]},
        )
        downloadfile = download_path / "ifs_humidity.zarr"
        with monitor.progress_bar(progress):
            ds_regridded.to_zarr(downloadfile, mode="w", compute=False).compute()

    @staticmethod
    def chunks(ds: xr.Dataset) -> int | Mapping[str, int]:
        # Split the vertical levels across two chunks.
        num_levels = ds["level"].size

        return {
            "latitude": -1,
            "longitude": -1,
            "time": -1,
            "level": (num_levels // 2) + 1,
        }

    @staticmethod
    def open(download_path: Path) -> xr.Dataset:
        ds = xr.open_zarr(download_path / "ifs_humidity.zarr").drop_encoding()
        ds = ds.isel(time=slice(0, 1))
        ds = ds.chunk(IFSHumidityDataset.chunks(ds))

        # Needed to make the dataset CF-compliant.
        ds.longitude.attrs["axis"] = "X"
        ds.latitude.attrs["axis"] = "Y"
        ds.level.attrs["axis"] = "Z"
        ds.time.attrs["standard_name"] = "time"
        return ds


if __name__ == "__main__":
    main(IFSHumidityDataset)
