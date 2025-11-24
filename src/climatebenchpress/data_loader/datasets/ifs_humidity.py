__all__ = ["IFSUncompressedDataset"]

import argparse
from pathlib import Path

import xarray as xr

from .. import (
    monitor,
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Dataset
from .ifs_uncompressed import load_hplp_data, regrid_to_regular

BASE_URL = "https://object-store.os-api.cci1.ecmwf.int/esiwacebucket"


class IFSHumidityDataset(Dataset):
    """Dataset for the humidity field of the uncompressed IFS data.

    Contains data from the [hplp](https://apps.ecmwf.int/ifs-experiments/rd/hplp/)
    experiment from the Integrated Forecasting System (IFS) model. Crucially,
    this dataset contains uncompressed 64-bit floating point data.
    """

    name = "ifs-humidity"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        ds = load_hplp_data(leveltype="ml", gridtype="reduced_gg", step=0)
        ds = ds[["q"]]
        ds_regridded = regrid_to_regular(
            ds,
            in_grid={"grid": "O400"},
            out_grid={"grid": [0.25, 0.25]},
        )
        downloadfile = download_path / "ifs_humidity.zarr"
        with monitor.progress_bar(progress):
            ds_regridded.to_zarr(
                downloadfile, mode="w", encoding=dict(), compute=False
            ).compute()

    @staticmethod
    def open(download_path: Path) -> xr.Dataset:
        ds = xr.open_dataset(download_path / "ifs_humidity.zarr")

        # Needed to make the dataset CF-compliant.
        ds.longitude.attrs["axis"] = "X"
        ds.latitude.attrs["axis"] = "Y"
        ds.level.attrs["axis"] = "Z"
        ds.time.attrs["standard_name"] = "time"
        return ds


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--basepath", type=Path, default=Path())
    args = parser.parse_args()

    ds = open_downloaded_canonicalized_dataset(
        IFSHumidityDataset, basepath=args.basepath
    )
    open_downloaded_tiny_canonicalized_dataset(
        IFSHumidityDataset, basepath=args.basepath
    )

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
