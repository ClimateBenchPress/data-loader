__all__ = ["CamsNitrogenDioxideDataset"]

import logging
from pathlib import Path

import xarray as xr

from .. import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from ..download import _download_netcdf
from .abc import Dataset

NO2_FILE = "https://object-store.os-api.cci1.ecmwf.int/esiwacebucket/CAMS/eac4-plev-no2-2023.nc"
NUM_RETRIES = 3


class CamsNitrogenDioxideDataset(Dataset):
    name = "cams-nitrogen-dioxide"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        downloadfile = download_path / Path(NO2_FILE).name

        for _ in range(NUM_RETRIES):
            success = _download_netcdf(NO2_FILE, downloadfile, progress)
            if success:
                break
        if not success:
            logging.info(f"Failed to download {downloadfile}")
            return

    @staticmethod
    def open(download_path: Path) -> xr.Dataset:
        ds = xr.open_dataset(download_path / Path(NO2_FILE).name)

        # Restrict data to a single day.
        # The specific day is arbitrary.
        ds = ds.sel(valid_time=slice("2023-06-15", "2023-06-15"))
        # Needed to make the dataset CF-compliant.
        ds.longitude.attrs["axis"] = "X"
        ds.latitude.attrs["axis"] = "Y"
        ds.pressure_level.attrs["axis"] = "Z"
        return ds


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(CamsNitrogenDioxideDataset)
    open_downloaded_tiny_canonicalized_dataset(CamsNitrogenDioxideDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
