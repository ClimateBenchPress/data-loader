__all__ = ["CamsNitrogenDioxideDataset"]

import argparse
import logging
from pathlib import Path

import xarray as xr

from .. import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from ..download import _download_netcdf
from .abc import Dataset

NO2_FILE = (
    "https://object-store.os-api.cci1.ecmwf.int/esiwacebucket/CAMS_hej6/cams_no2.nc"
)
NUM_RETRIES = 3


class CamsNitrogenDioxideDataset(Dataset):
    """Dataset for CAMS Nitrogen Dioxide data.

    The dataset comes from the
    [Copernicus Atmosphere Monitoring Service (CAMS)](https://atmosphere.copernicus.eu/).
    This particular class downloads Nitrogen Dioxide reanalysis data.
    """

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
        ds = xr.open_dataset(download_path / Path(NO2_FILE).name).chunk(-1)

        # valid_time contains actual dates, whereas step is the seconds (in simulated time)
        # since the model as been initialised.
        ds = ds.assign_coords(valid_time=("step", ds.valid_time.data))
        ds = ds.swap_dims({"step": "valid_time"})
        ds = ds.reset_coords("step", drop=True)
        # Needed to make the dataset CF-compliant.
        ds.longitude.attrs["axis"] = "X"
        ds.latitude.attrs["axis"] = "Y"
        ds.hybrid.attrs["axis"] = "Z"
        ds.valid_time.attrs["axis"] = "T"
        return ds


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--basepath", type=Path, default=Path())
    args = parser.parse_args()

    ds = open_downloaded_canonicalized_dataset(
        CamsNitrogenDioxideDataset, basepath=args.basepath
    )
    open_downloaded_tiny_canonicalized_dataset(
        CamsNitrogenDioxideDataset, basepath=args.basepath
    )

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
