__all__ = ["EsaBiomassCciDataset"]

import logging
from pathlib import Path

import xarray as xr

from .. import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from ..download import _download_netcdf
from .abc import Dataset

NUM_RETRIES = 3
# Bounding box for an area in mainland France
FRANCE_BBOX = {"T": slice(0, 1), "X": slice(202531, 207531), "Y": slice(35469, 40469)}


class EsaBiomassCciDataset(Dataset):
    name = "esa-biomass-cci"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        urls = [
            f"https://dap.ceda.ac.uk/neodc/esacci/biomass/data/agb/maps/v5.01/netcdf/ESACCI-BIOMASS-L4-AGB-MERGED-100m-{year}-fv5.01.nc"
            # Restrict to 2 years for now for smaller download.
            for year in [2010, 2015]
        ]
        for url in urls:
            output_path = download_path / Path(url).name
            for _ in range(NUM_RETRIES):
                success = _download_netcdf(url, output_path, progress)
                if success:
                    break
            if not success:
                logging.info(f"Failed to download {url}")
                return

    @staticmethod
    def open(download_path: Path) -> xr.Dataset:
        # Need string conversion for argument to be interpreted as a glob pattern.
        ds = xr.open_mfdataset(str(download_path / "*.nc"))
        # Needed to make the dataset CF-compliant.
        ds.lon.attrs["axis"] = "X"
        ds.lat.attrs["axis"] = "Y"
        return ds[["agb"]]


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(EsaBiomassCciDataset)
    open_downloaded_tiny_canonicalized_dataset(EsaBiomassCciDataset, slices=FRANCE_BBOX)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
