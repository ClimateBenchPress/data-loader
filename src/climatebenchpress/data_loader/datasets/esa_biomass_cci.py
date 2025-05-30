__all__ = ["EsaBiomassCciDataset"]

import logging
from pathlib import Path

import xarray as xr

from .. import (
    _rechunk_dataset,
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from ..download import _download_netcdf
from .abc import Dataset

NUM_RETRIES = 3

# Define rough bounding box coordinates for mainland France.
# Format: [min_longitude, min_latitude, max_longitude, max_latitude].
FRANCE_BBOX = [-5.5, 42.3, 9.6, 51.1]

# Biomass estimate for the year 2020.
BIOMASS_URL = "https://dap.ceda.ac.uk/neodc/esacci/biomass/data/agb/maps/v5.01/netcdf/ESACCI-BIOMASS-L4-AGB-MERGED-100m-2020-fv5.01.nc"


class EsaBiomassCciDataset(Dataset):
    name = "esa-biomass-cci"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        output_path = download_path / Path(BIOMASS_URL).name
        for _ in range(NUM_RETRIES):
            success = _download_netcdf(BIOMASS_URL, output_path, progress)
            if success:
                break
        if not success:
            logging.info(f"Failed to download {BIOMASS_URL}")
            return

    @staticmethod
    def open(download_path: Path) -> xr.Dataset:
        # Need string conversion for argument to be interpreted as a glob pattern.
        ds = xr.open_mfdataset(str(download_path / "*.nc"))
        # Needed to make the dataset CF-compliant.
        ds.lon.attrs["axis"] = "X"
        ds.lat.attrs["axis"] = "Y"
        # We are constraining the dataset to mainland France to reduce its overall size.
        # The global snapshot would be around 20 GB, which is too large for our use case.
        # We chose France because it should have a fairly diverse set of biomass estimates
        # but the choice is overall somewhat arbitrary.
        ds = ds.sel(
            lon=slice(FRANCE_BBOX[0], FRANCE_BBOX[2]),
            lat=slice(FRANCE_BBOX[3], FRANCE_BBOX[1]),
        )
        ds = _rechunk_dataset(ds)
        return ds[["agb"]]


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(EsaBiomassCciDataset)
    num_lon, num_lat = ds.lon.size, ds.lat.size
    open_downloaded_tiny_canonicalized_dataset(
        EsaBiomassCciDataset,
        # Use a smaller spatial subset for the tiny dataset.
        slices={
            "X": slice(num_lon // 2, (num_lon // 2) + 500),
            "Y": slice(num_lat // 2, (num_lat // 2) + 500),
        },
    )

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
