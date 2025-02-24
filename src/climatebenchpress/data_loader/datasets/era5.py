__all__ = ["Era5Dataset"]

import xarray as xr

from .. import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Dataset

ERA5_GCP_PATH = "https://storage.googleapis.com/gcp-public-data-arco-era5/ar/1959-2022-full_37-1h-0p25deg-chunk-1.zarr-v2"


class Era5Dataset(Dataset):
    name = "era5"

    @staticmethod
    def open() -> xr.Dataset:
        era5 = xr.open_zarr(ERA5_GCP_PATH, chunks={"time": 48}, consolidated=True)

        ds = era5.sel(time=slice("2020-03-01", "2020-03-07"))[
            [
                "mean_sea_level_pressure",
                "10m_u_component_of_wind",
                "10m_v_component_of_wind",
            ]
        ]
        # Needed to make the dataset CF-compliant.
        ds.time.attrs["standard_name"] = "time"
        ds.latitude.attrs["axis"] = "X"
        ds.longitude.attrs["axis"] = "Y"
        return ds


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Era5Dataset)
    open_downloaded_tiny_canonicalized_dataset(Era5Dataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
