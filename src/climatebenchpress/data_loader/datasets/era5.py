__all__ = ["Era5Dataset"]

import argparse
from pathlib import Path

import xarray as xr

from .. import (
    monitor,
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Dataset

ERA5_GCP_PATH = "https://storage.googleapis.com/gcp-public-data-arco-era5/ar/1959-2022-full_37-1h-0p25deg-chunk-1.zarr-v2"


class Era5Dataset(Dataset):
    """ERA5 reanalysis dataset.

    This dataset accesses the cloud optimized ERA5 reanalysis data published on the
    Google Cloud Platform. See [https://github.com/google-research/arco-era5](https://github.com/google-research/arco-era5)
    for more details.

    The original ERA5 dataset is generated and published by [ECMWF](https://www.ecmwf.int/).
    """

    name = "era5"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        downloadfile = download_path / "download.zarr"
        donefile = downloadfile.parent / (downloadfile.name + ".done")
        if donefile.exists():
            return

        era5 = xr.open_zarr(ERA5_GCP_PATH, chunks={"time": 48}, consolidated=True)

        # Restrict data to a single day.
        # The specific day is arbitrary.
        ds = era5.sel(time=slice("2020-03-01", "2020-03-01"))[
            [
                "mean_sea_level_pressure",
                "10m_u_component_of_wind",
                "10m_v_component_of_wind",
            ]
        ].chunk(-1)
        # Needed to make the dataset CF-compliant.
        ds.time.attrs["standard_name"] = "time"
        ds.longitude.attrs["axis"] = "X"
        ds.latitude.attrs["axis"] = "Y"
        with monitor.progress_bar(progress):
            ds.to_zarr(downloadfile, mode="w", encoding=dict(), compute=False).compute()
        donefile.touch()

    @staticmethod
    def open(download_path: Path) -> xr.Dataset:
        return xr.open_zarr(download_path / "download.zarr")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--basepath", type=Path, default=Path())
    args = parser.parse_args()

    ds = open_downloaded_canonicalized_dataset(Era5Dataset, basepath=args.basepath)
    open_downloaded_tiny_canonicalized_dataset(Era5Dataset, basepath=args.basepath)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
