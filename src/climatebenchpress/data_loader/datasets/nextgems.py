__all__ = ["NextGemsDataset"]

import argparse
from pathlib import Path

import healpy
import intake
import numpy as np
import xarray as xr

from .. import (
    monitor,
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Dataset

NEXTGEMS_CATALOG = "https://data.nextgems-h2020.eu/online.yaml"
# Zoom level for the HealPIX grid (https://easy.gems.dkrz.de/Processing/healpix/index.html#healpix-spatial-resolution).
# See https://swift.dkrz.de/v1/dkrz_b381d76e-63d7-4aeb-96f0-dfd91e102d40/nextgems_prefinal/nextgems_prefinal_output.html
# for which variables are available at a given zoom level and time resolution.
ZOOM = 9
# Allowed values: "P1D" (daily), "PT3H" (3-hourly), "PT15M" (15-minute).
TIME_RESOLUTION = "PT3H"

PRECIP_KEY = "pr"
OLR_KEY = "rlut"

NUM_LON = 2880
NUM_LAT = 1440


class NextGemsDataset(Dataset):
    """NextGEMS ICON dataset.

    This dataset provides model output from ICON climate model runs, published as
    part of the [NextGEMS project](https://nextgems-h2020.eu/).

    See [https://easy.gems.dkrz.de/index.html](https://easy.gems.dkrz.de/index.html)
    for more details on the NextGEMS data.
    """

    name = "nextgems-icon"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        downloadfile = download_path / "download.zarr"
        donefile = downloadfile.parent / (downloadfile.name + ".done")
        if donefile.exists():
            return

        cat = intake.open_catalog(NEXTGEMS_CATALOG)
        icon = cat["ICON.ngc4008"](
            zoom=ZOOM, time=TIME_RESOLUTION, chunks=dict()
        ).to_dask()

        # Restrict data to a single day.
        # The specific day is arbitrary.
        ds = icon[[PRECIP_KEY, OLR_KEY]].sel(time=slice("2020-03-01", "2020-03-01"))
        # Regrid the data to 0.125 degree resolution.
        # NOTE:
        # This is using nearest neighbour interpolation. Different interpolation methods
        # should not have a drastic effect on the intercomparison of different compressors.
        # However, this should be studied in more detail because re-gridding can often
        # have unforeseen consequences.
        idx = _get_nn_lon_lat_index(
            2**ZOOM, np.linspace(-180, 180, NUM_LON), np.linspace(-90, 90, NUM_LAT)
        )
        ds = ds.isel(cell=idx).chunk(-1)
        ds.lon.attrs["axis"] = "X"
        ds.lat.attrs["axis"] = "Y"

        with monitor.progress_bar(progress):
            ds.to_zarr(downloadfile, mode="w", encoding=dict(), compute=False).compute()
        donefile.touch()

    @staticmethod
    def open(download_path: Path) -> xr.Dataset:
        return xr.open_zarr(download_path / "download.zarr")


def _get_nn_lon_lat_index(nside, lons, lats):
    """For each lon/lat pair, find the nearest neighbour index in the HEALPix grid.

    The HEALPix grid is not a rectilinear grid, in xarray all the individual cells
    are stored in a single dimension indexed by a cell number. This function maps
    lon/lat pairs and returns the cell number of cell closest to a given lon/lat pair.

    See https://easy.gems.dkrz.de/Processing/healpix/lonlat_remap.html for more details.

    Args:
        nside (int): The HealPIX grid resolution.
        lons (np.ndarray): The longitudes.
        lats (np.ndarray): The latitudes.

    Returns:
        xr.DataArray: The nearest neighbour index for each lon/lat pair.
    """
    lons2, lats2 = np.meshgrid(lons, lats)
    return xr.DataArray(
        healpy.ang2pix(nside, lons2, lats2, nest=True, lonlat=True),
        coords=[("lat", lats), ("lon", lons)],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--basepath", type=Path, default=Path())
    args = parser.parse_args()

    ds = open_downloaded_canonicalized_dataset(NextGemsDataset, basepath=args.basepath)
    open_downloaded_tiny_canonicalized_dataset(NextGemsDataset, basepath=args.basepath)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
