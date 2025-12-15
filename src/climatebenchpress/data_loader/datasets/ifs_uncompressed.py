__all__ = ["IFSUncompressedDataset"]

import argparse
from pathlib import Path

import earthkit.regrid
import numpy as np
import requests
import xarray as xr

from .. import (
    monitor,
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Dataset

BASE_URL = "https://object-store.os-api.cci1.ecmwf.int/esiwacebucket"


class IFSUncompressedDataset(Dataset):
    """Dataset for IFS uncompressed data.

    Contains data from the [hplp](https://apps.ecmwf.int/ifs-experiments/rd/hplp/)
    experiment from the Integrated Forecasting System (IFS) model. Crucially,
    this dataset contains uncompressed 64-bit floating point data.
    """

    name = "ifs-uncompressed"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        ds = load_hplp_data(leveltype="sfc", gridtype="reduced_gg")
        ds = ds[["msl", "10u", "10v"]]
        ds_regridded = regrid_to_regular(
            ds,
            in_grid={"grid": "O400"},
            out_grid={"grid": [0.25, 0.25]},
        )
        downloadfile = download_path / "ifs_uncompressed.zarr"
        with monitor.progress_bar(progress):
            ds_regridded.to_zarr(
                downloadfile, mode="w", encoding=dict(), compute=False
            ).compute()

    @staticmethod
    def open(download_path: Path) -> xr.Dataset:
        ds = xr.open_dataset(download_path / "ifs_uncompressed.zarr")

        # Needed to make the dataset CF-compliant.
        ds.longitude.attrs["axis"] = "X"
        ds.latitude.attrs["axis"] = "Y"
        ds.time.attrs["standard_name"] = "time"
        return ds


def load_hplp_data(leveltype=None, gridtype=None, step=None, remap=False):
    """Function taken from: https://github.com/climet-eu/compression-lab-notebooks/blob/d297ee98be916359fde16ab36f0f9e0681662df8/04-example-datasets/01-hplp.ipynb."""
    if leveltype not in {"pl", "ml", "sfc", "wave"}:
        raise ValueError(
            f"Invalid leveltype: '{leveltype}'. Available leveltypes: pl, ml, sfc, wave"
        )

    if leveltype in {"ml", "pl"} and not gridtype:
        raise ValueError(
            f"Gridtype is required for leveltype '{leveltype}'. Available: reduced_gg, sh"
        )

    if remap and gridtype != "sh":
        raise ValueError("Only 'sh' fields can be remapped.")

    if leveltype == "wave" and gridtype != "reduced_ll":
        print("Warning: Wave model data are stored on a reduced_ll grid.")

    if leveltype == "sfc" and gridtype != "reduced_gg":
        print("Warning: Surface level data are stored on a reduced_gg grid.")

    if step and not (leveltype == "ml" and gridtype == "reduced_gg"):
        print(
            "Warning: Specifying 'step' is unnecessary for this configuration and will be ignored."
        )

    if leveltype in {"sfc", "wave"}:
        url = f"{BASE_URL}/hplp/hplp_{leveltype}.grib"
    elif leveltype == "ml" and gridtype == "reduced_gg":
        if step is None:
            raise ValueError(
                "The ml reduced_gg data are split into two parts:\n"
                "  - Steps: 0, 12, 24, 36, 48, 60, 72, 84, 96, 108, 120 (2020-07-21T00:00:00 to 2020-07-26T00:00:00)\n"
                "  - Steps: 132, 144, 156, 168, 180, 192, 204, 216, 228, 240 (2020-07-26T12:00:00 to 2020-07-31T00:00:00)\n"
                "Specify a step smaller than 120 for accessing the first part, \n"
                "and a step greater or equal to 132 for accessing the second part."
            )
        if step <= 120:
            url = f"{BASE_URL}/hplp/hplp_{leveltype}_{gridtype}_levels_0_120.grib"
        else:
            url = f"{BASE_URL}/hplp/hplp_{leveltype}_{gridtype}_levels_132_240.grib"
    else:
        url = f"{BASE_URL}/hplp/hplp_{leveltype}_{gridtype}" + (
            "_O400.grib" if remap else ".grib"
        )
    ref = requests.get(f"{url}.ref").json()

    print(f"Loading dataset {url}")

    return xr.open_dataset(
        "reference://",
        engine="zarr",
        backend_kwargs=dict(storage_options=dict(fo=ref, asynchronous=False)),
        consolidated=False,
    )


def regrid_to_regular(ds, in_grid, out_grid):
    """Regrid dataset to a regular lat-lon grid.

    Parameters
    ----------
    ds : xr.Dataset
        The input dataset to regrid
    in_grid : dict
        The input grid specification for earthkit.regrid.interpolate
    out_grid : dict
        The output grid specification for earthkit.regrid.interpolate. Is assumed to be
        a regular lat-lon grid with equal spacing in latitude and longitude, e.g. {"grid": [0.25, 0.25]}.
    """
    out_data = {var: [] for var in ds.data_vars}
    for var in ds.data_vars:
        var_has_level = "level" in ds[var].dims
        for time in ds.time:
            if var_has_level:
                level_data = []
                for level in ds[var].level:
                    r = earthkit.regrid.interpolate(
                        ds[var].sel(time=time, level=level).values,
                        in_grid=in_grid,
                        out_grid=out_grid,
                        method="linear",
                    )
                    level_data.append(r)
                out_data[var].append(level_data)
            else:
                r = earthkit.regrid.interpolate(
                    ds[var].sel(time=time).values,
                    in_grid=in_grid,
                    out_grid=out_grid,
                    method="linear",
                )
                out_data[var].append(r)

    dx = out_grid["grid"][0]
    assert out_grid["grid"][0] == out_grid["grid"][1], (
        "Only grids with equal latitude and longitude spacing are supported."
    )
    lats = np.linspace(90, -90, int(180 / dx) + 1)
    lons = np.linspace(0, 360 - dx, int(360 / dx))
    coords = {
        "time": ds.time,
        "latitude": lats,
        "longitude": lons,
    }

    data_vars = {}
    for var in ds.data_vars:
        if "level" in ds[var].dims:
            coords["level"] = ds[var].level
            data_vars[var] = (("time", "level", "latitude", "longitude"), out_data[var])
        else:
            data_vars[var] = (("time", "latitude", "longitude"), out_data[var])

    out_ds = xr.Dataset(data_vars, coords=coords)
    return out_ds


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--basepath", type=Path, default=Path())
    args = parser.parse_args()

    ds = open_downloaded_canonicalized_dataset(
        IFSUncompressedDataset, basepath=args.basepath
    )
    open_downloaded_tiny_canonicalized_dataset(
        IFSUncompressedDataset, basepath=args.basepath
    )

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
