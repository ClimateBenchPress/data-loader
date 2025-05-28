__all__ = [
    "Cmip6Dataset",
    "Cmip6AtmosphereDataset",
    "Cmip6OceanDataset",
]

from functools import lru_cache
from pathlib import Path
from typing import Optional

import fsspec
import pandas as pd
import xarray as xr

from ... import monitor
from ..abc import Dataset


class Cmip6Dataset(Dataset):
    model_id: str
    ssp_id: str
    variable_id: str
    table_id: str

    @staticmethod
    def download_with(
        download_path: Path,
        model_id: str,
        ssp_id: str,
        variable_id: str,
        table_id: str,
        variable_selector: Optional[list[str]] = None,
        progress: bool = True,
    ):
        downloadfile = download_path / "download.zarr"
        donefile = downloadfile.parent / (downloadfile.name + ".done")
        if donefile.exists():
            return

        df = Cmip6Dataset.get_stores()

        df_ta = df.query(
            f"variable_id == '{variable_id}' & experiment_id == '{ssp_id}' & "
            f"source_id == '{model_id}' & table_id == '{table_id}'"
        )

        zstore = df_ta.zstore.values[-1]
        zstore = zstore.replace("gs://", "https://storage.googleapis.com/")

        ds = xr.open_zarr(fsspec.get_mapper(zstore), consolidated=True)
        # Only select the year 2020 for the dataset.
        # .chunk(-1) ensures that we only use a single chunk for the entire dataset.
        ds = ds.sel(time=slice("2020", "2020")).chunk(-1)
        if variable_selector is not None:
            ds = ds[variable_selector]

        with monitor.progress_bar(progress):
            ds.to_zarr(downloadfile, mode="w", encoding=dict(), compute=False).compute()

        donefile.touch()

    @staticmethod
    def open(download_path: Path) -> xr.Dataset:
        return xr.open_zarr(download_path / "download.zarr")

    @lru_cache
    @staticmethod
    def get_stores() -> pd.DataFrame:
        return pd.read_csv(
            "https://storage.googleapis.com/cmip6/cmip6-zarr-consolidated-stores.csv"
        )


class Cmip6AtmosphereDataset(Cmip6Dataset):
    variable_id = "ta"
    table_id = "Amon"


class Cmip6OceanDataset(Cmip6Dataset):
    variable_id = "tos"
    table_id = "Omon"
