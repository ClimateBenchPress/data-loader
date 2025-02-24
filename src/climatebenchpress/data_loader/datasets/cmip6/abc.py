__all__ = ["Cmip6Dataset, Cmip6TemperatureDataset, Cmip6SeaSurfaceTemperatureDataset"]

from functools import lru_cache

import fsspec
import pandas as pd
import xarray as xr

from ..abc import Dataset


class Cmip6Dataset(Dataset):
    model_id: str
    ssp_id: str
    variable_id: str
    table_id: str

    @staticmethod
    def open_with(
        model_id: str, ssp_id: str, variable_id: str, table_id: str
    ) -> xr.Dataset:
        df = Cmip6Dataset.get_stores()

        df_ta = df.query(
            f"variable_id == '{variable_id}' & experiment_id == '{ssp_id}' & "
            f"source_id == '{model_id}' & table_id == '{table_id}'"
        )

        zstore = df_ta.zstore.values[-1]
        zstore = zstore.replace("gs://", "https://storage.googleapis.com/")

        return xr.open_zarr(fsspec.get_mapper(zstore), consolidated=True)

    @lru_cache
    @staticmethod
    def get_stores() -> pd.DataFrame:
        return pd.read_csv(
            "https://storage.googleapis.com/cmip6/cmip6-zarr-consolidated-stores.csv"
        )


class Cmip6TemperatureDataset(Cmip6Dataset):
    variable_id = "ta"
    table_id = "Amon"


class Cmip6SeaSurfaceTemperatureDataset(Cmip6Dataset):
    variable_id = "tos"
    table_id = "Omon"
