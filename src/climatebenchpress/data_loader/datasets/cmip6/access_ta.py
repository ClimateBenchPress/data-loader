__all__ = ["Cmip6TemperatureAccessDataset"]

import xarray as xr

from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Cmip6Dataset, Cmip6TemperatureDataset


class Cmip6TemperatureAccessDataset(Cmip6TemperatureDataset):
    name = "cmip6-access-ta"

    model_id = "ACCESS-ESM1-5"
    ssp_id = "ssp585"

    @staticmethod
    def open() -> xr.Dataset:
        return Cmip6Dataset.open_with(
            Cmip6TemperatureAccessDataset.model_id,
            Cmip6TemperatureAccessDataset.ssp_id,
            Cmip6TemperatureAccessDataset.variable_id,
            Cmip6TemperatureAccessDataset.table_id,
        )


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6TemperatureAccessDataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6TemperatureAccessDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
