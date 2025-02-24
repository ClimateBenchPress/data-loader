__all__ = ["Cmip6SeaSurfaceTemperatureAccessDataset"]

import xarray as xr

from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Cmip6Dataset, Cmip6SeaSurfaceTemperatureDataset


class Cmip6SeaSurfaceTemperatureAccessDataset(Cmip6SeaSurfaceTemperatureDataset):
    name = "cmip6-access-tos"

    model_id = "ACCESS-ESM1-5"
    ssp_id = "ssp585"

    @staticmethod
    def open() -> xr.Dataset:
        ds = Cmip6Dataset.open_with(
            Cmip6SeaSurfaceTemperatureAccessDataset.model_id,
            Cmip6SeaSurfaceTemperatureAccessDataset.ssp_id,
            Cmip6SeaSurfaceTemperatureAccessDataset.variable_id,
            Cmip6SeaSurfaceTemperatureAccessDataset.table_id,
        )
        # Only keep the actual sea surface temperature.
        return ds[["tos"]]


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6SeaSurfaceTemperatureAccessDataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6SeaSurfaceTemperatureAccessDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
