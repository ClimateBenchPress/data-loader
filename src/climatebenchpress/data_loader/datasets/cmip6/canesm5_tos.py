__all__ = ["Cmip6SeaSurfaceTemperatureCanEsm5Dataset"]

import xarray as xr

from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Cmip6Dataset, Cmip6SeaSurfaceTemperatureDataset


class Cmip6SeaSurfaceTemperatureCanEsm5Dataset(Cmip6SeaSurfaceTemperatureDataset):
    name = "cmip6-canesm5-tos"

    model_id = "CanESM5"
    ssp_id = "ssp585"

    @staticmethod
    def open() -> xr.Dataset:
        ds = Cmip6Dataset.open_with(
            Cmip6SeaSurfaceTemperatureCanEsm5Dataset.model_id,
            Cmip6SeaSurfaceTemperatureCanEsm5Dataset.ssp_id,
            Cmip6SeaSurfaceTemperatureCanEsm5Dataset.variable_id,
            Cmip6SeaSurfaceTemperatureCanEsm5Dataset.table_id,
        )
        return ds[["tos"]]


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6SeaSurfaceTemperatureCanEsm5Dataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6SeaSurfaceTemperatureCanEsm5Dataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
