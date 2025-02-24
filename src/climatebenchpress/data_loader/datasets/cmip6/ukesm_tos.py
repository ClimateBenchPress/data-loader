__all__ = ["Cmip6SeaSurfaceTemperatureUkEsmDataset"]

import xarray as xr

from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Cmip6Dataset, Cmip6SeaSurfaceTemperatureDataset


class Cmip6SeaSurfaceTemperatureUkEsmDataset(Cmip6SeaSurfaceTemperatureDataset):
    name = "cmip6-ukesm-tos"

    model_id = "UKESM1-0-LL"
    ssp_id = "ssp585"

    @staticmethod
    def open() -> xr.Dataset:
        ds = Cmip6Dataset.open_with(
            Cmip6SeaSurfaceTemperatureUkEsmDataset.model_id,
            Cmip6SeaSurfaceTemperatureUkEsmDataset.ssp_id,
            Cmip6SeaSurfaceTemperatureUkEsmDataset.variable_id,
            Cmip6SeaSurfaceTemperatureUkEsmDataset.table_id,
        )
        return ds[["tos"]]


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6SeaSurfaceTemperatureUkEsmDataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6SeaSurfaceTemperatureUkEsmDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
