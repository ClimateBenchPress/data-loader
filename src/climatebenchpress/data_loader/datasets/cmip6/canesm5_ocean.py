__all__ = ["Cmip6OceanCanEsm5Dataset"]

import xarray as xr

from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Cmip6Dataset, Cmip6OceanDataset


class Cmip6OceanCanEsm5Dataset(Cmip6OceanDataset):
    name = "cmip6-canesm5-tos"

    model_id = "CanESM5"
    ssp_id = "ssp585"

    @staticmethod
    def open() -> xr.Dataset:
        ds = Cmip6Dataset.open_with(
            Cmip6OceanCanEsm5Dataset.model_id,
            Cmip6OceanCanEsm5Dataset.ssp_id,
            Cmip6OceanCanEsm5Dataset.variable_id,
            Cmip6OceanCanEsm5Dataset.table_id,
        )
        return ds[["tos"]]


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6OceanCanEsm5Dataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6OceanCanEsm5Dataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
