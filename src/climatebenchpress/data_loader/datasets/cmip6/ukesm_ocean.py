__all__ = ["Cmip6OceanUkEsmDataset"]

import xarray as xr

from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Cmip6Dataset, Cmip6OceanDataset


class Cmip6OceanUkEsmDataset(Cmip6OceanDataset):
    name = "cmip6-ukesm-tos"

    model_id = "UKESM1-0-LL"
    ssp_id = "ssp585"

    @staticmethod
    def open() -> xr.Dataset:
        ds = Cmip6Dataset.open_with(
            Cmip6OceanUkEsmDataset.model_id,
            Cmip6OceanUkEsmDataset.ssp_id,
            Cmip6OceanUkEsmDataset.variable_id,
            Cmip6OceanUkEsmDataset.table_id,
        )
        return ds[["tos"]]


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6OceanUkEsmDataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6OceanUkEsmDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
