__all__ = ["Cmip6AtmosphereCanEsm5Dataset"]

import xarray as xr

from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Cmip6AtmosphereDataset, Cmip6Dataset


class Cmip6AtmosphereCanEsm5Dataset(Cmip6AtmosphereDataset):
    name = "cmip6-canesm5-ta"

    model_id = "CanESM5"
    ssp_id = "ssp585"

    @staticmethod
    def open() -> xr.Dataset:
        return Cmip6Dataset.open_with(
            Cmip6AtmosphereCanEsm5Dataset.model_id,
            Cmip6AtmosphereCanEsm5Dataset.ssp_id,
            Cmip6AtmosphereCanEsm5Dataset.variable_id,
            Cmip6AtmosphereCanEsm5Dataset.table_id,
        )


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6AtmosphereCanEsm5Dataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6AtmosphereCanEsm5Dataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
