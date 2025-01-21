__all__ = ["Cmip6CanEsm5Dataset"]

import xarray as xr

from .abc import Cmip6Dataset
from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)

CMIP6_MODEL_ID = "CanESM5"
SSP_ID = "ssp585"


class Cmip6CanEsm5Dataset(Cmip6Dataset):
    name = "cmip6-canesm5"

    @staticmethod
    def open() -> xr.Dataset:
        return Cmip6Dataset.open_with(CMIP6_MODEL_ID, SSP_ID)


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6CanEsm5Dataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6CanEsm5Dataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
