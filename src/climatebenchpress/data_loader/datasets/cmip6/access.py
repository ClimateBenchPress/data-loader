__all__ = ["Cmip6AccessDataset"]

import xarray as xr

from .abc import Cmip6Dataset
from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)

CMIP6_MODEL_ID = "ACCESS-ESM1-5"
SSP_ID = "ssp585"


class Cmip6AccessDataset(Cmip6Dataset):
    name = "cmip6-access"

    @staticmethod
    def open() -> xr.Dataset:
        return Cmip6Dataset.open_with(CMIP6_MODEL_ID, SSP_ID)


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6AccessDataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6AccessDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
