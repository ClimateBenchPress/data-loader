__all__ = ["Cmip6UkEsmDataset"]

import xarray as xr

from .abc import Cmip6Dataset
from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)

CMIP6_MODEL_ID = "UKESM1-0-LL"
SSP_ID = "ssp585"


class Cmip6UkEsmDataset(Cmip6Dataset):
    name = "cmip6-ukesm"

    @staticmethod
    def open() -> xr.Dataset:
        return Cmip6Dataset.open_with(CMIP6_MODEL_ID, SSP_ID)


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6UkEsmDataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6UkEsmDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
