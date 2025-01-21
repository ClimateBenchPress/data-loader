__all__ = ["Cmip6UkEsmDataset"]

import xarray as xr

from .abc import Cmip6Dataset
from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)


class Cmip6UkEsmDataset(Cmip6Dataset):
    name = "cmip6-ukesm"

    model_id = "UKESM1-0-LL"
    ssp_id = "ssp585"

    @staticmethod
    def open() -> xr.Dataset:
        return Cmip6Dataset.open_with(
            Cmip6UkEsmDataset.model_id, Cmip6UkEsmDataset.ssp_id
        )


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6UkEsmDataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6UkEsmDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
