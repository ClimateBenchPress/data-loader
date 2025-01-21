__all__ = ["Cmip6AccessDataset"]

import xarray as xr

from .abc import Cmip6Dataset
from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)


class Cmip6AccessDataset(Cmip6Dataset):
    name = "cmip6-access"

    model_id = "ACCESS-ESM1-5"
    ssp_io = "ssp585"

    @staticmethod
    def open() -> xr.Dataset:
        return Cmip6Dataset.open_with(
            Cmip6AccessDataset.model_id, Cmip6AccessDataset.ssp_id
        )


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6AccessDataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6AccessDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
