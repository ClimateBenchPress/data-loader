__all__ = ["Cmip6CanEsm5Dataset"]

import xarray as xr

from .abc import Cmip6Dataset
from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)


class Cmip6CanEsm5Dataset(Cmip6Dataset):
    name = "cmip6-canesm5"

    model_id = "CanESM5"
    ssp_id = "ssp585"

    @staticmethod
    def open() -> xr.Dataset:
        return Cmip6Dataset.open_with(
            Cmip6CanEsm5Dataset.model_id, Cmip6CanEsm5Dataset.ssp_id
        )


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6CanEsm5Dataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6CanEsm5Dataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
