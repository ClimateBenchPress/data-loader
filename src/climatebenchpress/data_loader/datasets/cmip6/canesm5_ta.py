__all__ = ["Cmip6TemperatureCanEsm5Dataset"]

import xarray as xr

from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Cmip6Dataset, Cmip6TemperatureDataset


class Cmip6TemperatureCanEsm5Dataset(Cmip6TemperatureDataset):
    name = "cmip6-canesm5-ta"

    model_id = "CanESM5"
    ssp_id = "ssp585"

    @staticmethod
    def open() -> xr.Dataset:
        return Cmip6Dataset.open_with(
            Cmip6TemperatureCanEsm5Dataset.model_id,
            Cmip6TemperatureCanEsm5Dataset.ssp_id,
        )


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6TemperatureCanEsm5Dataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6TemperatureCanEsm5Dataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
