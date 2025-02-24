__all__ = ["Cmip6TemperatureUkEsmDataset"]

import xarray as xr

from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Cmip6Dataset, Cmip6TemperatureDataset


class Cmip6TemperatureUkEsmDataset(Cmip6TemperatureDataset):
    name = "cmip6-ukesm-ta"

    model_id = "UKESM1-0-LL"
    ssp_id = "ssp585"

    @staticmethod
    def open() -> xr.Dataset:
        return Cmip6Dataset.open_with(
            Cmip6TemperatureUkEsmDataset.model_id, Cmip6TemperatureUkEsmDataset.ssp_id
        )


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6TemperatureUkEsmDataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6TemperatureUkEsmDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
