__all__ = ["Cmip6AtmosphereUkEsmDataset"]

import xarray as xr

from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Cmip6AtmosphereDataset, Cmip6Dataset


class Cmip6AtmosphereUkEsmDataset(Cmip6AtmosphereDataset):
    name = "cmip6-ukesm-ta"

    model_id = "UKESM1-0-LL"
    ssp_id = "ssp585"

    @staticmethod
    def open() -> xr.Dataset:
        return Cmip6Dataset.open_with(
            Cmip6AtmosphereUkEsmDataset.model_id,
            Cmip6AtmosphereUkEsmDataset.ssp_id,
            Cmip6AtmosphereUkEsmDataset.variable_id,
            Cmip6AtmosphereUkEsmDataset.table_id,
        )


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6AtmosphereUkEsmDataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6AtmosphereUkEsmDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
