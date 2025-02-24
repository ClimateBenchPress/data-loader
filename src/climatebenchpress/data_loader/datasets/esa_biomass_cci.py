__all__ = ["EsaBiomassCciDataset"]

import kerchunk.hdf
import xarray as xr

from .abc import Dataset
from .. import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)


class EsaBiomassCciDataset(Dataset):
    name = "esa-biomass-cci"

    @staticmethod
    def open() -> xr.Dataset:
        urls = [
            f"https://dap.ceda.ac.uk/neodc/esacci/biomass/data/agb/maps/v5.01/netcdf/ESACCI-BIOMASS-L4-AGB-MERGED-100m-{year}-fv5.01.nc"
            for year in [2010, 2015, 2016, 2017, 2018, 2019, 2020, 2021]
        ]

        kcs = [
            kerchunk.hdf.SingleHdf5ToZarr(
                url,
                inline_threshold=0,
                error="raise",
            ).translate()
            for url in urls
        ]

        dss = [
            xr.open_dataset(
                "reference://",
                engine="zarr",
                backend_kwargs=dict(
                    storage_options=dict(fo=kc),
                ),
                consolidated=False,
                chunks=dict(),
            )
            for kc in kcs
        ]

        return xr.concat(dss, dim="time")


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(EsaBiomassCciDataset)
    open_downloaded_tiny_canonicalized_dataset(EsaBiomassCciDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
