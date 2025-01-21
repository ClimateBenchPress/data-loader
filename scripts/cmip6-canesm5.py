from pathlib import Path

import xarray as xr

import climatebenchpress.data_loader

CMIP6_MODEL_ID = "CanESM5"
SSP_ID = "ssp585"

name = Path(__file__).stem
path = climatebenchpress.data_loader.get_dataset_path(name)

if not path.exists():
    ds = climatebenchpress.data_loader.get_cmip6_dataset(CMIP6_MODEL_ID, SSP_ID)
    climatebenchpress.data_loader.download_canonicalized_dataset(ds, name)

ds = xr.open_dataset(path, chunks=dict(), engine="zarr")

for v, da in ds.items():
    print(f"- {v}: {da.dims}")
