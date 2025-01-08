from pathlib import Path

import climatebenchpress.data_loader
import fsspec
import pandas as pd
import xarray as xr

from dask.diagnostics import ProgressBar


download = Path("download.zarr")
if not download.exists():
    df = pd.read_csv(
        "https://storage.googleapis.com/cmip6/cmip6-zarr-consolidated-stores.csv"
    )

    ssp_test = "ssp585"
    model = "ACCESS-ESM1-5"

    df_ta = df.query(
        f"variable_id == 'ta' & experiment_id == '{ssp_test}' & "
        f"source_id == '{model}' & table_id == 'Amon'"
    )

    zstore = df_ta.zstore.values[-1]
    mapper = fsspec.get_mapper(zstore)

    ds = xr.open_zarr(mapper, consolidated=True)

    with ProgressBar():
        ds.to_zarr(download, encoding=dict(), compute=False).compute()

standardized = Path("standardized.zarr")
if not standardized.exists():
    ds = xr.open_dataset(download, chunks=dict())
    ds = climatebenchpress.data_loader.canonicalize_dataset(ds)

    with ProgressBar():
        ds.to_zarr(standardized, encoding=dict(), compute=False).compute()

ds = xr.open_dataset(standardized, chunks=dict())
print(ds.cf)
