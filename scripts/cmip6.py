from pathlib import Path

import climatebenchpress.data_loader
import fsspec
import pandas as pd
import xarray as xr


name = Path(__file__).stem
path = climatebenchpress.data_loader.get_dataset_path(name)

if not path.exists():
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

    climatebenchpress.data_loader.download_canonicalized_dataset(ds, name)

ds = xr.open_dataset(path, chunks=dict())

for v, da in ds.items():
    print(f"- {v}: {da.dims}")
