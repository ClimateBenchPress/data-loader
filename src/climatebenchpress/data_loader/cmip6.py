import fsspec
import pandas as pd
import xarray as xr


def get_cmip6_dataset(model: str, ssp_test: str) -> xr.Dataset:
    df = pd.read_csv(
        "https://storage.googleapis.com/cmip6/cmip6-zarr-consolidated-stores.csv"
    )

    df_ta = df.query(
        f"variable_id == 'ta' & experiment_id == '{ssp_test}' & "
        f"source_id == '{model}' & table_id == 'Amon'"
    )

    zstore = df_ta.zstore.values[-1]
    return xr.open_zarr(fsspec.get_mapper(zstore), consolidated=True)
