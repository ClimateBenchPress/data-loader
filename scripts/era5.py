from pathlib import Path

import xarray as xr

import climatebenchpress.data_loader

ERA5_GCP_PATH = (
    "gs://gcp-public-data-arco-era5/ar/1959-2022-full_37-1h-0p25deg-chunk-1.zarr-v2"
)

name = Path(__file__).stem
path = climatebenchpress.data_loader.get_dataset_path(name)

if not path.exists():
    era5 = xr.open_zarr(ERA5_GCP_PATH, chunks={"time": 48}, consolidated=True)
    ds = era5.sel(time=slice("2020-03-01", "2020-03-07"))[
        [
            "mean_sea_level_pressure",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
        ]
    ]
    # Needed to make the dataset CF-compliant.
    ds.time.attrs["standard_name"] = "time"
    climatebenchpress.data_loader.download_canonicalized_dataset(ds, name)

ds = xr.open_dataset(path, chunks=dict(), engine="zarr")

for v, da in ds.items():
    print(f"- {v}: {da.dims}")
