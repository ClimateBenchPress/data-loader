import cf_xarray  # noqa: F401
import xarray as xr


def _ensure_coordinate(da: xr.DataArray, c: str) -> tuple[xr.DataArray, str]:
    if c in da.cf.coordinates:
        return (da, da.cf[c].name)
    return (da.expand_dims(c), c)


def canonicalize_variable(da: xr.DataArray) -> xr.DataArray:
    da, time = _ensure_coordinate(da, "time")
    da, longitude = _ensure_coordinate(da, "longitude")
    da, latitude = _ensure_coordinate(da, "latitude")
    da, vertical = _ensure_coordinate(da, "vertical")

    return da.transpose(time, longitude, latitude, vertical)


def canonicalize_dataset(ds: xr.Dataset):
    ds_new = {v: canonicalize_variable(da) for v, da in ds.items()}

    return xr.Dataset(ds_new, coords=ds.coords, attrs=ds.attrs)
