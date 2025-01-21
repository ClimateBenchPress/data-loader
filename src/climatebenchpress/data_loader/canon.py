__all__ = [
    "canonicalize_dataset",
    "canonicalize_variable",
]

import xarray as xr

from . import cf


def _ensure_coordinate(da: xr.DataArray, c: str) -> tuple[xr.DataArray, str]:
    if c in da.cf.coordinates:
        return (da, da.cf[c].name)

    da2 = da.expand_dims(c)
    da2[c].attrs.update(cf._ATTRS[c])

    return (da2, c)


def canonicalize_variable(da: xr.DataArray) -> xr.DataArray:
    da, realization = _ensure_coordinate(da, "realization")
    da, time = _ensure_coordinate(da, "time")
    da, vertical = _ensure_coordinate(da, "vertical")
    da, latitude = _ensure_coordinate(da, "latitude")
    da, longitude = _ensure_coordinate(da, "longitude")

    return da.transpose(realization, time, vertical, latitude, longitude)


def canonicalize_dataset(ds: xr.Dataset):
    ds_new = {v: canonicalize_variable(da) for v, da in ds.items()}

    return xr.Dataset(ds_new, coords=ds.coords, attrs=ds.attrs)
