__all__ = [
    "canonicalize_dataset",
    "canonicalize_variable",
    "canonical_tiny_dataset",
    "canonical_tiny_variable",
]

from typing import Optional
import xarray as xr

from . import cf


def _ensure_coordinate(da: xr.DataArray, c: str) -> tuple[xr.DataArray, str]:
    if c in da.cf.coordinates:
        return (da, da.cf[c].name)

    da2 = da.expand_dims(c)
    da2[c].attrs.update(cf._ATTRS[c])

    return (da2, c)


def canonicalize_variable(da: xr.DataArray) -> xr.DataArray:
    if len(da.dims) == 0:
        return da

    da_old = da.copy(deep=False)

    da, realization = _ensure_coordinate(da, "realization")
    da, time = _ensure_coordinate(da, "time")
    da, vertical = _ensure_coordinate(da, "vertical")
    da, latitude = _ensure_coordinate(da, "latitude")
    da, longitude = _ensure_coordinate(da, "longitude")

    new_dims = [realization, time, vertical, latitude, longitude]

    if not all(d in new_dims for d in da.dims):
        return da_old

    return da.transpose(*new_dims)


def canonicalize_dataset(ds: xr.Dataset):
    ds_new = {v: canonicalize_variable(da) for v, da in ds.items()}

    return xr.Dataset(ds_new, coords=ds.coords, attrs=ds.attrs)


def canonical_tiny_variable(
    da: xr.DataArray, slices: Optional[dict[str, slice]] = None
) -> xr.DataArray:
    if slices is None:
        slices = _TINY_SLICES

    return da.isel(
        {
            da.cf[coord].name: slice_
            for coord, slice_ in slices.items()
            if coord in da.cf.coordinates
        }
    )


def canonical_tiny_dataset(
    ds: xr.Dataset, slices: Optional[dict[str, slice]] = None
) -> xr.Dataset:
    if slices is None:
        slices = _TINY_SLICES

    # we need to slice the Dataset instead of the individual
    #  variables to ensure that the coordinates are adjusted
    return ds.isel(
        {
            ds.cf[coord].name: slice_
            for coord, slice_ in slices.items()
            if coord in ds.cf.coordinates
        }
    )


_TINY_SLICES: dict[str, slice] = dict(
    realization=slice(0, 1),
    time=slice(0, 4),
    vertical=slice(0, 4),
    latitude=slice(None),
    longitude=slice(None),
)
