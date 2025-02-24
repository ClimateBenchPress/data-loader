__all__ = [
    "canonicalize_dataset",
    "canonicalize_variable",
    "canonical_tiny_dataset",
    "canonical_tiny_variable",
]

from typing import Optional

import xarray as xr

from . import cf


def _ensure_axis(da: xr.DataArray, c: str) -> tuple[xr.DataArray, str]:
    if c in da.cf.axes:
        return (da, da.cf[c].name)

    da2 = da.expand_dims(c)
    da2[c].attrs.update(cf._ATTRS[c])

    return (da2, c)


def canonicalize_variable(da: xr.DataArray) -> xr.DataArray:
    da, realization = _ensure_axis(da, "E")
    da, time = _ensure_axis(da, "T")
    da, vertical = _ensure_axis(da, "Z")
    da, latitude = _ensure_axis(da, "Y")
    da, longitude = _ensure_axis(da, "X")

    return da.transpose(realization, time, vertical, latitude, longitude)


def canonicalize_dataset(ds: xr.Dataset):
    ds_new = {v: canonicalize_variable(da) for v, da in ds.items()}

    return xr.Dataset(ds_new, coords=ds.coords, attrs=ds.attrs)


def canonical_tiny_variable(
    da: xr.DataArray, slices: Optional[dict[str, slice]] = None
) -> xr.DataArray:
    if slices is None:
        slices = _TINY_SLICES

    return da.isel(
        {da.cf[ax].name: slice_ for ax, slice_ in slices.items() if ax in da.cf.axes}
    )


def canonical_tiny_dataset(
    ds: xr.Dataset, slices: Optional[dict[str, slice]] = None
) -> xr.Dataset:
    if slices is None:
        slices = _TINY_SLICES

    # we need to slice the Dataset instead of the individual
    # variables to ensure that the coordinates are adjusted
    return ds.isel(
        {ds.cf[ax].name: slice_ for ax, slice_ in slices.items() if ax in ds.cf.axes}
    )


_TINY_SLICES: dict[str, slice] = dict(
    E=slice(0, 1),
    T=slice(0, 4),
    Z=slice(0, 4),
    Y=slice(None),
    X=slice(None),
)
