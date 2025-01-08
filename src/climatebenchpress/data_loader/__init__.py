import cf_xarray as cfxr
import xarray as xr


_ENSEMBLE_CRITERIA = dict(
    standard_name=("realization",),
    axis=("E",),
    cartesian_axis=("E",),
    grads_dim=("e",),
)

cfxr.set_options(
    custom_criteria=dict(
        realization=_ENSEMBLE_CRITERIA,
        E=_ENSEMBLE_CRITERIA,
    )
)


_ENSEMBLE_ATTRS = dict(
    axis="E",
    standard_name="realization",
)
_ATTRS = {**cfxr.accessor.ATTRS, **dict(realization=_ENSEMBLE_ATTRS, E=_ENSEMBLE_ATTRS)}


def _ensure_coordinate(da: xr.DataArray, c: str) -> tuple[xr.DataArray, str]:
    if c in da.cf.coordinates:
        return (da, da.cf[c].name)

    da2 = da.expand_dims(c)
    da2[c].attrs.update(_ATTRS[c])

    return (da2, c)


def canonicalize_variable(da: xr.DataArray) -> xr.DataArray:
    da, time = _ensure_coordinate(da, "time")
    da, longitude = _ensure_coordinate(da, "longitude")
    da, latitude = _ensure_coordinate(da, "latitude")
    da, vertical = _ensure_coordinate(da, "vertical")
    da, realization = _ensure_coordinate(da, "realization")

    return da.transpose(time, longitude, latitude, vertical, realization)


def canonicalize_dataset(ds: xr.Dataset):
    ds_new = {v: canonicalize_variable(da) for v, da in ds.items()}

    return xr.Dataset(ds_new, coords=ds.coords, attrs=ds.attrs)
