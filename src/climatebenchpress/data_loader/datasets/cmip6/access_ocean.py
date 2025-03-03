__all__ = ["Cmip6OceanAccessDataset"]

from pathlib import Path

from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Cmip6Dataset, Cmip6OceanDataset


class Cmip6OceanAccessDataset(Cmip6OceanDataset):
    name = "cmip6-access-tos"

    model_id = "ACCESS-ESM1-5"
    ssp_id = "ssp585"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        Cmip6Dataset.download_with(
            download_path,
            Cmip6OceanAccessDataset.model_id,
            Cmip6OceanAccessDataset.ssp_id,
            Cmip6OceanAccessDataset.variable_id,
            Cmip6OceanAccessDataset.table_id,
            # Only download the actual sea surface temperature.
            variable_selector=["tos"],
            progress=progress,
        )


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6OceanAccessDataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6OceanAccessDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
