__all__ = ["Cmip6AtmosphereAccessDataset"]

from pathlib import Path

from ... import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Cmip6AtmosphereDataset, Cmip6Dataset


class Cmip6AtmosphereAccessDataset(Cmip6AtmosphereDataset):
    name = "cmip6-access-ta"

    model_id = "ACCESS-ESM1-5"
    ssp_id = "ssp585"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        Cmip6Dataset.download_with(
            download_path,
            Cmip6AtmosphereAccessDataset.model_id,
            Cmip6AtmosphereAccessDataset.ssp_id,
            Cmip6AtmosphereAccessDataset.variable_id,
            Cmip6AtmosphereAccessDataset.table_id,
            progress=progress,
        )


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(Cmip6AtmosphereAccessDataset)
    open_downloaded_tiny_canonicalized_dataset(Cmip6AtmosphereAccessDataset)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
