__all__ = ["Cmip6OceanAccessDataset"]

from pathlib import Path

from ...cli import main
from .abc import Cmip6Dataset, Cmip6OceanDataset


class Cmip6OceanAccessDataset(Cmip6OceanDataset):
    """Sea surface temperature dataset from the CMIP6 ACCESS-ESM1-5 model under the SSP585 scenario."""

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
    main(Cmip6OceanAccessDataset)
