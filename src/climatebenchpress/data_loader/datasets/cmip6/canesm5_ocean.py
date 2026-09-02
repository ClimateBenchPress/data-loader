__all__ = ["Cmip6OceanCanEsm5Dataset"]

from pathlib import Path

from ...cli import main
from .abc import Cmip6Dataset, Cmip6OceanDataset


class Cmip6OceanCanEsm5Dataset(Cmip6OceanDataset):
    name = "cmip6-canesm5-tos"

    model_id = "CanESM5"
    ssp_id = "ssp585"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        Cmip6Dataset.download_with(
            download_path,
            Cmip6OceanCanEsm5Dataset.model_id,
            Cmip6OceanCanEsm5Dataset.ssp_id,
            Cmip6OceanCanEsm5Dataset.variable_id,
            Cmip6OceanCanEsm5Dataset.table_id,
            # Only download the actual sea surface temperature.
            variable_selector=["tos"],
            progress=progress,
        )


if __name__ == "__main__":
    main(Cmip6OceanCanEsm5Dataset)
