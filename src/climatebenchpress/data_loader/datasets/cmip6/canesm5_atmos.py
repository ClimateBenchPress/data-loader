__all__ = ["Cmip6AtmosphereCanEsm5Dataset"]

from pathlib import Path

from ...cli import main
from .abc import Cmip6AtmosphereDataset, Cmip6Dataset


class Cmip6AtmosphereCanEsm5Dataset(Cmip6AtmosphereDataset):
    name = "cmip6-canesm5-ta"

    model_id = "CanESM5"
    ssp_id = "ssp585"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        Cmip6Dataset.download_with(
            download_path,
            Cmip6AtmosphereCanEsm5Dataset.model_id,
            Cmip6AtmosphereCanEsm5Dataset.ssp_id,
            Cmip6AtmosphereCanEsm5Dataset.variable_id,
            Cmip6AtmosphereCanEsm5Dataset.table_id,
            progress=progress,
        )


if __name__ == "__main__":
    main(Cmip6AtmosphereCanEsm5Dataset)
