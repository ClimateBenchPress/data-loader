__all__ = ["Cmip6OceanUkEsmDataset"]

from pathlib import Path

from ...cli import main
from .abc import Cmip6Dataset, Cmip6OceanDataset


class Cmip6OceanUkEsmDataset(Cmip6OceanDataset):
    name = "cmip6-ukesm-tos"

    model_id = "UKESM1-0-LL"
    ssp_id = "ssp585"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        Cmip6Dataset.download_with(
            download_path,
            Cmip6OceanUkEsmDataset.model_id,
            Cmip6OceanUkEsmDataset.ssp_id,
            Cmip6OceanUkEsmDataset.variable_id,
            Cmip6OceanUkEsmDataset.table_id,
            # Only download the actual sea surface temperature.
            variable_selector=["tos"],
            progress=progress,
        )


if __name__ == "__main__":
    main(Cmip6OceanUkEsmDataset)
