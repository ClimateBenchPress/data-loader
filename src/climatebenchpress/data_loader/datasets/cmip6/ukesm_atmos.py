__all__ = ["Cmip6AtmosphereUkEsmDataset"]

from pathlib import Path

from ...cli import main
from .abc import Cmip6AtmosphereDataset, Cmip6Dataset


class Cmip6AtmosphereUkEsmDataset(Cmip6AtmosphereDataset):
    name = "cmip6-ukesm-ta"

    model_id = "UKESM1-0-LL"
    ssp_id = "ssp585"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        Cmip6Dataset.download_with(
            download_path,
            Cmip6AtmosphereUkEsmDataset.model_id,
            Cmip6AtmosphereUkEsmDataset.ssp_id,
            Cmip6AtmosphereUkEsmDataset.variable_id,
            Cmip6AtmosphereUkEsmDataset.table_id,
            progress=progress,
        )


if __name__ == "__main__":
    main(Cmip6AtmosphereUkEsmDataset)
