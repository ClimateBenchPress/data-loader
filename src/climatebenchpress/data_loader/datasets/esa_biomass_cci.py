__all__ = ["EsaBiomassCciDataset"]

import logging
from pathlib import Path

import requests
import xarray as xr
from tqdm import tqdm

from .. import (
    open_downloaded_canonicalized_dataset,
    open_downloaded_tiny_canonicalized_dataset,
)
from .abc import Dataset

NUM_RETRIES = 3
# Approximate bounding box for mainland France
FRANCE_BBOX = {"X": slice(196313, 213750), "Y": slice(32063, 43875)}


class EsaBiomassCciDataset(Dataset):
    name = "esa-biomass-cci-test"

    @staticmethod
    def download(download_path: Path, progress: bool = True):
        urls = [
            f"https://dap.ceda.ac.uk/neodc/esacci/biomass/data/agb/maps/v5.01/netcdf/ESACCI-BIOMASS-L4-AGB-MERGED-100m-{year}-fv5.01.nc"
            # Restrict to 2 years for now for smaller download.
            for year in [2010, 2015]
        ]
        for url in urls:
            output_path = download_path / Path(url).name
            for _ in range(NUM_RETRIES):
                success = _download_netcdf(url, output_path, progress)
                if success:
                    break
            if not success:
                logging.info(f"Failed to download {url}")
                return

    @staticmethod
    def open(download_path: Path) -> xr.Dataset:
        # Need string conversion for argument to be interpreted as a glob pattern.
        ds = xr.open_mfdataset(str(download_path / "*.nc"))
        # Needed to make the dataset CF-compliant.
        ds.lon.attrs["axis"] = "X"
        ds.lat.attrs["axis"] = "Y"
        return ds


def _download_netcdf(
    url: str, output_path: Path, progress: bool, chunk_size=8192
) -> bool:
    """
    Download a large NetCDF file from a given URL. Ensures that download can be
    resumed if it is interrupted due to network failures.

    Args:
        url (str): URL of the NetCDF file
        output_path (str): Local path to save the file
        chunk_size (int): Size of chunks to download at a time in bytes

    Returns:
        bool: True if download was successful, False otherwise
    """
    donefile = output_path.with_name(output_path.name + ".done")
    if donefile.exists():
        logging.debug(f"File already downloaded: {output_path}")
        return True

    session = requests.Session()

    # Check if file exists and get its size for resume capability
    file_size = 0
    headers = {}
    if output_path.exists():
        file_size = output_path.stat().st_size
        headers["Range"] = f"bytes={file_size}-"

    try:
        response = session.get(url, headers=headers, stream=True, timeout=30)

        # Handle resume or new download
        if file_size > 0 and response.status_code == 206:
            mode = "ab"  # Append in binary mode
        else:
            mode = "wb"  # Write in binary mode
            file_size = 0

        total_size = int(response.headers.get("content-length", 0)) + file_size

        logging.debug(f"Downloading {url} to {output_path} in mode '{mode}'")
        logging.debug(f"File size: {total_size / 1e6:.2f} MB")

        with open(output_path, mode) as f:
            with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc=output_path.name,
                initial=file_size,
                ascii=True,
            ) as pbar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))

            logging.debug("\nDownload completed!")

        donefile.touch()

    except (requests.exceptions.RequestException, IOError) as e:
        logging.error(f"An error occurred: {e}")
        logging.error("You can resume the download by running the script again.")
        return False

    return True


if __name__ == "__main__":
    ds = open_downloaded_canonicalized_dataset(EsaBiomassCciDataset)
    open_downloaded_tiny_canonicalized_dataset(EsaBiomassCciDataset, slices=FRANCE_BBOX)

    for v, da in ds.items():
        print(f"- {v}: {da.dims}")
