__all__ = ["_download_netcdf"]

import logging
from pathlib import Path

import requests
from tqdm import tqdm


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
