"""TODO: Add description."""

import os
from logging import Logger, getLogger
from tempfile import NamedTemporaryFile
from typing import List
from zipfile import ZipFile

import requests


# class Downloader:
#     """TODO: Add description."""

URL_BINANCE: str = "https://data.binance.vision"

# def __init__(self) -> None:
#     """_summary_."""
#     self._logger: Logger = getLogger(self.__class__)

logger: Logger = getLogger(__name__)


def download(file_keys: List[str], output_dir: str):
    """Download all 1 minute klines from Binance's archive."""
    # Ensure the output directory is present
    os.makedirs(output_dir, exist_ok=True)

    for key in file_keys:
        url: str = "/".join((URL_BINANCE, key))
        response = requests.get(url)

        if not response.ok:
            logger.error(
                "Failed to download %s. Error: %s", key, response.text
            )
            continue

        with NamedTemporaryFile("wb") as tmp_file:
            tmp_file.write(response.content)
            logger.info("Downloaded %s", key)

            with ZipFile(tmp_file.name, "r") as zip_archive:
                basename: str = os.path.basename(key).replace(".zip", ".csv")
                zip_archive.extract(basename, output_dir)
                logger.info("Unzipped %s to %s/%s", key, output_dir, basename)
