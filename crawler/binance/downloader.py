"""TODO: Add description."""

import os
from logging import Logger, getLogger
from tempfile import NamedTemporaryFile
from typing import Dict, List
from zipfile import BadZipFile, ZipFile

import requests


# class Downloader:
#     """TODO: Add description."""

URL_BINANCE: str = "https://data.binance.vision"

# def __init__(self) -> None:
#     """_summary_."""
#     self._logger: Logger = getLogger(self.__class__)

logger: Logger = getLogger(__name__)


def download(file_keys: Dict[str, List[str]], output_dir: str) -> None:
    """
    Download all 1 minute klines from Binance's archive.

    Parameters
    ----------
    file_keys : Dict[str, List[str]]
        _description_
    output_dir : str
        _description_
    """
    # Ensure the output directory is present
    os.makedirs(output_dir, exist_ok=True)

    for paths in file_keys.values():
        for key in paths:
            url: str = "/".join((URL_BINANCE, key))
            response = requests.get(url)

            if not response.ok:
                logger.error(response.text)
                break

            with NamedTemporaryFile("wb") as tmp_file:
                tmp_file.write(response.content)
                logger.info("Downloaded %s", key)

                with ZipFile(tmp_file.name, "r") as zip_archive:
                    basename: str = os.path.basename(key).replace(
                        ".zip", ".csv"
                    )
                    try:
                        zip_archive.extract(basename, output_dir)
                    except BadZipFile:
                        logger.error(
                            "%s not in zip archive", basename, exc_info=True
                        )
                    else:
                        logger.info(
                            "Unzipped %s to %s/%s", key, output_dir, basename
                        )
