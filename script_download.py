"""Script for downloading files from binance vision."""

import argparse
import asyncio
import os
import sys
from json import load
from logging import getLogger, Logger
from typing import Collection, Dict, List, Literal

from crawler.binance.downloader import download

from consts import LIST_OF_TRIANGLES_PATH, DATA_DIRECTORY

logger: Logger = getLogger(__name__)


async def download_cli(downloader_input: Collection[str]) -> None:
    """
    Download the binance data.

    This is a wrapper around the ``download`` function that allows filtering
    of the desired coin pairings from the total of pairs that are due to be
    downloaded.

    Parameters
    ----------
    downloader_input : Collection[str]
        TODO: Add description.
    """
    logger.debug("Creating a list of existing files ...")
    directory_contents: List[str] = os.listdir(DATA_DIRECTORY)
    logger.debug("Done")

    logger.debug(
        "Comparing the list of existing files against the full list ..."
    )
    for pair, paths in downloader_input.items():
        downloader_input[pair] = [
            entry
            for entry in paths
            if os.path.basename(entry).replace(".zip", ".csv")
            not in directory_contents
        ]
    logger.debug("List generated. Ready to start downloading.")

    logger.debug("Download started")
    await download(downloader_input, DATA_DIRECTORY)
    logger.debug("Download finished")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Binance data downloader")

    parser.add_argument(
        "downloader_input",
        action="store",
        type=str,
        nargs="+",
    )
    parser.add_argument("-d", "--daily", action="store_true")

    namespace = parser.parse_args()
    freq: Literal["daily", "monthly"] = (
        "daily" if namespace.daily else "monthly"
    )
    path_to_list_of_triangles: str = LIST_OF_TRIANGLES_PATH.format(
        frequency=freq
    )

    logger.debug("Loading the list of all files ...")
    with open(path_to_list_of_triangles, "r", encoding="UTF-8") as file:
        pairs_list: Dict[str, List[str]] = load(file)
        logger.debug("Done")

    if namespace.downloader_input:
        pairs_list = {
            key: value
            for key, value in pairs_list.items()
            if key in namespace.downloader_input
        }

    try:
        asyncio.run(download_cli(pairs_list))
    except KeyboardInterrupt:
        print("\nDownload interrupted by user.")
        sys.exit(1)
