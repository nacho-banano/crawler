"""Script for downloading files from binance vision."""

import asyncio
import os
import sys
from json import load
from logging import getLogger, Logger
from typing import Dict, List

from crawler.binance.downloader import download

from consts import LIST_OF_TRIANGLES_PATH, DATA_DIRECTORY

# get our input file
# a location as we will ls this directory

if __name__ == "__main__":
    logger: Logger = getLogger(__name__)

    logger.debug("Creating a list of existing files ...")
    directory_contents: List[str] = os.listdir(DATA_DIRECTORY)
    logger.debug("Done")

    logger.debug("Loading the files of all files ...")
    with open(LIST_OF_TRIANGLES_PATH, "r", encoding="UTF-8") as file:
        downloader_input: Dict[str, List[str]] = load(file)
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

    try:
        logger.debug("Download started")
        asyncio.run(download(downloader_input, DATA_DIRECTORY))
    except KeyboardInterrupt:
        print("\nDownload interrupted by user.")
        sys.exit(1)
