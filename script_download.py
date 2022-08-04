"""Script for downloading files from binance vision."""

import os
from typing import List

from crawler.binance.downloader import download

from consts import LIST_OF_TRIANGLES_PATH, DATA_DIRECTORY

# get our input file
# a location as we will ls this directory

directory_contents: List[str] = os.listdir(DATA_DIRECTORY)

with open(LIST_OF_TRIANGLES_PATH, "r", encoding="UTF-8") as file:
    downloader_input: List[str] = file.read().split("\n")

downloader_input = [
    entry
    for entry in downloader_input
    if os.path.basename(entry).replace(".zip", ".csv")
    not in directory_contents
]

download(downloader_input, DATA_DIRECTORY)
