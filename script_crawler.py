"""TODO: Add description."""

import asyncio
import json
from os.path import exists
from typing import Dict, List, Set

from crawler.binance.web_crawler import get_bases, get_list_of_prefixes
from crawler.binance.process_file_input import Processing
from consts import (
    LIST_OF_PREFIXES_PATH,
    LIST_OF_RESOURCES_PATH,
    LIST_OF_BASES_PATH,
    LIST_OF_TRIANGLES_PATH,
    TRIANGLES_PATH,
)


async def crawl():
    """_summary_."""
    lop: List[str] = []
    lor: Dict[str, List[str]] = {}
    lob: Set[str] = set()
    lot: dict = {}

    if not exists(LIST_OF_BASES_PATH):
        # Get all bases
        lob = await get_bases()
        # Write the base tokens to a file
        with open(LIST_OF_BASES_PATH, "w", encoding="UTF-8") as file:
            for resource in lob:
                file.write(resource + "\n")
    else:
        with open(LIST_OF_BASES_PATH, "r", encoding="UTF-8") as file:
            for entry in file.readlines():
                lob.add(entry.replace("\n", ""))

    if not exists(LIST_OF_PREFIXES_PATH):
        # Crawl the web page and create file prefixes for each available
        # pairing
        lop = await get_list_of_prefixes()
        # Write the prefixes to a file
        with open(LIST_OF_PREFIXES_PATH, "w", encoding="UTF-8") as file:
            for prefix in lop:
                file.write(prefix + "\n")
    else:
        with open(LIST_OF_PREFIXES_PATH, "r", encoding="UTF-8") as file:
            for entry in file.readlines():
                lop.append(entry.replace("\n", ""))

    processing: Processing = Processing(lob, lop)

    if not exists(LIST_OF_RESOURCES_PATH):
        # Get all file resources for downloading
        lor = processing.get_all_zip_files()
        # Write the resource paths to a file
        with open(LIST_OF_RESOURCES_PATH, "w", encoding="UTF-8") as file:
            json.dump(lor, file, indent=2)
    # else:
    #     with open(LIST_OF_RESOURCES_PATH, "r", encoding="UTF-8") as file:
    #         lor = json.load(file)

    list_of_triangles_exists: bool = exists(LIST_OF_TRIANGLES_PATH)
    triangles_exist: bool = exists(TRIANGLES_PATH)
    if not list_of_triangles_exists or not triangles_exist:
        # Get all bases
        lot = processing.get_triangular_zip_files()

        # Write the base tokens to a file
        if not list_of_triangles_exists:
            with open(LIST_OF_TRIANGLES_PATH, "w", encoding="UTF-8") as file:
                json.dump(lot["valid_path_set"], file, indent=2)

        if not triangles_exist:
            with open(TRIANGLES_PATH, "w", encoding="UTF-8") as file:
                json.dump(lot["valid_paths"], file, indent=2)


if __name__ == "__main__":
    asyncio.run(crawl())
