"""Update list of triangles."""

import argparse
import asyncio
import json
from typing import List, Literal, Set

from crawler.binance.web_crawler import get_bases, get_list_of_prefixes
from crawler.binance.process_file_input import Processing
from consts import LIST_OF_TRIANGLES_PATH


async def crawl(frequency: Literal["daily", "monthly"]):
    """_summary_."""
    list_of_bases: Set[str] = set()
    list_of_prefixes: List[str] = []
    list_of_triangles: dict = {}

    list_of_bases = await get_bases()
    list_of_prefixes = await get_list_of_prefixes()

    list_of_triangles = Processing(
        list_of_bases, list_of_prefixes, frequency
    ).get_triangular_zip_files()

    # Write the base tokens to a file
    with open(
        LIST_OF_TRIANGLES_PATH.format(frequency=frequency),
        "w",
        encoding="UTF-8",
    ) as file:
        json.dump(list_of_triangles, file, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Binance Vision web crawler")

    parser.add_argument("-d", "--daily", action="store_true")

    namespace = parser.parse_args()
    freq: Literal["daily", "monthly"] = (
        "daily" if namespace.daily else "monthly"
    )

    asyncio.run(crawl(freq))
