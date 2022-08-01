"""TODO: Add description."""
from os.path import exists

from typing import List, Set

from crawler.binance.web_crawler import get_list_of_prefixes
from crawler.binance.process_file_input import get_all_files

LIST_OF_PREFIXES_PATH: str = "./list_of_prefixes"
LIST_OF_RESOURCES_PATH: str = "./list_of_resources"

lop: List[str] = []
lor: Set[str] = set()

if not exists(LIST_OF_PREFIXES_PATH):
    # Crawl the web page and create file prefixes for each available pairing
    lop = get_list_of_prefixes()
    # Write the prefixes to a file
    with open(LIST_OF_PREFIXES_PATH, "w", encoding="UTF-8") as file:
        for prefix in lop:
            file.write(prefix + "\n")
else:
    with open(LIST_OF_PREFIXES_PATH, "r", encoding="UTF-8") as file:
        for entry in file.readlines():
            lop.append(entry.replace("\n", ""))

if not exists(LIST_OF_RESOURCES_PATH):
    # Get all file resources for downloading
    lor = get_all_files(lop)
    # Write the resource paths to a file
    with open(LIST_OF_RESOURCES_PATH, "w", encoding="UTF-8") as file:
        for resource in lor:
            file.write(resource + "\n")
else:
    with open(LIST_OF_RESOURCES_PATH, "r", encoding="UTF-8") as file:
        for entry in file.readlines():
            lor.add(entry.replace("\n", ""))
