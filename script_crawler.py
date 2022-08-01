"""TODO: Add description."""
import json
from os.path import exists

from typing import List, Set

from crawler.binance.web_crawler import get_bases, get_list_of_prefixes
from crawler.binance.process_file_input import (
    get_all_zip_files,
    get_triangular_zip_files,
)

LIST_OF_PREFIXES_PATH: str = "./list_of_prefixes"
LIST_OF_RESOURCES_PATH: str = "./list_of_resources"
LIST_OF_BASES_PATH: str = "./list_of_bases"
LIST_OF_TRIANGLES_PATH: str = "./list_of_triangles"
TRIANGLES_PATH: str = "./triangles_path.json"

lop: List[str] = []
lor: Set[str] = set()
lob: Set[str] = set()
lot: dict = {}

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
    lor = get_all_zip_files(lop)
    # Write the resource paths to a file
    with open(LIST_OF_RESOURCES_PATH, "w", encoding="UTF-8") as file:
        for resource in lor:
            file.write(resource + "\n")
else:
    with open(LIST_OF_RESOURCES_PATH, "r", encoding="UTF-8") as file:
        for entry in file.readlines():
            lor.add(entry.replace("\n", ""))


if not exists(LIST_OF_BASES_PATH):
    # Get all bases
    lob = get_bases()
    # Write the base tokens to a file
    with open(LIST_OF_BASES_PATH, "w", encoding="UTF-8") as file:
        for resource in lob:
            file.write(resource + "\n")
else:
    with open(LIST_OF_BASES_PATH, "r", encoding="UTF-8") as file:
        for entry in file.readlines():
            lob.add(entry.replace("\n", ""))


list_of_triangles_exists: bool = exists(LIST_OF_TRIANGLES_PATH)
triangles_exist: bool = exists(TRIANGLES_PATH)
if not list_of_triangles_exists or not triangles_exist:
    # Get all bases
    lot = get_triangular_zip_files(lob, lop)
    # Write the base tokens to a file
    if not list_of_triangles_exists:
        with open(LIST_OF_TRIANGLES_PATH, "w", encoding="UTF-8") as file:
            for resource in lot["valid_path_set"]:
                file.write(resource + "\n")
    if not triangles_exist:
        with open(TRIANGLES_PATH, "w", encoding="UTF-8") as file:
            json.dump(lot["valid_paths"], file, indent=2)
else:
    with open(LIST_OF_TRIANGLES_PATH, "r", encoding="UTF-8") as file:
        the_set_i_build_now: Set[str] = set()
        for entry in file.readlines():
            the_set_i_build_now.add(entry.replace("\n", ""))
        lot["valid_path_set"] = the_set_i_build_now

    with open(TRIANGLES_PATH, "r", encoding="UTF-8") as file:
        lot["valid_paths"] = json.load(file)
