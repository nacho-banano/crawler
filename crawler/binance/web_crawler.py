from warnings import catch_warnings, simplefilter
from typing import List
from time import time

import requests
from bs4 import BeautifulSoup, Tag

URL_S3: str = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"


class BeautifulSoupProxy(BeautifulSoup):
    """TODO: Add class description."""

    def __init__(self, *args, **kwargs) -> None:
        """Instantiate ``BeautifulSoup`` and suppress the parsing warning."""
        with catch_warnings():
            simplefilter("ignore")
            super().__init__(*args, **kwargs)

    def insert_before(self, *args) -> None:
        raise NotImplementedError()

    def insert_after(self, *args) -> None:
        raise NotImplementedError()


def extract_prefix(document: BeautifulSoup) -> List[str]:
    """
    Extract prefix tag from a parsed list of Binance S3 blobs.

    Parameters
    ----------
    document : BeautifulSoup
        A data structure representing a parsed HTML or XML document.

    Returns
    -------
    List[str]
    """
    return [prefix.get_text() for prefix in document.find_all("prefix")][1:]


def extract_key(document: BeautifulSoup) -> List[str]:
    """
    Extract key tag from a parsed list of Binance S3 blobs.

    Parameters
    ----------
    document : BeautifulSoup
        A data structure representing a parsed HTML or XML document.

    Returns
    -------
    List[str]
    """
    return [
        key.get_text()
        for key in document.find_all("key")
        if not key.get_text().endswith(".CHECKSUM")
    ]


def list_of_files() -> List[str]:
    list_of_keys: List[str] = []

    parameters: dict = {
        "delimiter": "/",
        "prefix": "data/spot/daily/klines/",
        "marker": "data/spot/daily/klines/",
    }
    headers: dict = {"Content-Type": "application/xml"}
    response: requests.Response = requests.get(
        URL_S3, params=parameters, headers=headers
    )

    soup = BeautifulSoupProxy(response.text, features="lxml")
    prefixes: List[str] = extract_prefix(soup)
    next_marker: Tag = soup.find("nextmarker")

    while next_marker:
        parameters["marker"] = next_marker.get_text()
        response = requests.get(URL_S3, params=parameters, headers=headers)
        soup = BeautifulSoupProxy(response.text, features="lxml")
        next_prefixes: List[str] = extract_prefix(soup)
        print(f"extending prefix {len(next_prefixes)}")
        prefixes.extend(next_prefixes)
        next_marker: str = soup.find("nextmarker")  # re-set the ``next_marker``

    for prefix in prefixes:
        parameters["prefix"] = f"{prefix}1m/"
        parameters["marker"] = "data/spot/daily/klines/"
        t_0 = time()
        response = requests.get(URL_S3, params=parameters, headers=headers)
        print("response time:", time() - t_0)
        t_1 = time()
        soup = BeautifulSoupProxy(response.text, features="lxml")
        print("soup time:", time() - t_1)
        file_keys: List[str] = extract_key(soup)
        next_marker: Tag = soup.find("nextmarker")

        while next_marker:
            parameters["marker"] = next_marker.get_text()
            response = requests.get(URL_S3, params=parameters, headers=headers)
            soup = BeautifulSoupProxy(response.text, features="lxml")
            next_keys: List[str] = extract_key(soup)
            print(f"{prefix}: extended keys by {len(next_keys)}")
            file_keys.extend(next_keys)
            next_marker: str = soup.find("nextmarker")  # re-set the ``next_marker``

        list_of_keys.extend(file_keys)

    return list_of_keys
