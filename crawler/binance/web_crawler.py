"""
Binance web-crawler.

Build a list of the Binance archive for 1 minute kline data.
"""

__author__ = "Nacho Banana"

from datetime import datetime, timedelta
from warnings import catch_warnings, simplefilter
from typing import List, Set
import json

import requests
from bs4 import BeautifulSoup, Tag

URL_S3: str = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
PREFIX: str = "data/spot/daily/klines/"
INTERVAL: str = "1m"
START_DATE: datetime = datetime(2021, 3, 1)

URL_BINANCE_EXCHANGE_INFO: str = "https://api.binance.com/api/v3/exchangeInfo"


class BeautifulSoupProxy(BeautifulSoup):
    """TODO: Add class description."""

    def __init__(self, *args, **kwargs) -> None:
        """Instantiate ``BeautifulSoup`` and suppress the parsing warning."""
        with catch_warnings():
            simplefilter("ignore")
            super().__init__(*args, **kwargs)

    def insert_before(self, *args) -> None:
        """For surpressing stuff."""
        raise NotImplementedError()

    def insert_after(self, *args) -> None:
        """For surpressing stuff."""
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


def extract_key(pair: str) -> List[str]:
    """
    Generate a list of keys for the provided pair.

    The list represent the path to the file as determined by Binance Vision.

    Parameters
    ----------
    pair : str
        Crypto currency pair. Example: "BTCUSDT"

    Returns
    -------
    List[str]
    """
    keys: List[str] = []
    latest: datetime = datetime.utcnow() - timedelta(1)

    while START_DATE <= latest:
        keys.append(
            (
                f"{PREFIX}{pair}/{INTERVAL}/{pair}-{INTERVAL}-"
                f"{latest:%Y-%m-%d}.zip"
            )
        )
        latest -= timedelta(1)

    return keys


def get_list_of_prefixes() -> List[str]:
    """
    Generate a list of Binance prefixes.

    Returns
    -------
    List[str]
    """
    parameters: dict = {
        "delimiter": "/",
        "prefix": PREFIX,
        "marker": PREFIX,
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
        prefixes.extend(next_prefixes)
        next_marker: str = soup.find(
            "nextmarker"
        )  # re-set the ``next_marker``

    return prefixes


def get_all_pairs(list_of_prefixes: List[str]) -> List[str]:
    """
    Generate a list of all pairs.

    Parameters
    ----------
    list_of_prefixes : List[str]
        A prefix that follows this pattern:
            "data/spot/daily/klines/<A_PAIRING>/"

    Returns
    -------
    List[str]
        A list of pairings, for example: ["BTCUSDT", "BTCBUSD", "ETHUSDT"]
    """
    list_of_pairs: List[str] = []
    for prefix in list_of_prefixes:
        _, _, prefix = prefix.rstrip("/").rpartition("/")
        list_of_pairs.append(prefix)

    return list_of_pairs


def get_bases() -> Set[str]:
    """Return a list of base tokens."""
    response = requests.get(URL_BINANCE_EXCHANGE_INFO)
    data: List[dict] = json.loads(response.text)["symbols"]
    asset_set: Set[str] = set()
    for entry in data:
        asset_set.add(entry["baseAsset"])
        asset_set.add(entry["quoteAsset"])

    return asset_set


if __name__ == "__main__":
    # get_list_of_prefixes()
    get_bases()
