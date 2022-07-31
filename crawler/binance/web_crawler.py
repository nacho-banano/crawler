"""
Binance web-crawler.

Build a list of the Binance archive for 1 minute kline data.
"""

__author__ = "Nacho Banana"

from datetime import datetime, timedelta
from warnings import catch_warnings, simplefilter
from typing import List

import requests
from bs4 import BeautifulSoup, Tag

URL_S3: str = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
PREFIX: str = "data/spot/daily/klines/"
INTERVAL: str = "1m"
START_DATE: datetime = datetime(2021, 3, 1)


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


def get_list_of_files() -> List[str]:
    """
    Generate a list of Binance archives for 1m kline data.

    Returns
    -------
    List[str]
    """
    list_of_files: List[str] = []

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

    for prefix in prefixes:
        _, _, prefix = prefix.rstrip("/").rpartition("/")
        list_of_files.extend(extract_key(prefix))

    return list_of_files
