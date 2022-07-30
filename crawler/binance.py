"""Binance data web-crawler."""

import os
from tempfile import NamedTemporaryFile
from typing import List
from warnings import catch_warnings, simplefilter
from zipfile import ZipFile

import requests
from bs4 import BeautifulSoup, Tag

URL_S3: str = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
URL_BINANCE: str = "https://data.binance.vision"


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


def download_all(output_dir: str):
    """Download all 1 minute klines from Binance's archive."""
    # Ensure the output directory is present
    os.makedirs(output_dir, exist_ok=True)

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
        prefixes.extend(extract_prefix(soup))
        next_marker: str = soup.find(  # re-set the ``next_marker``
            "nextmarker"
        )

    for prefix in prefixes:
        parameters["prefix"] = f"{prefix}1m/"
        parameters["marker"] = "data/spot/daily/klines/"
        response = requests.get(URL_S3, params=parameters, headers=headers)
        soup = BeautifulSoupProxy(response.text, features="lxml")

        file_keys: List[str] = extract_key(soup)
        next_marker: Tag = soup.find("nextmarker")

        while next_marker:
            parameters["marker"] = next_marker.get_text()
            response = requests.get(URL_S3, params=parameters, headers=headers)
            soup = BeautifulSoupProxy(response.text, features="lxml")
            file_keys.extend(extract_key(soup))
            next_marker: str = soup.find(  # re-set the ``next_marker``
                "nextmarker"
            )

        for key in file_keys:
            url: str = "/".join((URL_BINANCE, key))
            response = requests.get(url)

            with NamedTemporaryFile("wb") as tmp_file:
                tmp_file.write(response.content)

                with ZipFile(tmp_file.name, "r") as zip_archive:
                    basename: str = os.path.basename(key).replace(
                        ".zip", ".csv"
                    )
                    zip_archive.extract(basename, output_dir)
