import os
from tempfile import NamedTemporaryFile
from typing import List

from zipfile import ZipFile

import requests

URL_BINANCE: str = "https://data.binance.vision"


def download(file_keys: List[str], output_dir: str):
    """Download all 1 minute klines from Binance's archive."""
    # Ensure the output directory is present
    os.makedirs(output_dir, exist_ok=True)
    for key in file_keys:
        url: str = "/".join((URL_BINANCE, key))
        response = requests.get(url)

        if not response.ok:
            continue

        with NamedTemporaryFile("wb") as tmp_file:
            tmp_file.write(response.content)

            with ZipFile(tmp_file.name, "r") as zip_archive:
                basename: str = os.path.basename(key).replace(".zip", ".csv")
                zip_archive.extract(basename, output_dir)
