"""Binance web-crawler."""

__version__ = "0.1.1"

from datetime import datetime
from logging import INFO, basicConfig
from os import mkdir
from os.path import isdir, join

LOG_FOLDER: str = "logs"

if not isdir(LOG_FOLDER):
    mkdir(LOG_FOLDER)

basicConfig(
    filename=join(LOG_FOLDER, f"{datetime.now():%Y%m%d}.log"),
    format="%(asctime)s - %(levelname)s - %(name)s: %(message)s",
    style="%",
    datefmt="%Y-%m-%d %H:%M:%S%z",
    level=INFO,
)
