"""TODO: Add description."""

from typing import Set, List
from datetime import datetime, timedelta

PREFIX: str = "data/spot/daily/klines/"
INTERVAL: str = "1m"
START_DATE: datetime = datetime(2021, 3, 1)


def get_all_files(list_of_prefixes: List[str]) -> Set[str]:
    """
    Use this to get a list of prefixes.

    Parameters
    ----------
    list_of_prefixes : List[str]
        A list of prefixes from the web or read in from a file

    Returns
    -------
    Set[str]
        A set of file resources used to download zips from binance data
    """
    return get_resource_set(list_of_prefixes)


def get_all_triangle_candidate_list(list_of_prefixes: List[str]) -> Set[str]:
    """
    When given a list of prefixes it will remove all pairs that cannot be \
    used in triangular arbitrage.

    Parameters
    ----------
    list_of_prefixes : List[str]
        A list of prefixes from the web or read in from a file

    Returns
    -------
    Set[str]
        A filtered set with valid triangular pairings
    """
    return get_resource_set(get_triangles(list_of_prefixes))


def get_triangles(list_of_prefixes: List[str]) -> List[str]:
    """
    Return a filtered list containing only pairs with valid triangular paths.

    Returns
    -------
    List[str]
    """
    # TODO: solve this bitch
    ...


def get_triangle(list_of_prefixes: List[str]) -> Set[str]:
    """
    Use this for specific triangular path data downloads.

    Parameters
    ----------
    list_of_prefixes : List[str]
        A list of size 3 that contains a valid triangular path of prefixes,
        for example:
            [
                "data/spot/daily/klines/BTCXNO",
                "data/spot/daily/klines/XNOETH",
                "data/spot/daily/klines/ETHBTC"
            ]

    Returns
    -------
    Set[str]
        A set of file resources used to download zips from binance data based
        on the input
    """
    return get_resource_set(list_of_prefixes)


def get_resource_set(list_of_prefixes: List[str]) -> Set[str]:
    """
    Get a set of resources used in downloading.

    Parameters
    ----------
    list_of_prefixes : List[str]
        _description_

    Returns
    -------
    Set[str]
        _description_
    """
    resource_set: Set[str] = set()
    for prefix in get_all_pairs(list_of_prefixes):
        for key in extract_key(prefix):
            resource_set.add(key)

    return resource_set


def get_all_pairs(list_of_prefixes: List[str]) -> Set[str]:
    """
    Generate a list of all pairs.

    Parameters
    ----------
    list_of_prefixes : List[str]
        A prefix that follows this pattern:
            "data/spot/daily/klines/<A_PAIRING>/"

    Returns
    -------
    Set[str]
        A list of pairings, for example: ["BTCUSDT", "BTCBUSD", "ETHUSDT"]
    """
    list_of_pairs: Set[str] = set()
    for prefix in list_of_prefixes:
        _, _, prefix = prefix.rstrip("/").rpartition("/")
        list_of_pairs.add(prefix)

    return list_of_pairs


def extract_key(pair: str) -> Set[str]:
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
    keys: Set[str] = set()
    latest: datetime = datetime.utcnow() - timedelta(1)

    while START_DATE <= latest:
        keys.add(
            (
                f"{PREFIX}{pair}/{INTERVAL}/{pair}-{INTERVAL}-"
                f"{latest:%Y-%m-%d}.zip"
            )
        )
        latest -= timedelta(1)

    return keys
