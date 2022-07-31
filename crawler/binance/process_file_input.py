from typing import Set, List
from datetime import datetime, timedelta

PREFIX: str = "data/spot/daily/klines/"
INTERVAL: str = "1m"
START_DATE: datetime = datetime(2021, 3, 1)


def get_all_files(list_of_prefixes: List[str]) -> Set[str]:
    return get_list(list_of_prefixes)


def get_all_triangle_candidate_list(list_of_prefixes: List[str]) -> Set[str]:
    return get_list(get_triangles(list_of_prefixes))


def get_triangles() -> List[str]:
    """
    Return a filtered list containing only pairs with valid triangular paths.

    Returns
    -------
    List[str]
    """
    # TODO: solve this bitch


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
        _description_
    """
    return get_list(list_of_prefixes)


def get_list(list_of_prefixes: List[str]) -> Set[str]:
    list_of_files: Set[str] = set()
    for prefix in get_all_pairs(list_of_prefixes):
        list_of_files.add(**extract_key(prefix))

    return list_of_files


def get_all_pairs(list_of_prefixes: List[str]) -> Set[str]:
    """
    Generate a list of all pairs.

    Parameters
    ----------
    list_of_prefixes : List[str]
        A prefix that follows this pattern: "data/spot/daily/klines/<A_PAIRING>/"

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
            (f"{PREFIX}{pair}/{INTERVAL}/{pair}-{INTERVAL}-" f"{latest:%Y-%m-%d}.zip")
        )
        latest -= timedelta(1)

    return keys
