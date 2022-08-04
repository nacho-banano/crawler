"""TODO: Add description."""

from typing import Set, List
from datetime import datetime, timedelta

PREFIX: str = "data/spot/daily/klines/"
INTERVAL: str = "1m"
START_DATE: datetime = datetime(2021, 3, 1)


def get_all_zip_files(list_of_prefixes: List[str]) -> Set[str]:
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
    return get_zip_list(list_of_prefixes)


def get_triangular_zip_files(
    bases: Set[str], list_of_prefixes: List[str]
) -> dict:
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
    result: dict = get_triangles(bases, list_of_prefixes)

    return {
        "valid_path_set": get_zip_list(result["valid_path_set"]),
        "valid_paths": result["valid_paths"],
    }


def get_single_triangular_zip_files(list_of_prefixes: List[str]) -> Set[str]:
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
    return get_zip_list(list_of_prefixes)


def get_triangles(bases: Set[str], list_of_prefixes: List[str]) -> dict:
    """
    Return a filtered list containing only pairs with valid triangular paths.

    Returns
    -------
    List[str]
    """
    result: dict = {}
    valid_path_set: Set[str] = set()
    valid_paths: List[dict] = []

    for base in bases:
        filter_results = filter_list(base, get_all_pairs(list_of_prefixes))

        for filter_result in filter_results["valid_path_set"]:
            valid_path_set.add(filter_result)

        for filter_result in filter_results["valid_paths"]:
            valid_paths.append(filter_result)

    result["valid_path_set"] = valid_path_set
    result["valid_paths"] = valid_paths

    return result


def _coin_in_pair(coin: str, pair: str) -> bool:
    """
    TODO: Add description.

    Parameters
    ----------
    coin : str
        _description_
    pair : str
        _description_

    Returns
    -------
    bool
        _description_
    """
    return pair.startswith(coin) or pair.endswith(coin)


def filter_list(base: str, pairs: Set[str]) -> dict:
    """
    Create a set of valid list of triangular pairs.

    Parameters
    ----------
    base : str
        _description_
    pairs : Set[str]
        _description_

    Returns
    -------
    Set[str]
        _description_
    """
    result: dict = {}
    valid_path_set: Set[str] = set()
    valid_paths: List[dict] = []

    for step_one in pairs:
        # check if the base is in the pair
        if _coin_in_pair(base, step_one):
            intermediate: str = step_one.replace(base, "")

            for step_two in pairs:
                if _coin_in_pair(intermediate, step_two) and not _coin_in_pair(
                    base, step_two
                ):
                    ticker: str = step_two.replace(intermediate, "")

                    for step_three in pairs:
                        if (
                            step_three.replace(base, "") == ticker
                            and step_three.replace(ticker, "") == base
                        ):
                            valid_path_set.add(step_one)
                            valid_path_set.add(step_two)
                            valid_path_set.add(step_three)
                            valid_paths.append(
                                {
                                    "base": step_one,
                                    "intermediate": step_two,
                                    "ticker": step_three,
                                }
                            )

    result["valid_path_set"] = valid_path_set
    result["valid_paths"] = valid_paths

    return result


def get_zip_list(list_of_prefixes: List[str]) -> Set[str]:
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

        for key in generate_zip_list(prefix):
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


def generate_zip_list(pair: str) -> Set[str]:
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
