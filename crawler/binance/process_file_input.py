"""TODO: Add description."""

from logging import Logger, getLogger
from typing import Collection, Dict, Set, List
from datetime import datetime, timedelta


class Processing:
    """TODO: Add description."""

    PREFIX: str = "data/spot/daily/klines/"
    INTERVAL: str = "1m"
    START_DATE: datetime = datetime(2021, 3, 1)

    def __init__(self, bases: Collection[str], prefixes: List[str]) -> None:
        """
        TODO: Add description.

        Parameters
        ----------
        bases : Collection[str]
            _description_
        prefixes : List[str]
            _description_
        """
        self._bases: Collection[str] = bases
        self._prefixes: List[str] = prefixes
        self._logger: Logger = getLogger(self.__class__.__name__)

    def get_all_zip_files(self) -> Dict[str, List[str]]:
        """
        Use this to get a list of prefixes.

        Returns
        -------
        Dict[str, List[str]]
            A collection of file resources used to download zips from binance.
        """
        return self._get_zip_dict(self._prefixes)

    def get_triangular_zip_files(self) -> dict:
        """
        TODO: Add a line here.

        When given a list of prefixes it will remove all pairs that cannot be
        used in triangular arbitrage.

        Returns
        -------
        dict
            A filtered dictionary with valid triangular pairings.
        """
        result: dict = self.get_triangles()

        return {
            "valid_path_set": self._get_zip_dict(result["valid_path_set"]),
            "valid_paths": result["valid_paths"],
        }

    def get_single_triangular_zip_files(
        self, triangle: Collection[str]
    ) -> Dict[str, List[str]]:
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
        Dict[str, List[str]]
            A dictionary of file resources used to download zips from binance
            data based on the input.
        """
        return self._get_zip_dict(triangle)

    def get_triangles(self) -> dict:
        """
        Return a filtered list with only valid pairs of triangular paths.

        Returns
        -------
        dict
        """
        result: dict = {}
        valid_path_set: Set[str] = set()
        valid_paths: List[dict] = []

        for base in self._bases:
            filter_results = self._filter_list(
                base, self._get_all_pairs(self._prefixes)
            )

            for filter_result in filter_results["valid_path_set"]:
                valid_path_set.add(filter_result)

            for filter_result in filter_results["valid_paths"]:
                valid_paths.append(filter_result)

        result["valid_path_set"] = valid_path_set
        result["valid_paths"] = valid_paths

        return result

    def _filter_list(self, base: str, pairs: Set[str]) -> dict:
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
        dict
            _description_
        """
        result: dict = {}
        valid_path_set: Set[str] = set()
        valid_paths: List[dict] = []

        for step_one in pairs:
            # check if the base is in the pair
            if self.coin_in_pair(base, step_one):
                intermediate: str = step_one.replace(base, "")

                for step_two in pairs:
                    if self.coin_in_pair(
                        intermediate, step_two
                    ) and not self.coin_in_pair(base, step_two):
                        ticker: str = step_two.replace(intermediate, "")

                        step_three: str = (
                            base + ticker
                            if base + ticker in pairs
                            else ticker + base
                            if ticker + base in pairs
                            else ""
                        )

                        if step_three:
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

    def _get_zip_dict(self, prefixes: List[str]) -> Dict[str, List[str]]:
        """
        Get a dictionary of resources used in downloading.

        Parameters
        ----------
        prefixes : List[str]
            _description_

        Returns
        -------
        Dict[str, List[str]]
            _description_
        """
        resource_dict: Dict[str, List[str]] = {}

        for prefix in self._get_all_pairs(prefixes):
            resource_dict[prefix] = sorted(
                self._generate_zip_list(prefix),
                reverse=True,
            )

        return resource_dict

    def _generate_zip_list(self, pair: str) -> Set[str]:
        """
        Generate a list of keys for the provided pair.

        The list represent the path to the file as determined by Binance
        Vision.

        Parameters
        ----------
        pair : str
            Crypto currency pair. Example: "BTCUSDT"

        Returns
        -------
        Set[str]
        """
        keys: Set[str] = set()
        latest: datetime = datetime.utcnow() - timedelta(1)

        while self.START_DATE <= latest:
            keys.add(
                (
                    f"{self.PREFIX}{pair}/{self.INTERVAL}/{pair}-"
                    f"{self.INTERVAL}-{latest:%Y-%m-%d}.zip"
                )
            )
            latest -= timedelta(1)

        return keys

    def _get_all_pairs(self, prefixes: List[str]) -> Set[str]:
        """
        Generate a list of all pairs.

        Parameters
        ----------
        prefixes : List[str]
            A prefix that follows this pattern:
                "data/spot/daily/klines/<A_PAIRING>/"

        Returns
        -------
        Set[str]
            A list of pairings, for example: ["BTCUSDT", "BTCBUSD", "ETHUSDT"]
        """
        list_of_pairs: Set[str] = set()

        for prefix in prefixes:
            prefix = self.get_pair(prefix)
            list_of_pairs.add(prefix)

        return list_of_pairs

    @staticmethod
    def get_pair(prefix: str) -> str:
        """
        Extract the pair from a string of prefixes.

        Parameters
        ----------
        prefix : str
            Something that looks like this:
            "data/spot/daily/klines/<A_PAIRING>/"

        Returns
        -------
        str
            Pair element of the prefix.

        Example
        -------
        >>> prefix: str = "data/spot/daily/klines/BTC/"
        >>> pair = Processing.get_pair(prefix)
        >>> print(pair)
        "BTC"
        """
        _, _, pair = prefix.rstrip("/").rpartition("/")

        return pair

    @staticmethod
    def coin_in_pair(coin: str, pair: str) -> bool:
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
