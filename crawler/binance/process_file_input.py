"""TODO: Add description."""

from logging import Logger, getLogger
from typing import Collection, Dict, Literal, Set, List
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta


class Processing:
    """TODO: Add description."""

    PREFIX: str = "data/spot/{frequency}/klines/"
    INTERVAL: str = "1m"
    START_DATE: datetime = datetime(2021, 3, 1)

    def __init__(
        self,
        bases: Collection[str],
        prefixes: List[str],
        frequency: Literal["daily", "monthly"],
    ) -> None:
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
        self._frequency: Literal["daily", "monthly"] = frequency
        self._prefix: str = self.PREFIX.format(frequency=frequency)
        self._logger: Logger = getLogger(self.__class__.__name__)

    def get_triangular_zip_files(self) -> Dict[str, List[str]]:
        """
        TODO: Add a line here.

        When given a list of prefixes it will remove all pairs that cannot be
        used in triangular arbitrage.

        Returns
        -------
        dict
            A filtered dictionary with valid triangular pairings.
        """
        result: Set[str] = self.get_triangles()

        return self._get_zip_dict(result)

    def get_triangles(self) -> List[str]:
        """
        Return a filtered list with only valid pairs of triangular paths.

        Returns
        -------
        List[str]
        """
        valid_path_set: List[str] = []

        for base in self._bases:
            filter_results = self._filter_list(
                base, self._get_all_pairs(self._prefixes)
            )

            for filter_result in filter_results:
                valid_path_set.append(filter_result)

        return sorted(valid_path_set, reverse=True)

    def _filter_list(self, base: str, pairs: Set[str]) -> Set[str]:
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
        valid_path_set: Set[str] = set()

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

        return valid_path_set

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
        result: Set[str] = (
            self._generate_zip_list_daily(pair)
            if self._frequency == "daily"
            else self._generate_zip_list_monthly(pair)
        )
        return result

    def _generate_zip_list_daily(self, pair: str) -> Set[str]:
        """
        TODO: Add description.

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
                    f"{self._prefix}{pair}/{self.INTERVAL}/{pair}-"
                    f"{self.INTERVAL}-{latest:%Y-%m-%d}.zip"
                )
            )
            latest -= timedelta(1)

        return keys

    def _generate_zip_list_monthly(self, pair: str) -> Set[str]:
        """
        TODO: Add description.

        Parameters
        ----------
        pair : str
            Crypto currency pair. Example: "BTCUSDT"

        Returns
        -------
        Set[str]
        """
        keys: Set[str] = set()
        latest: datetime = datetime.utcnow() - relativedelta(months=1)

        while self.START_DATE <= latest:
            keys.add(
                (
                    f"{self._prefix}{pair}/{self.INTERVAL}/{pair}-"
                    f"{self.INTERVAL}-{latest:%Y-%m}.zip"
                )
            )
            latest -= relativedelta(months=1)

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
