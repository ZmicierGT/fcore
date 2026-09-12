"""Pluggable portfolio weighting classes.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import abc

from data.fvalues import Quotes


class Weigher(metaclass=abc.ABCMeta):
    """
        Base class for portfolio weighers. A weigher defines how much weight each security
        gets within its scope (a group or the whole portfolio) and how trade sizes are
        derived from the weights. Custom weighers subclass this and at minimum implement
        weight(). A weigher instance is simply passed to BackTest's constructor
        (weighted=...); the engine is reached via ex.get_caller() — no binding step needed.

        Note: weight() runs before grouping values are recalculated for the cycle, so
        group state (e.g. ex.group_mean) read there reflects the previous cycle.
    """

    # TODO MID update_globals writes private engine fields (bt._max_price, _max_cap,
    #  _grouping_max_cap) — move that state into weighers/public API so custom weighers
    #  don't need to touch engine internals.
    def update_globals(self, bt):
        """
            Recalculate portfolio-global values the weigher needs (called each cycle before
            weights are computed). No-op by default. May write internal fields of bt
            (e.g. _max_price, _max_cap) — this is engine-internal code.

            Args:
                bt(BackTest): the backtest instance.
        """

    def validate_data(self, data):
        """
            Check that the security dataset has everything the weigher needs; raise
            BackTestError otherwise. Called once per BackTestData/BackTestOperations setup.
            No-op by default.

            Args:
                data(BackTestData): the security dataset.
        """

    @abc.abstractmethod
    def weight(self, ex):
        """
            The weight of the current position of the given security.

            Args:
                ex(BackTestOperations): the security executable (bound to the current bar).

            Returns:
                float: the weight value.
        """

    def weight_per_position(self, ex):
        """
            Weight added by a single position (for linear weighers where
            weight = positions * weight_per_position). Used by the default get_trade_num().

            Args:
                ex(BackTestOperations): the security executable.

            Returns:
                float: weight per one position.
        """
        num = ex.get_long_positions()
        if num:
            return ex.weight / num
        return 1  # fallback when there is no position to derive from

    def get_trade_num(self, ex, deviation):
        """
            The default ideal trade size for linear weighers. Grouping caps are applied by
            the engine AFTER this returns (do not apply them here).

            Args:
                ex(BackTestOperations): the security executable.
                deviation(float): max deviation multiplier for the trade.

            Returns:
                float: number of securities to trade (may be negative).
        """
        if ex.group_mean:
            return (ex.group_mean * deviation - ex.weight) / self.weight_per_position(ex)
        return ex.get_max_trade_size()


class EqualWeigher(Weigher):
    """Equal (value) weighting."""

    def weight(self, ex):
        return ex.get_long_positions() * ex.get_market_price()

    def weight_per_position(self, ex):
        return ex.get_market_price()


class PriceWeigher(Weigher):
    """Price weighting (as in DJIA)."""

    def weight(self, ex):
        return ex.get_long_positions()

    def weight_per_position(self, ex):
        return 1

    def update_globals(self, bt):
        if bt.get_long_positions_num():
            # No need to calculate anything if any position opened
            bt._max_price = 0
            return

        max_price = 0
        for ex in bt.all_exec():
            if ex.weighted and ex.get_index() is not None:
                max_price = max(max_price, ex.get_row()[Quotes.Open])
        bt._max_price = max_price

    def get_trade_num(self, ex, deviation):
        if ex.group_mean:
            return super().get_trade_num(ex, deviation)
        # If opening the first position, then need to avoid the situation when selected security is cheap
        # as if the maximum possible positions will be opened - then it will be difficult to balance
        # the portfolio in the future. Need to limit the number of cheap securities.
        return ex.get_max_trade_size() * (ex.get_row()[Quotes.Open] / ex.get_caller().max_price) * deviation


class CapWeigher(Weigher):
    """Capitalization weighting. Requires a 'cap' column in quote rows (via Subquery).

        The cap is used at the current bar's decision time, so it is the user's responsibility to make it
        known at the bar's Open — e.g., calculate it as Open * outstanding shares. A subquery-sourced cap
        (e.g. 'fmp_capitalization') is an EOD value and would leak future data into decisions - use for demo purposes only.
    """

    def validate_data(self, data):
        if 'cap' not in data.rows.dtype.names:
            from backtest.base import BackTestError  # local import to avoid a circular import
            raise BackTestError(f"No 'cap' column in dataset for {data.title} but it is required by the weighting method.")

    def update_globals(self, bt):
        if bt.grouping_attr is not None:
            bt._grouping_max_cap = dict.fromkeys(bt._grouping_shares.keys(), 0)

        max_cap = 0
        for ex in bt.all_exec():
            if ex.weighted and ex.get_index() is not None:
                current_cap = ex.get_row()['cap']
                max_cap = max(max_cap, current_cap)
                if bt.grouping_attr is not None:
                    bt._grouping_max_cap[ex.group] = max(bt._grouping_max_cap[ex.group], current_cap)
        bt._max_cap = max_cap

    def weight(self, ex):
        if ex.get_long_positions():
            return ex.get_caller().max_cap / ex.get_row()['cap'] * ex.get_total_value()
        return 0

    def get_trade_num(self, ex, deviation):
        if ex.weight:
            ratio = (ex.group_mean * deviation - ex.weight) / ex.get_total_value()
        else:
            # Limit the number of positions for lower cap companies
            ratio = ex.get_row()['cap'] / ex.group_max_cap * deviation

        if ratio:
            trade_size = ex.get_max_trade_size()  # May buy more
        else:
            trade_size = ex.get_long_positions()  # May sell some

        return ratio * trade_size
