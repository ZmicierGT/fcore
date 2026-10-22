"""Pluggable portfolio weighting classes.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import abc

from data.fvalues import Quotes

__all__ = ["Weigher", "EqualWeigher", "PriceWeigher", "CapWeigher"]


class Weigher(metaclass=abc.ABCMeta):
    """
        Base class for portfolio weighers. A weigher defines how much weight each security
        gets within its scope (a group or the whole portfolio) and how trade sizes are
        derived from the weights. Custom weighers subclass this and at minimum implement
        weight(). A weigher instance is simply passed to BackTest's constructor
        (weighted=...); the engine is reached via ex.caller — no binding step needed.

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
        num = ex.long_positions
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
        return ex.long_positions * ex.market_price

    def weight_per_position(self, ex):
        return ex.market_price


class PriceWeigher(Weigher):
    """Price weighting (as in DJIA)."""

    def __init__(self):
        # Max Open price per group (populated each cycle when grouping is enabled).
        # Used to size the first position in a group by its price rank within the group,
        # so that one group cannot consume too much cash before the others are filled.
        self._group_max_price = {}

    def weight(self, ex):
        return ex.long_positions

    def weight_per_position(self, ex):
        return 1

    def update_globals(self, bt):
        from backtest.base import SecState  # local import to avoid a circular import

        # The values depend on Open prices only and are valid regardless of the opened positions.
        self._group_max_price = {}

        max_price = 0
        for ex in bt.all_exec:
            # Delisted and skipped securities must not pollute the global values
            if ex.weighted and ex.get_index() is not None and ex.state is SecState.Active:
                price = ex.get_row()[Quotes.Open]
                max_price = max(max_price, price)
                if bt.grouping_attr is not None:
                    self._group_max_price[ex.group] = max(self._group_max_price.get(ex.group, 0), price)

        bt._max_price = max_price

    def get_trade_num(self, ex, deviation):
        if ex.group_mean:
            return super().get_trade_num(ex, deviation)

        # For the first position, size the buy by the price rank within the group (or the whole
        # portfolio without grouping) to avoid opening too many positions of a cheap security.
        group = ex.group
        if group is not None and group in self._group_max_price:
            max_price = self._group_max_price[group]
        else:
            max_price = ex.caller.max_price

        # Defensive guard: no active weighted quotes (e.g. an empty group) - the price rank
        # cannot be determined. super() returns the maximum trade size here (group_mean is
        # falsy in this branch), effectively skipping the price-rank limitation.
        if not max_price:
            return super().get_trade_num(ex, deviation)

        return ex.get_max_trade_size() * (ex.get_row()[Quotes.Open] / max_price) * deviation


class CapWeigher(Weigher):
    """Capitalization weighting. Requires a 'cap' column in quote rows (via Subquery)."""

    def __init__(self):
        self._total_cap = 0

    def validate_data(self, data):
        if 'cap' not in data.rows.dtype.names:
            from backtest.base import BackTestError  # local import to avoid a circular import
            raise BackTestError(f"No 'cap' column in dataset for {data.title} but it is required by the weighting method.")

    def update_globals(self, bt):
        from backtest.base import SecState  # local import to avoid a circular import

        if bt.grouping_attr is not None:
            bt._grouping_max_cap = dict.fromkeys(bt._grouping_shares.keys(), 0)

        max_cap = 0
        total_cap = 0
        for ex in bt.all_exec:
            # Delisted and skipped securities must not pollute the global values
            if ex.weighted and ex.get_index() is not None and ex.state is SecState.Active:
                current_cap = ex.get_row()['cap']
                max_cap = max(max_cap, current_cap)
                total_cap += current_cap
                if bt.grouping_attr is not None:
                    bt._grouping_max_cap[ex.group] = max(bt._grouping_max_cap[ex.group], current_cap)
        bt._max_cap = max_cap
        self._total_cap = total_cap

    def weight(self, ex):
        if ex.long_positions:
            return ex.total_value / ex.get_row()['cap']
        return 0

    def weight_per_position(self, ex):
        return ex.market_price / ex.get_row()['cap']

    def get_trade_num(self, ex, deviation):
        if ex.weight:
            # Relative change needed to reach the group mean value per unit of cap.
            ratio = (ex.group_mean * deviation - ex.weight) / ex.weight
            return ratio * ex.long_positions

        if self._total_cap and ex.group is None:
            # Size a new position from the whole portfolio value, not from the available cash only.
            target_value = ex.caller.total_value * ex.get_row()['cap'] / self._total_cap
            price = ex.market_price
            if price:
                return target_value / price

        if ex.group_max_cap:
            # Limit the number of positions for lower cap companies
            ratio = ex.get_row()['cap'] / ex.group_max_cap * deviation
            return ratio * ex.get_max_trade_size()

        # No active capped symbols in the group - trade size cannot be determined
        return 0
