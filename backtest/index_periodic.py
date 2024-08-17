"""Index simulation implementation.

This simulation can assemble a portfolio using periodic deposits which follows an index.
An index is defined by compositions (see data.fvalues.djia_dict as an example) and
weightening method (see data.fvalues.Weighted). Also stock grade/sector/whatever grouping
may be applied (see grouping_attr and grouping_shares attributes of the BackTest class).

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.base import BackTest, BackTestError

from data.fvalues import StockQuotes

import sys

class IndexSim(BackTest):
    """
        Index simulation implementation.

        This strategy simulates some index by combining the stock in a way to correspond the index.
    """
    def __init__(self,
                 compositions=None,
                 **kwargs):
        """
            Initializes index simulation implementation.

            Args:
                compositions(dict): index composition periods.

            Raises:
                BackTestError: incorrect arguments values.
        """
        super().__init__(**kwargs)

        self._compositions = compositions  # TODO High Think how better to handle compositions (move it to the base class?)

    def skip_criteria(self, index):
        """
            Estimate if we should skip the current cycle (no data for calculation and so on).

            Args:
                index(int): index of the cycle to calculate.

            Returns:
                True is the cycle should be skipped, False otherwise.
        """
        return False

    def do_tech_calculation(self, ex):
        """
            Perform technical data calculation for the strategy.
        """
        pass

    def do_calculation(self):
        """
            Perform strategy calculation.

            Raises:
                BackTestError: not enough data for calculation.
        """

        ######################################
        # Perform the global calculation setup
        ######################################

        self.setup()

        ############################################################
        # Iterate through all rows and calculate the required values
        ############################################################

        for row in self.get_main_data().get_rows():

            ####################################################################################################
            # Setup cycle calculations if current cycle shouldn't be skipped (because of offset or lack of data)
            ####################################################################################################

            if self.do_cycle(row) is False:
                continue

            #######################################################################################
            # Check if we need/can to open a long position of the symbol with the lowest Stochastic
            #######################################################################################

            min_weight = sys.maxsize  # Minimum weight for the selected security in the current cycle
            max_weight = -1

            target_buy = None  # The security to open a position
            target_sell = None  # The security to close a position

            for ex in self.all_exec():
                if ex.get_index() is None or ex.weighted is False:
                    continue  # Skip securities without data in the cycle or unweighted securities (if eventually we have any)

                if ex.get_short_positions() > 0:
                    raise BackTestError(f"This strategy does not involve shorting but {ex.get_short_positions()} positions were detected for {ex.title}")

                # Check if we need to close the position because the security was excluded from the index
                if ex.title not in self.composition and ex.get_long_positions() and ex.is_limit is False:
                    ex.sell(num=ex.get_long_positions(), limit=ex.get_row()[StockQuotes.Close], limit_deviation=0.01, recalculate=True, exact=True)

                    continue

                # Check if we need to close the position if a security brakes a diversification
                if ex.title in self.composition and ex.weight > max_weight and ex.is_limit is False \
                    and ex.is_min_capacity_group and self.mean_weight:
                    max_weight = ex.weight
                    target_sell = ex

                # Choose the security with minimum weight to open a position
                if ex.title in self.composition and ex.weight < min_weight and ex.is_limit is False \
                    and ex.is_max_capacity_group and ex.get_max_trade_size_cash():
                    min_weight = ex.weight
                    target_buy = ex

            if target_sell is not None and target_sell.get_sell_num() > 0:
                target_sell.sell(limit=target_sell.get_row()[StockQuotes.Close], limit_deviation=0.02)

            if target_buy is not None and (\
                (self.mean_weight and target_buy.get_buy_num() >= 1) or \
                    target_buy.get_max_trade_size_cash()[0] >= 1):
                target_buy.buy(limit=target_buy.get_row()[StockQuotes.Close], limit_deviation=0.01)

            ##############################
            # Teardown the cycle
            ##############################

            self.tear_down()
