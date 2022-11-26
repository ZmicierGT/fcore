"""Module for Buy and Hold strategy implementation.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

from backtest.base import BackTest

class BuyAndHold(BackTest):
    """
        Buy and hold strategy implementation.
    """
    def skip_criteria(self, index):
        """
            Abstract method placeholder. Check if this cycle should be skipped. Not relevant to B&H strategy.
        """
        return False

    def do_tech_calculation(self, ex):
        """
            Abstract method placeholder. B&H strategy does not involve any technical calculation.
        """

    def do_calculation(self):
        """
            Main strategy calculation method.
        """
        rows = self.get_main_data().get_rows()

        ######################################
        # Perform the global calculation setup
        ######################################

        self.setup()

        # Iterate through all rows and calculate the required values
        for row in rows:
            ####################################################################################################
            # Setup cycle calculations if current cycle shouldn't be skipped (because of offset or lack of data)
            ####################################################################################################

            if self.do_cycle(rows.index(row)) == False:
                continue

            ########################
            # Open positions
            ########################

            # Open a long position if we have enough cash
            self.exec().open_long_max()

            ##############################
            # Teardown the cycle
            ##############################

            self.tear_down()
