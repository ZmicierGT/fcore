"""Base module for backtesting strategies.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import abc
from datetime import datetime
from enum import IntEnum

from itertools import repeat

from data.fvalues import Rows

from pandas.core.series import Series

from threading import Thread, Event

from data.futils import thread_available

# Enum class for backtesting results data order.
class BTDataEnum(IntEnum):
    """Enum to describe a list with backtesting result."""
    DateTime = 0
    TotalValue = 1
    Deposits = 2
    Cash = 3
    Borrowed = 4
    # Other profit may be dividends in case of stock or coupon on case of bonds.
    OtherProfit = 5
    CommissionExpense = 6
    SpreadExpense = 7
    DebtExpense = 8
    # Other profit may be dividend fees in a case of a short position.
    OtherExpense = 9
    TotalExpenses = 10
    TotalTrades = 11
    Symbols = 12

# Enum class for backtesting data regarding a particular symbol
class BTSymbolEnum(IntEnum):
    """Enum to describe a list with backtesting result for each particular symbol."""
    Title = 0
    Quote = 1
    TradePriceLong = 2
    TradePriceShort = 3
    TradePriceMargin = 4
    LongPositions = 5
    ShortPositions = 6
    MarginPositions = 7
    TradesNo = 8
    Tech = 9

# Exception class for general backtesting errors
class BackTestError(Exception):
    """Class to represent an exception triggered during backtesting."""
    pass

# Data storage class for backtesting
class BackTestData():
    """Thread-safe class which represents data used in backtesting.
    
        This class is used as a base class for various financial instruments. For example, if you add to it a yield and yield interval,
        then it may be used to represent a stock.
    """
    def __init__(self,
                 rows,
                 title='',
                 margin_req=0,
                 margin_rec=0,
                 spread=0,
                 margin_fee=0,
                 trend_change_period=0,
                 trend_change_percent=0
                ):
        """Initializes BackTestData class.

            Args:
                rows(list): data for the particular symbol obtained from the database using fdata module.
                title(str): title of the symbol used in the class.
                margin_req(float): required margin ratio for the symbol. Default is 0.
                margin_rec(float): recommended margin ratio for the symbol. Default is 0.
                margin_fee(float): annual margin fee for the symbol (in percent). Default is 0.
                spread(float): pre-defined spread for the symbol.
                trend_change_period(int): indicates the period in timespans when the trend for this symbol is considered as changed.
                    Default is 0.
                trend_change_percent(float): indicates the change of the quote in percent when the trend for the symbol is considered as changed
                    immediately. Default is 0.

            Raises:
                BackTestError: inaproppriate values were provided.                 
        """

        if len(rows) == 0:
            raise BackTestError("Length of list with quotes can't be 0.")

        # Data for the calculation
        self._rows = rows

        # Required margin ratio. For example, if the required ration ratio is 0.7 and current financial instrument price is $1000,
        # then at maximum $7000 may be lended by a broker. In case if maximum margin limit is hit, margin call is possible.
        if margin_req < 0:
            raise BackTestError(f"margin_req can't be less than 0. Specified value is {margin_req}")
        self._margin_req = margin_req

        # Recommended margin ratio. Backtesting engine won't try to exceed it. Exceeding this limit
        # is still acceptable but is considered at potentially dangerous and may lead to a margin call.
        if margin_rec < 0:
            raise BackTestError(f"margin_rec can't be less than 0. Specified value is {margin_rec}")
        self._margin_rec = margin_rec

        # Recommended margin ratio should be less than required
        if margin_rec > margin_req:
            raise BackTestError(f"margin_rec should be less than margin_req, however {margin_rec} is not < {margin_req}")

        # Spread (in percent)
        if spread < 0 or spread > 100:
            raise BackTestError(f"Spread can't be less than 0% or more than 100%. Specified value is {spread}")
        self._spread = spread

        if margin_fee < 0 or margin_fee > 100:
            raise BackTestError(f"margin_fee can't be less than 0% or more than 100%. Specified value is {margin_fee}")
        self._margin_fee = margin_fee

        # Indicates how many periods need to pass that we consider that the signal has changed
        if trend_change_period < 0:
            raise BackTestError(f"trend_change_period can't be less than 0%. Specified value is {trend_change_period}")
        self._trend_change_period = trend_change_period

        # Indicates quote change in percent when we consider that the signal changed immediately
        if trend_change_percent < 0:
            raise BackTestError(f"trend_change_percent can't be less than 0%. Specified value is {trend_change_percent}")
        self._trend_change_percent = trend_change_percent

        # Title of the financial instrument
        self._title = title

    #####################
    # Thread safe methods
    #####################

    def get_rows(self):
        """
            Get data used in calculations.

            Returns:
                list: Data obtained from the databased used in the backtesting strategy.
        """
        return self._rows

    def get_title(self):
        """
            Get symbol title.

            Returns:
                str: Symbol title (like a ticker).
        """
        return self._title

    def get_margin_req(self):
        """
            Get the required margin ratio for the symbol. If the ratio is 0.7 and you have 10 financial instrument in the portfolio and the price
            of the financial instrument is $100, then this position will give you a $700 of margin buying power. Exceeding buying power
            will trigger a margin call.

            Returns:
                float: Maximum possible margin ratio for this symbol.
        """
        return self._margin_req

    def get_margin_rec(self):
        """
            Get the recommended margin ratio for the symbol. If the ratio is 0.7 and you have 10 same financial instruments
            in the portfolio and the price of the financial instrument is $100, then this position will give you a $700 of margin buying power.
            Exceeding buying power (unless it hits the required ratio) will not trigger a margin call but backtesting engine
            won't open new positions in such case. Default is 0.

            Returns:
                float: Recommended margin ratio for this symbol.
        """
        return self._margin_rec

    def get_spread(self):
        """
            Returns:
                float: Spread for the symbol.
        """
        return self._spread

    def get_margin_fee(self):
        """
            Get annual margin fee for the symbol (in percent). Calculated daily based on the value of opened margin positions.

            Returns:
                float: Aannual margin fee (in percent) for the symbol.
        """
        return self._margin_fee

    def get_trend_change_period(self):
        """
            Get the period which indicates the trend change. The period is a number of timespan cycles (days, minutes and so on) used in the calculation.
            For example, if it is 1 and we use EOD data, then backtesting engine will consider that the trend has changed only after 1 additional day
            if there are no other trend change indications used in the strategy (high volume, essential change in percent and so on).

            Returns:
                int: Trend change period.
        """
        return self._trend_change_period

    def get_trend_change_percent(self):
        """
            Get the value in % which indicates the trend change of the symbol. For example, if trend_change_percent is 3 and the quote changed
            by 4% in one cycle of calculation (day, minute and so on - depending on a used timestamp), then the trend is considered as
            changed immediately.

            Returns:
                float: Trend change percent.
        """
        return self._trend_change_percent

    def get_first_year(self):
        """
            Get first year in the dataset for the calculation.

            Returns:
                The first year in the dataset.

            Raises:
                BackTestError: incorrect date in the provided data.
        """
        dt_str = self._rows[0][Rows.DateTime]

        try:
            dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            raise BackTestError(f"The date {dt_str} is incorrect: {e}") from e

        return dt

    def create_exec(self, caller):
        """
            Create an instance with operations for the particular symbol. The instance is not thread-safe and should be created separately
            for the each particular thread. Each data class (BackTestData) may have more than one corresponding data operations instance.

            Returns:
                BackTestOperations: Class instance to perform the operations on the data for a particular symbol.
        """
        exec_instance = BackTestOperations(data=self, caller=caller)

        return exec_instance

#############################
# Base backtesting operations
#############################

class BackTestOperations():
    """ Class to repsesent the operations for a particular symbol.
    
        Several instances of this class may use one thread-safe data-class (BackTestData)
    """
    def __init__(self, data, caller):
        """
            Initialises BackTestOperations class.

            Args:
                data(BackTestData): corresponding thread-safe data class for a particular symbol.
                caller(BackTest): instance of the 'main' backtesting class which creates the instance of the current class.
        """
        ############################################################
        # General data used for calculations for a particular symbol
        ############################################################

        self.__data = data
        self.__title = None

        # Opened positions
        self._long_positions = 0
        self._long_positions_cash = 0
        self._short_positions = 0

        # Number of trades per symbol
        self._trades_no = 0

        # Backtesting class instance
        self.__caller = caller

        # Portfolio with long/short positions for the symbol
        self._portfolio = []

        # Quote to calculate signal change
        self._signal_quote = None
        # Index for signal change calculations
        self._signal_index = None

        # TA indicator(s) calculated values
        self._values = None

        ####################################################################
        # Cycle specific data for calculations. Need to be reset each cycle.
        ####################################################################

        # Trade prices in the current cycle
        self._trade_price_long = None
        self._trade_price_short = None
        self._trade_price_margin = None

    def data(self):
        """
            Gets the used data class instance.

            Returns:
                BackTestData: The corresponding data class.
        """
        return self.__data

    #############################################################
    # General functions with calculations for a particular symbol
    #############################################################

    def set_title(self, title):
        """
            Set the title of the calculation.

            Args:
                title(str): the title for the calculation. Usually it represents the indicators used for the strategy, not a symbol title.
        """
        self.__title = title

    def set_values(self, values):
        """
            Set the values of the calculation.

            Args:
                values: values of the calculation. Usually it is a pandas DataFrame, usual list, may be a numpy array and so on.
                May be a multi-dimensional list.
        """
        self._values = values

    def get_values(self):
        """
            Get the values of the calculation.

            Returns:
                The values of the calculation of the indicators for the symbol used in the strategy. Usually it is a pandas DataFrame,
                usual list, may be a numpy array and so on. It may be a multi dimensional list as well.
        """
        return self._values

    def get_title(self):
        """
            Get the title of the calculation.

            Returns:
                str: The title of the calculation. Usually it represents the indicators used in the calculation, not a symbol title.
        """
        return self.__title

    def get_value(self, num=None):
        """
            Get the current value or a value at the particular index of the calculated indicator(s)

            Args:
                int: optional index of the value to get. Default is None.

            Returns:
                The values (it may be a list as well) which represents a calculated indicator value at the current position or at
                the particular index.
        """
        if num == None:
            num = self.get_caller_index()

        # Workaround to handle both pandas and list instances of values
        if isinstance(self._values, Series) or isinstance(self._values, list):
            return self._values[num]

    def get_value_offset_from_end(self, offset):
        """
            Get the value with the offset from the current index of the calculated indicator(s)

            Args:
                int: offset of the index to get.

            Returns:
                The values (it may be a list as well) which represents a calculated indicator value at the offset from thecurrent position.
                For example, if the offset is 1, it will return the last but one calculated value.
        """
        try:
            value = self.get_value(self.get_caller_index() - offset)
        except IndexError:
            value = None

        return value

    def get_first_year(self):
        """
            Get the first year in the dataset.

            Returns:
                str: The first year in the dataset.
        """
        return self.data().get_first_year()

    # Fee calculated based on commission in percent of the trade
    def get_trade_percent_fee(self):
        """
            Get the percent fee for the current trade (1 instrument)

            Returns:
                float: percent fee for the trade for 1 instrument
        """
        return self.get_quote() * self.get_caller().get_commission_percent() / 100

    # Fee for one share
    def get_share_fee(self):
        """
            Get the one share fee for the trade (expect commission for the trade).

            Returns:
                float: one share fee (excluding commission for the whole trade)
        """
        return self.get_trade_percent_fee() + self.get_caller().get_commission_share()

    # Total fee for a trade (1 share)
    def get_total_fee(self):
        """
            Get the total fee for 1 share trade.

            Returns:
                float: the total fee for 1 share trade.
        """
        return self.get_share_fee() + self.get_caller().get_commission()

    def get_max_positions(self):
        """
            Get the maximum number of opened positions for the symbol (no matter long or short).

            Returns:
                int: the number of currently opened positions.
        """
        return max(self._long_positions, self._short_positions)

    def is_long(self):
        """
            Indicated if currently opened positions are long.

            Returns:
                bool: True if there are at least one long position opened, false otherwise.

            Raises:
                BackTestError: long and short positions are opened the same time for the same symbol.
        """
        if self._long_positions > 0 and self._short_positions > 0:
            raise BackTestError(f"Can not hold long and short positions for {self.data().get_title()} the same time: long - {self._long_positions}, short - {self._short_positions}")

        return self._long_positions > 0

    def get_margin_positions(self):
        """
            Get the number of margin positions.

            Returns:
                int: the number of currently opened margin positions for the corresponding symbol.
        """
        if self.is_long():
            return self._long_positions - self._long_positions_cash
        else:
            return self._short_positions

    def get_datetime_str(self, index=None):
        """
            Get the datetime string for the current index or for a particular offset

            Returns:
                str: datetime string for the current position or specified offset.
        """
        if index == None:
            index = self.get_caller_index()

        return self.data().get_rows()[index][Rows.DateTime]

    def get_datetime(self, index=None):
        """
            Get the datetime for the current index or for a particular offset

            Returns:
                DateTime: datetime for the current position or specified offset.

            Raises:
                BackTestError: incorrect datetime presents in the provided data.
        """
        if index == None:
            index = self.get_caller_index()

        dt_str = self.get_datetime_str(index)

        try:
            dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            raise BackTestError(f"The date {dt_str} is incorrect: {e}") from e

        return dt

    def get_year(self):
        """
            Get the current year during the calculation.

            Returns:
                The current year of the calculation.
        """
        return self.get_datetime().year

    # Get current quote
    def get_quote(self):
        """
            Get the current quote.

            Returns:
                float: the quote at the current index of the calculation.
        """
        return self.data().get_rows()[self.get_caller_index()][Rows.AdjClose]

    def apply_margin_fee(self):
        """
            Apply margin fees for the current day of the calculation for the particular symbol.
        """
        if self.get_margin_positions() and self.get_caller().did_day_changed():
            margin_fee = self.get_daily_margin_expenses()

            self.get_caller().add_debt_expense(margin_fee)
            # Assume that slightly negative cash balance is possible on a margin account
            self.get_caller().add_cash (-abs(margin_fee))

    def get_daily_margin_expenses(self):
        """
            Get daily margin expenses for the corresponding symbol.

            Returns:
                float: current daily margin expenses for the symbol.
        """
        return self.get_margin_positions() * self.get_quote() * self.data().get_margin_fee() / 100 / 240

    def get_spread_deviation(self):
        """
            Get the current spread deviation. Buy price is the current quote plus the deviation, sell otherwise.

            Returns:
                float: the spread deviation for the corresponding symbol.
        """
        return self.get_quote() * self.data().get_spread() / 100 / 2

    def get_buy_price(self):
        """
            Get the buy price of the current symbol in the current cycle of the calculation.

            Returns:
                float: the buy price of the symbol
        """
        return self.get_quote() + self.get_spread_deviation()

    def get_sell_price(self):
        """
            Get the sell price of the current symbol in the current cycle of the calculation.

            Returns:
                float: the sell price of the symbol.
        """
        return self.get_quote() - self.get_spread_deviation()

    def get_long_positions(self):
        """
            Get the number of long positions.

            Returns:
                int: the number of currently opened long positions for the symbol.
        """
        return self._long_positions

    def get_short_positions(self):
        """
            Get the number of short positions.

            Returns:
                int: the number of currently opened short positions for the symbol.
        """
        return self._short_positions

    def reset_trade_prices(self):
        """
            Reset trade prices used in the current cycle.
        """
        self._trade_price_long = None
        self._trade_price_short = None
        self._trade_price_margin = None

    def get_caller(self):
        """
            Get the backtesting class instance which called the data class.

            Returns:
                BackTest: the 'main' backtesting class instance.
        """
        return self.__caller

    def get_caller_index(self):
        """
            Get the index of the current calculation.

            Returns:
                int: index of the record in the dataset used for calculation.
        """
        return self.__caller.get_index()

    def apply_days_counter(self, days_delta):
        """
            Applies days counter for the calculation to a particular symbol.

            It may be used for a specific financial instruments. For example, days counter is used to estimate a
            dividend yield date. The function is supposed to be overloaded in a particular financial instument implementation.

            Args:
                days_delta(int): number of days to apply to the counter.
        """
        pass

    def get_total_value(self):
        """
            Get the total value of positions opened for the particular symbol.

            Returns:
                float: the total value of the all opened positions.
        """
        total_value = 0

        if self.is_long():
            total_value += self.get_sell_price() * self._long_positions_cash
            for j in range(self.get_margin_positions()):
                total_value += self.get_sell_price() - self._portfolio[j]
        else:
            for j in range(self._short_positions):
                total_value += self._portfolio[j] - self.get_buy_price()

        return total_value

    def get_symbol_result(self, value=None):
        """
            Generate symbol-specific results for the current cycle.
        """
        symbol_result = [
            self.data().get_title(),
            self.get_quote(),
            self._trade_price_long,
            self._trade_price_short,
            self._trade_price_margin,
            self._long_positions,
            self._short_positions,
            self.get_margin_positions(),
            self._trades_no]

        if value != None:
            symbol_result.append(value)
        else:
            symbol_result.append(self.get_value())

        return symbol_result

    def trend_changed(self, is_uptrend):
        """
            Checks if we consider that the signal to buy/sell has changed in the current cycle.

            Returns:
                bool: True if the signal to buy/sell is considered as changed, false otherwise
        """
        quote = self.get_quote()
        index = self.get_caller().get_index()

        if is_uptrend == self.is_long():
            self._signal_quote = None
            self._signal_index = None
            
            return False
        else:
            if self._signal_quote == None:
                self._signal_quote = quote

            if self._signal_index == None:
                self._signal_index = index

        if index - self._signal_index >= self.data().get_trend_change_period():
            self._signal_quote = None

            return True

        max_quote = max(quote, self._signal_quote)
        min_quote = min(quote, self._signal_quote)

        if max_quote / min_quote >= 1 + (self.data().get_trend_change_percent() / 100):
            self._signal_quote = None

            return True

        # Indicates that the function returned true earlier in this cycle but was called once again
        if self._signal_quote == None and index == self._signal_index:
            return True

        return False

    #########################################
    # Methods related to margin calculations.
    #########################################

    def get_margin_buying_power(self):
        """
            Get margin buying power based on opened long positions of the corresponding symbol.

            Returns:
                float: the buying power based on the long positions opened for the corresponding symbol.
        """
        return self._long_positions_cash * self.get_quote() * self.data().get_margin_rec()

    def get_future_margin_buying_power(self):
        """
            Gets buying power if we open the maximum possible number of positions using cash.

            Returns:
                float: the possible buying power if we open the maximum number of positions of the corresponding symbol.
        """
        return self.get_shares_num_cash() * self.get_quote() * self.data().get_margin_rec()

    def get_used_margin(self):
        """
            Get how much margin buying power are used by the current positions.

            Returns:
                float: the current used margin by the corresponding symbol.
        """
        return self.get_quote() * self.get_margin_positions()

    def get_margin_limit(self):
        """
            Get the current margin limit to open the positions based on the current opened long positions.

            Returns:
                float: the current margin limit.
        """
        return self._long_positions_cash * self.get_quote() * self.data().get_margin_req()

    def check_margin_requirements(self):
        """
            Check margin requirements related to this position only. Close the positions exceeding margin limit.
        """
        if self.get_margin_positions() > 0:
            deficit = self.get_used_margin() - self.get_caller().get_margin_limit() - self.get_margin_limit()

            if deficit:
                # Close margin positions to meet margin requirement
                shares_num = 0

                # Estimate how many positions we need to close to meet the margin requirement
                while deficit > 0:
                    shares_num += 1

                    try:
                        portfolio_price = self._portfolio[self.get_margin_positions() - shares_num]

                        if self.is_long():
                            deficit -= self.get_sell_price() - portfolio_price
                        else:
                            deficit -= portfolio_price - self.get_buy_price()
                    except IndexError:
                        # Bankrupt
                        break

                # Close the positions which exceed margin requirement
                self.close(shares_num)

    #######################################
    # Methods related to opening positions.
    #######################################

    def get_shares_num_cash(self):
        """
            Get the maxumum number of shares which we can buy using the cash balance without going negative.

            Return:
                int: the maximum of positions to open using cash only.
        """
        shares_num_estimate = int((self.get_caller().get_cash() - self.get_total_fee()) / self.get_buy_price())
        cash_available = self.get_caller().get_cash() - self.get_caller().get_commission() - self.get_share_fee() * shares_num_estimate
        shares_num_cash = int((cash_available) / self.get_buy_price())

        return shares_num_cash

    def get_shares_num_margin(self):
        """
            Get number of shares which we can buy using margin.

            Returns:
                int: the maxumum number of shares to buy using margin.
        """
        return int(self.get_future_margin_buying_power() / self.get_buy_price())

    def get_total_shares_num(self):
        """
            Get total number of shares which we may buy.

            Returns:
                int: the total number of shares which we can buy using both cash and margin.
        """
        return self.get_shares_num_cash() + self.get_shares_num_margin()

    def open_long(self, num):
        """
            Open the specified number of long position.

            Args:
                num(int): the number of shares to buy.

            Raises:
                BackTestError: not enough cash/margin to open the position.
                BackTestError: Can't open the negative number of positions.
        """
        if num < 0:
            raise BackTestError(f"Can't open negative number of long positions: {num}")

        if num > self.get_total_shares_num():
            raise BackTestError(f"Not enough cash/margin to open the position. {num} > {self.get_total_shares_num()}")

        if num == 0:
            return

        shares_num_cash = min(num, self.get_shares_num_cash())
        shares_num_margin = max(0, num - shares_num_cash)

        total_buying_fee = self.get_share_fee() * num + self.get_caller().get_commission()
        total_spread_expense = self.get_spread_deviation() * num
        total_cash_price = self.get_buy_price() * shares_num_cash

        self.get_caller().add_cash(-abs(total_buying_fee + total_cash_price))
        self._long_positions += num
        self._long_positions_cash += shares_num_cash

        self._portfolio.extend(repeat(self.get_buy_price(), shares_num_margin))

        # Add expenses for this trade
        self.get_caller().add_commission_expense(total_buying_fee)
        self.get_caller().add_spread_expense(total_spread_expense)

        self._trades_no += 1
        self.get_caller().add_total_trades(1)

        self._trade_price_long = self.get_buy_price()

    def get_total_shares_num_short(self):
        """
            Get the total number of shares which we can short.

            Returns:
                int: the total number of shares which we can short.
        """
        return int(self.get_caller().get_available_margin(self.get_total_fee()) / self.get_sell_price())

    def open_short(self, num):
        """
            Open the short position.

            Args:
                num(int): the number of shares to short.

            Raises:
                BackTestError: not enough cash/margin to open the position.
                BackTestError: Can't open the negative number of positions.
        """
        if num > self.get_total_shares_num_short():
            raise BackTestError(f"Not enough margin to buy {num} shares. Available margin is for {self.get_total_shares_num_short()} shares only.")

        if num < 0:
            raise BackTestError(f"Can't open negative number of short positions: {num}")

        if num == 0:
            return

        # Assume that slightly negative cash balance is possible on a margin account
        self.get_caller().add_cash(-abs(self.get_share_fee() * num + self.get_caller().get_commission()))
        self._short_positions += num

        self._portfolio.extend(repeat(self.get_sell_price(), num))

        # Calculate expenses for this trade
        self.get_caller().add_commission_expense(self.get_caller().get_commission() + self.get_share_fee() * num)
        self.get_caller().add_spread_expense(self.get_spread_deviation() * num)

        self._trades_no += 1
        self.get_caller().add_total_trades(1)
        self._trade_price_short = self.get_sell_price()

    # Open maxumum possible positions
    def open_long_max(self):
        """
            Open maximum possible number of long positions.
        """
        self.open_long(self.get_total_shares_num())

    def open_short_max(self):
        """
            Open maximum possible number of short positions.
        """
        self.open_short(self.get_total_shares_num_short())

    #######################################
    # Methods related to closing positions.
    #######################################

    def close(self, num):
        """
            Close the number of positions. No matter long or short.

            Args:
                num(int): the number of positions to close.

            Raises:
                BackTestError: trying to close a negative number of positions.
        """
        if num < 0:
            raise BackTestError(f"Number of positions to close can't be less than 0. {num} is specified.")

        if num == 0:
            return

        if self.is_long():
            self.close_long(num)
        else:
            self.close_all_short()

    def close_long(self, num):
        """
            Close the number of long positions.

            Args:
                num(int): the number of positions to close.

            Raises:
                BackTestError: to many positions to close.
        """
        if num > self._long_positions:
            raise BackTestError(f"Number of long positions to close is bigger than the number of actual positions: {num} > {self._long_positions}")

        if self._long_positions == 0:
            return

        cash_positions = min(num, self._long_positions_cash)
        margin_positions = 0

        # Check if we have at least one margin position to close
        if num > self._long_positions_cash:
            margin_positions = num - self._long_positions_cash

        total_commission = self.get_share_fee() * num + self.get_caller().get_commission()

        self._trade_price_long = self.get_sell_price()

        # Close cash long positions
        self.get_caller().add_cash(self.get_sell_price() * cash_positions)
        self.get_caller().add_cash(-abs(total_commission))
        
        self.get_caller().add_commission_expense(total_commission)
        self.get_caller().add_spread_expense(self.get_spread_deviation() * self._long_positions)

        # Close margin long positions
        delta = 0

        for _ in range(margin_positions):
            delta += self.get_sell_price() - self._portfolio.pop()

        self._long_positions -= num
        self._long_positions_cash -= cash_positions

        self.get_caller().add_cash(delta)

        self._trades_no += 1
        self.get_caller().add_total_trades(1)

    def close_short(self, num):
        """
            Close the number of short positions.

            Args:
                num(int): the number of positions to close.

            Raises:
                BackTestError: too many positions to close.
        """
        if num > self._short_positions:
            raise BackTestError(f"Number of short positions to close is bigger than the number of actual positions: {num} > {self._short_positions}")

        if self._short_positions == 0:
            return

        delta = 0

        for _ in range (num):
            delta += self._portfolio.pop() - self.get_buy_price()
            # Assume that slightly negative cash balance is possible on a margin account
            self.get_caller().add_cash(-abs(self.get_share_fee()))

            self.get_caller().add_commission_expense(self.get_share_fee())
            self.get_caller().add_spread_expense(self.get_spread_deviation())

        self.get_caller().add_commission_expense(self.get_caller().get_commission())

        self._short_positions -= num

        self.get_caller().add_cash(delta)

        self._trades_no += 1
        self.get_caller().add_total_trades(1)

        self._trade_price_short = self.get_buy_price()

    def close_all_long(self):
        """
            Close all long positions.
        """
        self.close_long(self._long_positions)

    def close_all_short(self):
        """
            Close all short positions.
        """
        self.close_short(self._short_positions)

    def close_all(self):
        """
            Close all positions.
        """
        self.close(self.get_max_positions())

################################################
# Classes for data structures used in reporting.
################################################

class BTData():
    """
        The class used in charting which represents the whole portfolio.
    """
    def __init__(self):
        self.DateTime = None
        self.TotalValue = None
        self.Deposits = None
        self.Cash = None
        self.Borrowed = None
        self.OtherProfit = None
        self.CommissionExpense = None
        self.SpreadExpense = None
        self.DebtExpense = None
        self.OtherExpense = None
        self.TotalExpenses = None
        self.TotalTrades = None
        self.Symbols = None

class BTSymbol():
    """
        The class used in charting which represents the particular symbol used in the strategy. More than one symbols may be used.
    """
    def __init__(self):
        self.Title = None
        self.Quote = None
        self.TradePriceLong = None
        self.TradePriceShort = None
        self.TradePriceMargin = None
        self.LongPositions = None
        self.ShortPositions = None
        self.MarginPositions = None
        self.TradesNo = None
        self.Tech = None

########################
# Back backtesting class
########################

class BackTest(metaclass=abc.ABCMeta):
    def __init__(self,
                 data,
                 commission=0,
                 commission_percent=0,
                 commission_share=0,
                 initial_deposit=0,
                 periodic_deposit=0,
                 deposit_interval=00,
                 inflation=0,
                 margin_req=0,
                 margin_rec=0,
                 offset=0,
                 timeout=10
        ):
        """
            The main backtesting class.

            Args:
                data(list of BackTestData): the list of data classes for calculation.
                commission(float): commission per trade.
                commission_percent(float): commission in percent of the trade volume.
                commission_share(float): commission per share.
                initial_deposit(float): initial deposit to test the strategy.
                periodic_deposit(float): periodic deposit to the account.
                deposit_interval(int): interval (in days) to add a periodic deposit to the account.
                inflation(float): annual inflation used in the calculation.
                margin_req(float): determines the buying power of the cash balance for a margin account.
                margin_rec(float): determines the recommended buying power of the cash balance for a margin account.
                offset(int): the offset for the calculation.
                timeout(int): timeout in seconds to cancel the calculation if some thread can not finish in time.

            Raises:
                BackTestError: incorrect arguments.
        """

        ####################################################
        # Setting protected variables used for a calculation
        ####################################################

        # Data to perform a backtesting
        self.__data = data

        # Commission for a trade (flat rate)
        if commission < 0:
            raise BackTestError(f"commission can't be less than 0. Specified value is {commission}")
        self._commission = commission

        # Commission for a trade (in percent of the sym of order)
        if commission_percent < 0 or commission_percent > 100:
            raise BackTestError(f"commission_percent can't be less than 0% or more than 100%. Specified value is {commission_percent}")
        self._commission_percent = commission_percent

        # Commission per share
        if commission_share < 0:
            raise BackTestError(f"commission_share can't be less than 0. Specified value is {commission_share}")
        self._commission_share = commission_share

        # Initial deposit
        if initial_deposit < 0:
            raise BackTestError(f"Initial assets can't be less than 0. Specified value is {initial_deposit}")
        self._initial_deposit = initial_deposit

        # Monthly deposit (in the currency of the instrument)
        if periodic_deposit < 0:
            raise BackTestError(f"periodic_deposit can't be less than 0. Specified value is {periodic_deposit}")
        self._periodic_deposit = periodic_deposit
    
        # Deposit interval (days)
        if deposit_interval < 0:
            raise BackTestError(f"deposit_interval can't be less than 0. Specified value is {deposit_interval}")
        self._deposit_interval = deposit_interval

        # Annual inflation (in percent) to correct the periodic deposit
        if inflation < 0 or inflation > 100:
            raise BackTestError(f"inflation can't be less than 0% or more than 100%. Specified value is {inflation}")
        self._inflation = inflation

        # Required loan to cash ratio. For example, if loan to cash ratio is 0.7 and current cash is $1000,
        # then at maximum $7000 may be lended by a broker. In case if maximum margin limit is hit, margin call is possible.
        if margin_req < 0:
            raise BackTestError(f"margin_req can't be less than 0. Specified value is {margin_req}")
        self._margin_req = margin_req

        # Recommended loan to cash ratio. Backtesting engine won't try to exceed it. Exceeding this limit
        # is still acceptable but is considered at potentially dangerous and may lead to a margin call.
        if margin_rec < 0:
            raise BackTestError(f"margin_rec can't be less than 0. Specified value is {margin_rec}")
        self._margin_rec = margin_rec

        # Recommended loan to cash should be less than required
        if margin_rec > margin_req:
            raise BackTestError(f"load_to_asset_rec should be less than margin_req, however {margin_rec} is not < {margin_req}")

        # Offset for calculation
        if offset < 0:
            raise BackTestError(f"offset can't be less than 0. Specified value is {offset}")
        self._offset = offset

        # Timeout for calculations
        if timeout < 0:
            raise BackTestError(f"timeout can't be less than 0. Specified value is {timeout}")
        self.__timeout = timeout        

        #############################
        # Now internal variables are listed which are used in a calculation. They are added to the results list
        # per each timespan period.
        #############################

        # Current cash available
        self._cash = self._initial_deposit
        # Total deposits
        self._deposits = self._cash

        # Profit obtained by dividends and coupon.
        self._other_profit = 0

        #########################################
        # Typical expenses for an active strategy
        #########################################

        # Expenses spend on commission
        self._commission_expense = 0
        # Expenses caused by spread
        self._spread_expense = 0
        # Costs of lending money from a broker
        self._debt_expense = 0
        # Expenses caused paying dividends of lended securities
        self._other_expense = 0

        #######################################
        # General data for backtesting strategy
        #######################################

        # Total number of trades
        self._total_trades = 0

        # Results of the calculation
        self._results = []

        # Counter till deposit date
        self._deposit_counter = 0

        # Year of the calculations
        self._year = None

        # Index for calculations
        self.__index = None

        # Indicates if strategy setup has already been set up
        self.__is_setup = False

        # Indicates if the current cycle is being calculated
        self.__is_active = False

        # Indicates if calculation is finished
        self.__is_finished = False

        # Instances for calculations
        self.__exec = []

        # Indicates if multiple symbol data should be expected
        self._is_multi = False

        ###################################################
        # Properties related to multithreading calculations
        ###################################################

        # Event which indicates the finishing of calculation
        self.__event = None

        # Separate thread for calculation
        self.__thread = None

        # Threads for technical indicators calculations
        self.__threads = None

    #############
    # Functions
    #############

    def is_finished(self):
        """
            Indicates if the calculation is finished.

            Returns:
                True if calculation is finished, False otherwise.
        """
        return self.__is_finished

    def get_initial_deposit(self):
        """
            Get the initial deposit.

            Returns:
                float: the initial deposit for the calculation.
        """
        return self._initial_deposit

    def get_raw_results(self):
        """
            Get the result list of the calculation.

            Returns:
                list: the results of the calculation.

            Reises:
                BackTestError: results were requested but calculation is not performed.
        """
        if self.__event == None:
            raise BackTestError("Calulation was not performed.")

        result = self.__event.wait(self.__timeout)

        if self.__thread != None:
            self.__thread.join()
            self.__thread = None

        if self.__threads != None:
            for thread in self.__threads:
                thread.join()

        self.__threads = None

        if result == False:
            raise BackTestError(f"Timeout ({self.__timeout} sec) has happened. Calculation is not finished.")

        return self._results

    def get_results(self):
        """
            Get the results as BTData and BTSymbol instances for a more convenient reporting.

            Returns:
                BTData: results of the calculation.
        """
        # Get list with data
        self.get_raw_results()

        # Data structures classes preparation

        bt_data = BTData()

        bt_data.DateTime = [row[BTDataEnum.DateTime] for row in self._results]
        bt_data.TotalValue = [row[BTDataEnum.TotalValue] for row in self._results]
        bt_data.Deposits = [row[BTDataEnum.Deposits] for row in self._results]
        bt_data.Cash = [row[BTDataEnum.Cash] for row in self._results]
        bt_data.Borrowed = [row[BTDataEnum.Borrowed] for row in self._results]
        bt_data.OtherProfit = [row[BTDataEnum.OtherProfit] for row in self._results]
        bt_data.CommissionExpense = [row[BTDataEnum.CommissionExpense] for row in self._results]
        bt_data.SpreadExpense = [row[BTDataEnum.SpreadExpense] for row in self._results]
        bt_data.DebtExpense = [row[BTDataEnum.DebtExpense] for row in self._results]
        bt_data.OtherExpense = [row[BTDataEnum.OtherExpense] for row in self._results]
        bt_data.TotalExpenses = [row[BTDataEnum.TotalExpenses] for row in self._results]
        bt_data.TotalTrades = [row[BTDataEnum.TotalTrades] for row in self._results]

        bt_data.Symbols = []

        for i in range(len(self.all_exec())):
            bt_symbol = BTSymbol()

            bt_symbol.Title = [row[BTDataEnum.Symbols][i][BTSymbolEnum.Title] for row in self._results]
            bt_symbol.Quote = [row[BTDataEnum.Symbols][i][BTSymbolEnum.Quote] for row in self._results]
            bt_symbol.TradePriceLong = [row[BTDataEnum.Symbols][i][BTSymbolEnum.TradePriceLong] for row in self._results]
            bt_symbol.TradePriceShort = [row[BTDataEnum.Symbols][i][BTSymbolEnum.TradePriceShort] for row in self._results]
            bt_symbol.TradePriceMargin = [row[BTDataEnum.Symbols][i][BTSymbolEnum.TradePriceMargin] for row in self._results]
            bt_symbol.LongPositions = [row[BTDataEnum.Symbols][i][BTSymbolEnum.LongPositions] for row in self._results]
            bt_symbol.ShortPositions = [row[BTDataEnum.Symbols][i][BTSymbolEnum.ShortPositions] for row in self._results]
            bt_symbol.MarginPositions = [row[BTDataEnum.Symbols][i][BTSymbolEnum.MarginPositions] for row in self._results]
            bt_symbol.TradesNo = [row[BTDataEnum.Symbols][i][BTSymbolEnum.TradesNo] for row in self._results]
            bt_symbol.Tech = [row[BTDataEnum.Symbols][i][BTSymbolEnum.Tech] for row in self._results]

            bt_data.Symbols.append(bt_symbol)

        return bt_data

    def get_prev_dt(self):
        """
            Get the DateTime of the previous cycle.

            Returns:
                DateTime: the DateTime of the previous cycle.
        """
        prev_index = self.get_index() - 1

        if self.skipped(prev_index) == False:
            return self.exec().get_datetime(prev_index)

        return None

    def get_days_delta(self):
        """
            Get days delta betwen the current and previous cycle.
            For example, days delta between two working days will be 1, days delta between two trading dates with a holiday
            between them may be much bigger.

            Returns:
                int: Days delta between two trading days.
        """
        days_delta = 0
        prev_dt = self.get_prev_dt()

        if prev_dt != None:
            delta = self.exec().get_datetime() - prev_dt
            days_delta = delta.days

        return days_delta

    def adjust_days_delta(self):
        """
            Adjust days delta for counters.
        """
        days_delta = self.get_days_delta()

        for ex in self.__exec:
            ex.apply_days_counter(days_delta)

        self._deposit_counter += days_delta

    def did_day_changed(self):
        """
            Indicated if day has changed between two trading cycles.
            For example, day won't change between two intraday cycles which happened the same day.

            Returns:
                True if day has changed, False otherwise.
        """
        return self.get_days_delta() > 0

    def deposit(self):
        """
            Check if periodic deposit should be added to the balance and adds it if there is a need.
        """
        # Check if the deposit should be inflation adjusted
        current_year = self.exec().get_year()

        if self._inflation != 0 and self._year != current_year:
            self._year = current_year
            self._periodic_deposit = self._periodic_deposit + self._periodic_deposit * (self._inflation / 100)

        # Check if we make a regular deposit today
        if self._periodic_deposit != 0 and self._deposit_interval <= self._deposit_counter:
            self._cash += self._periodic_deposit
            self._deposits += self._periodic_deposit
            self._deposit_counter = 0

    def is_multi_symbol(self):
        """
            Check if data for several symbols was added during the initialization.

            Returns:
                True is data for several symbols presents, False otherwise.
        """
        return len(self.get_data()) > 1

    def get_main_data(self):
        """
            Get the first BackTestData instance used in the calculation. It is considered as the 'main' one.

            Returns:
                BackTestData: the first instance added during initialization.
        """
        return self.get_data()[0]

    def get_data(self):
        """
            Get all BackTestData instances used by the calculation.

            Retunrs:
                list of BackTestData: all the instances used in the calculation.
        """
        return self.__data

    def set_index(self, index):
        """
            Set current index for a calculations.

            Args:
                int: index for calculation.

            Raises:
                BackTestError: index not found.
        """
        if index >= len(self.get_main_data().get_rows()):
            raise BackTestError(f"Provided data does not have index {index}")

        self.__index = index

    def get_index(self):
        """
            Get current index for a calculations.

            Returns:
                int: index for calculation.
        """
        return self.__index

    def skipped(self, index=None):
        """
            Check if the current cycle should be skipped.
            Skipping criteria is set by the offset of by other factors used in the strategy (period and so on.)

            Returns:
                True if the cycle should be skipped, False otherwise.
        """
        if index == None:
            index = self.get_index()

        if index < 0:
            return True

        return self.skip_criteria(index) or index < self.get_offset()

    def to_skip(self):
        """
            Check if the cycle must be skipped and skip if it is neccessary.

            Returns:
                True if the cycle was skipped, False otherwise.
        """
        if self.skipped():
            self._results.append([None] * len(BTDataEnum))
            self._results[self.get_index()][BTDataEnum.DateTime] = self.exec().get_datetime_str()

            symbol_rows = []

            for ex in self.all_exec():
                symbol_row = []

                symbol_row.extend(repeat(None, len(BTSymbolEnum)))
                symbol_row[BTSymbolEnum.Title] = ex.data().get_title()
                symbol_row[BTSymbolEnum.Quote] = ex.get_quote()

                symbol_rows.append(symbol_row)

            self._results[self.get_index()][BTDataEnum.Symbols] = symbol_rows

            return True

        return False

    def get_offset(self):
        """
            Get the offset of the calculation.

            Returns:
                int: the offset of the calculation.
        """
        return self._offset

    def get_commission(self):
        """
            Get the commission per trade.

            Returns:
                float: the commission per trade used in the calculation.
        """
        return self._commission

    def get_commission_percent(self):
        """
            Get the commission in percent of a trade volume.

            Returns:
                float: the commission in percent of a trade volume. used in the calculation.
        """
        return self._commission_percent

    def get_commission_share(self):
        """
            Get the commission per share.

            Returns:
                float: the commission per share used in the calculation.
        """
        return self._commission_share

    def add_cash(self, cash):
        """
            Add cash to the balance.

            Args:
                float: cash to add.
        """
        self._cash += cash

    def add_other_profit(self, other_profit):
        """
            Add other profit for to statistics.

            Args:
                float: other profit to add to the statistics.
        """
        self._other_profit += other_profit
        self.add_cash(other_profit)

    def add_other_expense(self, other_expense):
        """
            Add other expense to the statistics.

            Args:
                other_expense(float): other expense to add to the statistics.
        """
        self._other_expense += other_expense
        # TODO Need to check here if balance may go negative on a non-margin account.
        # Currently it is not relevant because the stock is the only financial intrument where other expenses may be applied
        # and it happens only on a margin account (dividend expenses while holding a short position).
        self.add_cash(-abs(other_expense))

    def add_debt_expense(self, debt_expense):
        """
            Add debt(margin) expense to the statistics.

            Args:
                float: debt(margin) expense to add to the statistics.
        """
        self._debt_expense += debt_expense

    def add_total_trades(self, num):
        """
            Add total trades number to the statistics.

            Args:
                int: total trades number to add to the statistics.
        """
        self._total_trades += num

    def add_commission_expense(self, expense):
        """
            Add commission expense to the statistics.

            Args:
                float: commission expense to add to the statistics.
        """
        self._commission_expense += expense

    def add_spread_expense(self, expense):
        """
            Add spread expense to the statistics.

            Args:
                float: spread expense to add to the statistics.
        """
        self._spread_expense += expense

    def add_result(self, result):
        """
            Add cycle's result to the calculation.

            Args:
                list: list of the calculation of the current cycle.
        """
        self._results.append(result)

    def get_margin_req(self):
        """
            Get the required margin ratio for the cash balance. For example, if the cash balance is 1000 and required margin ratio
            is 0.9, then the buying power will be 9000. In the case of this value is exceeded (also if opened long positions do not
            provide enough margin as well), margin call will happen and positions will be partially closed.

            Returns:
                float: cash to margin required ratio.
        """
        return self._margin_req

    def get_margin_rec(self):
        """
            Get the recommended margin ratio for the cash balance. For example, if the cash balance is 1000 and recommended margin ratio
            is 0.7, then the backtesting engine will open margin positions not exceeding 7000. In the case of this value is exceeded
            (also if opened long positions do not provide enough margin as well), margin call will NOT happen until the required ratio is met.

            Returns:
                float: cash to margin recommended ratio.
        """
        return self._margin_rec

    def get_cash(self):
        """
            Get the cash balance.

            Returns:
                float: the current cash balance.
        """
        return self._cash

    def get_total_trades(self):
        """
            Get the total number of simulated trades at the moment.

            Retunrs:
                int: the current number of simulated trades.
        """
        return self._total_trades

    def get_margin_buying_power(self, fees=0):
        """
            Get margin buying power based on the cash balance.

            Returns:
                float: the current margin buying power based on the cash balance.
        """
        return (self.get_cash() - fees) * self.get_margin_rec()

    def get_margin_limit(self):
        """
            Get the current margin limit based on the cash balance.
            If the limit will be exceeded and there are no long positions to provide additional buying power, margin call
            will happen and exceeding positions will be closed.

            Returns:
                float: the current margin limit based on the cash balance.
        """
        return self.get_cash() * self.get_margin_req()

    def get_total_used_margin(self):
        """
            Get the total used margin.

            Returns:
                float: the total used margin.
        """
        used_margin = 0

        for ex in self.__exec:
            used_margin += ex.get_used_margin()

        return used_margin

    def get_total_margin(self):
        """
            Get the total margin buying power.

            Returns:
                float: the total margin buying power.
        """
        total_margin = 0

        for ex in self.__exec:
            total_margin += ex.get_margin_buying_power()

        return total_margin

    def get_available_margin(self, fees=0):
        """
            Get the total available margin.

            Args:
                fees(float): the fees to substract from the amount of margin.

            Returns:
                float: the total available margin.
        """
        return self.get_margin_buying_power(fees) + self.get_total_margin() - self.get_total_used_margin()

    def get_total_deposits(self):
        """
            Get the total depositted money (initial deposit plus all periodic).

            Returns:
                float: the amount of total money depositted.

        """
        return self._deposits

    def get_other_profit(self):
        """
            Get the current other profit.

            Returns:
                float: the current other profit.
        """
        return self._other_profit

    def get_commission_expense(self):
        """
            Get the current commission expense.

            Returns:
                float: the current commission expense.
        """
        return self._commission_expense

    def get_spread_expense(self):
        """
            Get the current spread expense.

            Returns:
                float: the current spread expense.
        """
        return self._spread_expense

    def get_debt_expense(self):
        """
            Get the current debt expense (margin fees).

            Returns:
                float: the current debt expense.
        """
        return self._debt_expense

    def get_other_expense(self):
        """
            Get the current other expense. For example, in the case of stock, other expense is the expense which was payed
            instead of dividends to a lender while holding a short position.

            Returns:
                float: the current other expense.
        """
        return self._other_expense

    def get_total_expenses(self):
        """
            Get the total expenses.

            Returns:
                float: the current total expenses.
        """
        return self.get_commission_expense() + self.get_spread_expense() + self.get_other_expense() + self.get_debt_expense()

    def get_total_value(self):
        """
            Get the total value of the portfolio

            Returns:
                float: the total value of the portfolio.
        """
        total_value = self.get_cash()

        for ex in self.__exec:
            total_value += ex.get_total_value()

        return total_value

    def get_result(self):
        """
            Get result of the current cycle.

            Returns:
                list: the result of the current cycle.
        """
        symbol_results = []

        for ex in self.__exec:
            symbol_results.append(ex.get_symbol_result())

        result = [
            self.exec().get_datetime_str(),
            self.get_total_value(),
            self.get_total_deposits(),
            self.get_cash(),
            self.get_total_used_margin(),
            self.get_other_profit(),
            self.get_commission_expense(),
            self.get_spread_expense(),
            self.get_debt_expense(),
            self.get_other_expense(),
            self.get_total_expenses(),
            self.get_total_trades(),
            symbol_results
        ]

        return result

    def generate_cycle_result(self):
        """
            Append the result of the current cycle to the results list.
        """
        self.add_result(self.get_result())

    def setup(self):
        """
            Perform setup for the entire calculation.

            Raises:
                BackTestError: setup was already performed.
                BackTestError: provided data does not correspond multi symbol expectation.
                BackTestError: data misintegrity found.
        """
        if self.__is_setup:
            raise BackTestError("Setup has been already performed.")

        if self.is_multi_symbol() != self._is_multi:
            raise BackTestError(f"Provided data does not correspond multi symbol expectation: {self._is_multi}")

        # Get the initial year
        self._year = self.get_main_data().get_first_year()

        for data in self.__data:
            self.__exec.append(self.create_exec(data))

        # Check date integrity for multi symbol strategies

        all_execs = len(self.all_exec())

        # Needs a bit tricky implementation to handle python's automatic copying of objects
        # At first we need to remove all the entries where DateTime does not present in each symbol's data
        if all_execs > 1:
            dts_to_remove = []

            for i in range(all_execs):
                for j in range(len(self.exec(i).data().get_rows())):
                    dt = self.exec(i).data().get_rows()[j][Rows.DateTime]

                    for ex in self.all_exec():
                        dts2 = [row2[Rows.DateTime] for row2 in ex.data().get_rows()]
                        
                        if dt not in dts2:
                            dts_to_remove.append(dt)

            for dt in dts_to_remove:
                for ex in self.all_exec():
                    for i in range(len(ex.data().get_rows())):
                        if ex.data().get_rows()[i][Rows.DateTime] == dt:
                            del ex.data().get_rows()[i]
                            break

            length = len(self.exec().data().get_rows())

            # Check data integrity
            for i in range(length):
                dt = self.exec().data().get_rows()[i][Rows.DateTime]

                for j in range(1, all_execs):
                    ex = self.exec(j)
                    ex_row = ex.data().get_rows()[i]
                    ex_dt = ex_row[Rows.DateTime]
                    if ex_dt != dt:
                        raise BackTestError(f"Date misintegrity found at index {i}. {dt} of {self.exec().data().get_title()} != {ex_dt} of {ex.data().get_title()}")

        # Calculate technical data for each symbol
        self.calculate_all_tech()

        self.__is_setup = True

    def create_exec(self, data):
        """
            Create BackTestOperations instance based on BackTestData instance.
            BackTestData is a container for data used for calculation and the usage of every instance of this class is thread safe.
            Several BackTestOperations may be associated with a single BackTestData. BackTestOperations class is not thread safe
            and it represents an operations performed on a certain symbol in the portfolio.

            Args:
                BackTestData: backtesting data class

            Returns:
                BackTestOperations: instance for performing operations for a particular symbol in the portfolio.
        """
        exec_instance = data.create_exec(self)

        return exec_instance

    def all_exec(self):
        """
            Get all BackTestOperations instances used in the strategy.

            Returns:
                list of BackTestOperations: all instances of operation classes used in the strategy.
        """
        return self.__exec

    def exec(self, num=0, data=None):
        """
            Get the BackTestOperations instance to execute the calculations specific to a particular data.

            Args:
                num(int): index for the instance to return.
                data(BackTestData): data instance with the associated operations instance.

            Returns:
                BackTestOperations: operations instance.
        """
        instance = None

        if data != None:
            try:
                num = self.get_data().index(data)
            except ValueError as e:
                raise BackTestError(f"Can't find the specified data insance: {e}") from e

        try:
            instance = self.__exec[num]
        except IndexError as e:
            raise BackTestError(f"Can not find the instance with index {num}.") from e

        return instance

    def do_cycle(self, index):
        """
            Setup the current calculation cycle.

            Args:
                index(int): the index of the cycle.

            Returns:
                True if calculation was performed, False if the cycle was skipped.

            Raises:
                BackTestError: The setup wasn't called previously.
                BackTestError: The calculation has already finished.
                BackTestError: Index does not found.
                BackTestError: do_cycle was already called in this cycle.
        """
        if self.__is_setup == False:
            raise BackTestError("The setup wasn't called previously.")

        if self.__is_finished:
            raise BackTestError("The calculation has already finished.")

        if index >= len(self.get_main_data().get_rows()):
            raise BackTestError(f"Provided data does not have index {index}")

        # do_cycle() was already called for this cycle.
        if self.get_index() == index or self.__is_active:
            raise BackTestError("do_cycle was already called in this cycle.")

        self.set_index(index)

        if self.to_skip():
            return False

        # Set this cycle as active
        self.__is_active = True

        # Reset symbol specific-data
        for ex in self.__exec:
            ex.reset_trade_prices()

        # Calculate days delta between the cycles and check if day counter increased
        self.adjust_days_delta()

        # Check if we need to make a deposit today. Deposit if we need.
        self.deposit()

        # Calculate and apply margin expenses per day
        for ex in self.__exec:
            ex.apply_margin_fee()

        # Get current other profit/expense and apply it to the cash balance
        for ex in self.__exec:
            ex.apply_other_balance_changes()

        # Check if margin requirements are met
        for ex in self.__exec:
            ex.check_margin_requirements()

        return True

    def apply_other_balance_changes(self):
        """
            Apply other balance change (like dividends in case of stock and so on).
        """
        pass

    def tear_down(self):
        """
            Tear down the current calculation cycle.

            Raises:
                BackTestError: the cycle is not active.
        """
        if self.__is_active == True:
            self.generate_cycle_result()
            self.__is_active = False
        else:
            raise BackTestError("The current cycle is not active.")

        # Check if the calculation is finished
        if self.get_index() + 1 == len(self.get_main_data().get_rows()):
            self.__is_finished = True

    def calculate(self):
        """
            Perform the calculation of the entire strategy.
        """
        self.__event = Event()

        if thread_available():
            self.__thread = Thread(target=self.__do_calculation)
            self.__thread.start()
        else:
            self.__do_calculation()

    def __do_calculation(self):
        self.do_calculation()
        self.__event.set()

    def calculate_all_tech(self):
        """
            Calculate all the required technical data.
        """
        events = []
        self.__threads = []

        for ex in self.all_exec():
            stop_event = Event()

            if thread_available():
                thread = Thread(target=self.__do_tech_calculation, args=[ex, stop_event])
                self.__threads.append(thread)
                events.append(stop_event)
                thread.start()
            else:
                self.__do_tech_calculation(ex, stop_event)

        self.wait_till_threads_finish(events)

    def wait_till_threads_finish(self, events):
        """
            Waif for each calculation thread to finish.
        """
        for event in events:
            event.wait()

    def __do_tech_calculation(self, ex, event):
        # If eventually you get the following error in pandas source code in _format_argument_list() function
        # last = allow_args[-1]
        # IndexError: list index out of range
        # It is an interpreter bug. Just add a debug print at the beginning of that function like 
        # print(f"allow args is {allow_args}")
        # and it will work.
        self.do_tech_calculation(ex)
        event.set()

    ##########################
    # Abstract functions
    ##########################

    @abc.abstractmethod
    def do_calculation(self):
        """
            Perform backtest calculation."
        """
        pass

    @abc.abstractmethod
    def skip_criteria(self, index):
        """
            Estimate if we should skip the current cycle (no data for calculation and so on).

            Args:
                index(int): index of the cycle to calculate.

            Returns:
                True is the cycle should be skipped, False otherwise.
        """
        pass

    # Calculate technical data
    @abc.abstractmethod
    def do_tech_calculation(self, ex):
        """
            Perform technical data calculation for the strategy.
        """
        pass
