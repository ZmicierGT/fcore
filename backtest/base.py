"""Base module for backtesting strategies.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from data.fvalues import Quotes
from backtest.weigher import Weigher
from data.futils import thread_available, Log, add_column, get_dt

import abc
from collections import namedtuple
from dataclasses import dataclass, fields, astuple
from datetime import datetime
from enum import Enum, IntEnum
from itertools import repeat
import time
import numpy as np
from threading import Thread, Event
import math

# Enum class for backtesting results data order.
class BTDataEnum(IntEnum):
    """Enum to describe a list with backtesting result."""
    DateTime = 0
    TotalValue = 1
    Deposits = 2
    Cash = 3
    OtherProfit = 4
    CommissionExpense = 5
    SpreadExpense = 6
    OtherExpense = 7
    TotalExpenses = 8
    TotalTrades = 9

# Enum class for backtesting data regarding a particular symbol
class BTSymbolEnum(IntEnum):
    """Enum to describe a list with backtesting result for each particular symbol."""
    Open = 0
    Close = 1
    High = 2
    Low = 3
    PriceOpenLong = 4
    PriceCloseLong = 5
    LongPositions = 6
    TradesNo = 7

# Enum class to represent the lifecycle of a security during a simulation.
class SecState(Enum):
    """Enum to describe the current state of a security."""
    Active = 1  # The security is quoted and can be traded
    Delisted = 2  # The security stopped being quoted and its positions were written off
    Skipped = 3  # The ticker reappeared after delisting (likely another company) and must not be traded

# Dataclass for one cycle of portfolio results. Order mirrors BTDataEnum.
@dataclass
class BTDataRow:
    """One cycle of portfolio results. Order mirrors BTDataEnum."""
    date_time: str | None = None
    total_value: float | None = None
    deposits: float | None = None
    cash: float | None = None
    other_profit: float | None = None
    commission_expense: float | None = None
    spread_expense: float | None = None
    other_expense: float | None = None
    total_expenses: float | None = None
    total_trades: int | None = None

# Dataclass for one cycle of per-symbol results. Order mirrors BTSymbolEnum.
@dataclass
class BTSymbolRow:
    """One cycle of per-symbol results. Order mirrors BTSymbolEnum."""
    open: float | None = None
    close: float | None = None
    high: float | None = None
    low: float | None = None
    price_open_long: float | None = None
    price_close_long: float | None = None
    long_positions: int | None = None
    trades_no: int | None = None

# Charting bar: either a set of quote columns to build the bar from, or the bar values themselves
ChartingBar = namedtuple('ChartingBar', ['open', 'high', 'low', 'close'])

# Dataclass representing the statistics of a backtest calculation
@dataclass
class Statistics():
    """The dataclass with the statistics of a backtest calculation.

        Provides structured access to the individual metrics (via fields) and a formatted text form (via str()).
        The metrics mirror the annotations displayed on a report image by Report.add_annotations().
    """
    title: str = ""
    invested: float = 0
    total_value: float = 0
    profit: float = 0  # in percent
    other_profit: float = 0
    total_trades: float = 0
    total_expenses: float = 0
    commission_expense: float = 0
    spread_expense: float = 0
    other_expense: float = 0

    def __str__(self):
        """Get the statistics as a formatted multi-line text.

            The metrics are arranged in two columns mirroring the layout
            rendered on a report image by Report.add_annotations(): the
            performance metric on the left and the corresponding expense
            metric on the right of each row.

            Returns:
                str: the formatted statistics.
        """
        rows = [
            (f"Invested:     {round(self.invested, 2)}",
             f"Total expenses:     {round(self.total_expenses, 2)}"),
            (f"Total value:  {round(self.total_value, 2)}",
             f"Commission expense: {round(self.commission_expense, 2)}"),
            (f"Profit:       {round(self.profit, 2)}%",
             f"Spread expense:     {round(self.spread_expense, 2)}"),
            (f"Other profit: {round(self.other_profit, 2)}",
             f"Other expense:      {round(self.other_expense, 2)}"),
            (f"Total trades: {self.total_trades}", ""),
        ]

        col_width = 30
        lines = [f"{perf.ljust(col_width)}{exp}" if exp else perf for perf, exp in rows]

        title = f"{self.title}\n" if self.title else ""
        return f"{title}" + "\n".join(lines) + "\n"

class BackTestEvent(Event):
    """
        Class to represent a backtesting event.
    """
    def __init__(self, timeout, **kwargs):
        super().__init__(**kwargs)

        self.__timer = time.perf_counter()
        self.__timeout = timeout + self.__timer

    def time_left(self):
        """
            Get the remaining time before timeout happens.

            Returns:
                float: the remaining time in seconds.
        """
        return self.__timeout - time.perf_counter()

# Exception class for general backtesting errors
class BackTestError(Exception):
    """Class to represent an exception triggered during backtesting."""

# Data storage class for backtesting
class BackTestData():
    """Thread-safe class which represents data used in backtesting.
    
        This class is used as a base class for various financial instruments.
    """
    def __init__(self,
                 rows,
                 title='',
                 spread=0,
                 trend_change_period=0,
                 trend_change_percent=0,
                 timespan=None,
                 source=None,
                 weighted=True,
                 info=None,
                 lot=1,
                ):
        """Initializes BackTestData class.

            Args:
                rows(list): data for the particular symbol obtained from the database using fdata module.
                title(str): title of the symbol used in the class.
                spread(float): pre-defined spread for the symbol.
                trend_change_period(int): indicates the period in timespans when the trend for this symbol is considered as changed.
                    Default is 0.
                trend_change_percent(float): indicates the change of the quote in percent when the trend for the symbol is considered as changed
                    immediately. Default is 0.
                timespan(Timespan): time span used in data.
                source(string): data source.
                weighed(bool): indicates if the symbol is supposed to be weighted in a portfolio (market cap, equally etc.)
                info(dict): security profile information.
                lot(int): minimum lot size for a trade.

            Raises:
                BackTestError: inaproppriate values were provided.
        """

        if len(rows) == 0:
            raise BackTestError("Length of list with quotes can't be 0.")

        # Data for the calculation
        self._rows = rows

        # Spread (in percent). For more precise calculation, prefer to perform trades as limit orders.
        if spread < 0 or spread > 100:
            raise BackTestError(f"Spread can't be less than 0% or more than 100%. Specified value is {spread}")
        self._spread = spread

        # Indicates how many periods need to pass that we consider that the signal has changed
        if trend_change_period < 0:
            raise BackTestError(f"trend_change_period can't be less than 0%. Specified value is {trend_change_period}")
        self._trend_change_period = trend_change_period

        # Indicates quote change in percent when we consider that the signal changed immediately
        if trend_change_percent < 0:
            raise BackTestError(f"trend_change_percent can't be less than 0%. Specified value is {trend_change_percent}")
        self._trend_change_percent = trend_change_percent

        # Quote columns used to build a charting bar (raw prices for a general security)
        self._charting = ChartingBar(Quotes.Open, Quotes.High, Quotes.Low, Quotes.Close)

        # Title of the financial instrument
        self._title = title

        # Time span used in data
        self._timespan = timespan

        # Data source
        self._source = source

        # Security profile information.
        self._info = info

        self._weighted = weighted

        if lot < 1 or lot % 1 != 0:
            raise BackTestError(f"Lot can't be less than 1 or a decimal number: {lot}")

        self._lot = lot

    #####################
    # Properties
    #####################

    @property
    def charting(self):
        """
            Get the quote columns used to build a charting bar.

            The base class returns raw prices. Subclasses for security types with adjusted prices
            (e.g. stocks) override it to return adjusted columns. Used for reporting/charting and
            indicator calculation only - never for order execution or valuation.

            Returns:
                ChartingBar: the charting quote columns.
        """
        return self._charting

    @property
    def lot(self):
        """
            Get the minimum number of shares in a lot.

            Returns:
                float: the minimum number of shares in a lot.
        """
        return self._lot

    @property
    def rows(self):
        """
            Get data used in calculations.

            Returns:
                ndarray: Data obtained used in the backtesting strategy.
        """
        return self._rows

    @property
    def title(self):
        """
            Get symbol title.

            Returns:
                str: Symbol title (like a ticker).
        """
        return self._title

    @property
    def spread(self):
        """
            Returns:
                float: Spread for the symbol.
        """
        return self._spread

    @property
    def trend_change_period(self):
        """
            Get the period which indicates the trend change. The period is a number of timespan cycles (days, minutes and so on) used in the calculation.
            For example, if it is 1 and we use EOD data, then backtesting engine will consider that the trend has changed only after 1 additional day
            if there are no other trend change indications used in the strategy (high volume, essential change in percent and so on).

            Returns:
                int: Trend change period.
        """
        return self._trend_change_period

    @property
    def trend_change_percent(self):
        """
            Get the value in % which indicates the trend change of the symbol. For example, if trend_change_percent is 3 and the quote changed
            by 4% in one cycle of calculation (day, minute and so on - depending on a used timestamp), then the trend is considered as
            changed immediately.

            Returns:
                float: Trend change percent.
        """
        return self._trend_change_percent

    @property
    def first_year(self):
        """
            Get first year in the dataset for the calculation.

            Returns:
                The first year in the dataset.

            Raises:
                BackTestError: incorrect date in the provided data.
        """
        dt = get_dt(self._rows[0][Quotes.TimeStamp])

        return dt.year

    #####################
    # Thread safe methods
    #####################

    def create_exec(self, caller):
        """
            Create BackTestOperations instance based on BackTestData instance.
            BackTestData is a container for data used for calculation and the usage of every instance of this class is thread safe.
            Several BackTestOperations may be associated with a single BackTestData. BackTestOperations class is not thread safe
            and it represents an operations performed on a certain symbol in the portfolio.

            Args:
                caller(BackTest): caller class instance.

            Returns:
                BackTestOperations: Class instance to perform the operations on the data for a particular symbol.
        """
        return BackTestOperations(data=self, caller=caller)

#############################
# Base backtesting operations
#############################

# Parameters of a trade direction. sign is used in price/cash/positions arithmetic (+1 for buys, -1 for sells),
# deviation_attr - the caller's attribute with the balance deviation multiplier used for sizing,
# deviation_floor - its minimum when positions are few (too low values may block the portfolio),
# ref_field - the quote field which may fill a resting limit order, label - used in logging.
SideSpec = namedtuple('SideSpec', ['label', 'sign', 'deviation_attr', 'deviation_floor', 'ref_field'])

_SIDE_BUY = SideSpec(label='BUY', sign=1, deviation_attr='open_deviation', deviation_floor=2, ref_field=Quotes.Low)
_SIDE_SELL = SideSpec(label='SELL', sign=-1, deviation_attr='close_deviation', deviation_floor=3, ref_field=Quotes.High)


def _side_spec(buy):
    """
        Get the parameters of the specified trade direction.

    Returns:
        SideSpec: the parameters for buys (True) or sells (False)
    """
    return _SIDE_BUY if buy else _SIDE_SELL


@dataclass
class LimitOrder:
    """ A resting limit order for a symbol. """
    buy: bool  # Trade direction
    limit: float  # The limit price
    num: int | None  # The number of securities. None means max
    validity: int  # Validity in elapsed days (non-trading days count)
    recalculate: bool  # If weightening values should be recalculated after the trade
    exact: bool  # If the exact number of requested positions should be opened
    elapsed: int = 0  # Elapsed days since the placement, advanced by the engine's days_delta


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

        # Opened positions
        self._long_positions = 0

        # Number of trades per symbol
        self._trades_no = 0

        # Backtesting class instance
        self.__caller = caller

        self._portfolio_cash = []  # Portfolio with long positions for the symbol

        # Quote to calculate signal change
        self._signal_quote = None
        # Index for signal change calculations
        self._signal_index = None

        # Results of symbol's calculation
        self._sym_results = BTSymbol()  # TODO High Think of a better data structure here
        self._sym_results.Title = self.data.title

        ####################################################################
        # Cycle specific data for calculations. Need to be reset each cycle.
        ####################################################################

        # Trade prices in the current cycle (for reporting)
        self._price_open_long = None
        self._price_close_long = None

        ##############################
        # Data specific initialization
        ##############################

        # Need to create a labelled numpy array of the same length as the main dataset with time stamp colomn
        self._calc = np.zeros([len(self.data.rows), ], dtype=[('ts', '<i8')])
        self._calc['ts'] = self.data.rows[Quotes.TimeStamp]

        self._last_total_value = 0  # Total value at the moment of opening the last position
        self._total_profit = 0  # Total profit of all operations with this security

        ##############
        # Limit Orders
        ##############

        # TODO MID Do we need a functionality to have several limit orders for a security?
        # Also need to distinguish what to do if the order is filled partially.
        self._limit_order = None  # The active limit order (LimitOrder) if any

        # Calendar days since the last available bar for the symbol (used to detect delisting)
        self._days_since_last_bar = 0

        # Lifecycle state of the security (SecState)
        self._state = SecState.Active

        ################
        # Weight-related
        ################

        if self.caller.weigher is not None:
            self.caller.weigher.validate_data(self.data)

        self._weight = 0  # The weight of the current position

    ###################
    # Properties
    ###################

    @property
    def state(self):
        """
            Get the current lifecycle state of the security.

            Returns:
                SecState: the current state of the security.
        """
        return self._state

    @property
    def weighted(self):
        """
            Indicates if the symbol is supposed to be weighted in a portfolio.

            Returns:
                bool: indicates if the current security is weighted.
        """
        return self.data._weighted

    @property
    def weight(self):
        """
            The weight of the current security in the portfolio. Weight depends of the weightening method.
            Smaller weight (in comparison to mean) indicates that the security share in a portfolio is low.

            Returns:
                float: the weight of the current security in the portfolio.
        """
        return self._weight

    @property
    def has_positions(self):
        """
            Indicates of the current security has any positions opened.

            Returns:
                bool: indicates if the current security has any positions opened.
        """
        return self.long_positions > 0

    @property
    def group(self):
        """
            The value of the grouping attribute.

            Returns:
                The value of an attributed used to group the weighted securities in the portfolio.
        """
        value = None
        attr_title = self.caller.grouping_attr

        if attr_title is not None and hasattr(self.data, attr_title):
            value = self.data.__getattribute__(attr_title)

        return value

    @property
    def group_share(self):
        """
            Get the grouping share for the current attribute value.

            Returns:
                float: the grouping share for the current security.
        """
        share = 1

        if self.group:
            share = self.caller.get_grouping_share(self.group)

        return share

    @property
    def group_total_value(self):
        """
            Get the total_value of the current group.

            Returns:
                float: the total value of the current group
        """
        share = None

        if self.group is not None:
            share = self.caller.grouping_values[self.group]

        return share

    @property
    def group_mean(self):
        """
            Get the mean weight value for the current group.

            Returns:
                float: the mean value for the current group
        """
        if self.group:
            return self.caller.get_group_mean(self.group)
        else:
            return self.caller.mean_weight

    @property
    def group_max_cap(self):
        """
            Get the max cap value for the current group.

            Returns:
                float: the max cap value for the current group
        """
        if self.group:
            return self.caller.get_group_max_cap(self.group)
        else:
            return self.caller.max_cap

    @property
    def group_capacity(self):
        """
            Get the current group capacity (with deviation optionally).
            The bigger the value - the more capacity the group has.

            Returns:
                float: the current group capacity
        """
        if self.group is None:
            return 1
        else:
            return self.caller.get_group_capacity(self.group)

    @property
    def is_min_capacity_group(self):
        """
            Indicates if the group has a minimum capacity value of all groups.

            Returns:
                bool: if the current group has a minimum capacity
        """
        return self.group_capacity == self.caller.min_group_capacity

    @property
    def is_max_capacity_group(self):
        """
            Indicates if the group has a maximum capacity value of all groups.

            Returns:
                bool: if the current group has a maximum capacity
        """
        return self.group_capacity == self.caller.max_group_capacity

    @property
    def is_limit(self):
        """
            Check if there is a limit order for a security.

            Returns:
                bool: indicates if there is a limit order for a security
        """
        return self._limit_order is not None

    @property
    def data(self):
        """
            Gets the used data class instance.

            Returns:
                BackTestData: The corresponding data class.
        """
        return self.__data

    ################
    # Methods
    ################

    def add_col(self, name, data=None, dtype=object):
        """
            Add new column to the calculations array.

            Args:
                name(str): label for the column
                data(1d ndarray): data to add
                dtype: type of data
        """
        if data is not None and len(data) != len(self._calc):
            raise BackTestError(f"Length of appending data should equal the length of the base data: {len(data)} != {len(self._calc)}")

        self._calc = add_column(self._calc, name, dtype, default=np.nan)

        if data is not None:
            self._calc[name] = data

    @property
    def vals(self):
        """
            Get the calculated values dataset.

            Returns:
                ndarray: the calculated values dataset.
        """
        return self._calc

    # TODO LOW Refactor the functions above (to get indexes, rows, values)
    def get_val(self, offset=0, ts=None):
        """
            Get the row from the calculations dataset for a specified time stamp.

            Args:
                offset(int): offset from the time stamp row.
                ts(int): time stamp to get the row. None for the current one.

            Returns:
                ndarray: or None if none found.
        """
        local_index = self.get_index(ts)

        if local_index is None:
            return None
        else:
            local_index += offset

            # Numpy negative indexing wraps to the last (future) row — guard against it.
            if local_index < 0:
                return None

            return self._calc[local_index]

    def get_avail_val(self, offset=0, ts=None):
        """
            Get the last available row from the calculations dataset for a specified time stamp.

            Args:
                offset(int): offset from the time stamp row.
                ts(int): time stamp to get the row. None for the current one.

            Returns:
                ndarray: or None if none found.
        """
        local_index = self.get_avail_index(ts)

        if local_index is None:
            return None
        else:
            local_index += offset

            if local_index < 0:
                return None

            return self._calc[local_index]

    def get_row(self, offset=0, ts=None):
        """
            Get the row from dataset for a specified time stamp.

            Args:
                offset(int): offset from the time stamp row.
                ts(int): time stamp to get the row. None for the current one.

            Returns:
                ndarray: or None if none found.
        """
        local_index = self.get_index(ts)

        if local_index is None:
            return None
        else:
            local_index += offset

            if local_index < 0:
                return None

            return self.data.rows[local_index]

    def get_index(self, ts=None):
        """
            Get the index of the current security data according to the time stamp.

            Args:
                ts(int): time stamp to get the index for. None for the current one.

            Returns:
                int: index or None if not found.
        """
        # If time stamp is not specified, used the current time stamp from the main dataset.
        if ts is None:
            index = self.caller_index
            ts = self.caller.main_data.rows[index][Quotes.TimeStamp]

        idx = np.where(self.data.rows[Quotes.TimeStamp] == ts)[0]

        if len(idx):
            return idx[0]

        return None

    def get_avail_row(self, offset=0, ts=None):
        """
            Get the last available row from dataset for a specified time stamp.

            Args:
                offset(int): offset from the time stamp row.
                ts(int): time stamp to get the row. None for the current one.

            Returns:
                ndarray: or None if none found.
        """
        local_index = self.get_avail_index(ts)

        if local_index is None:
            return None
        else:
            local_index += offset

            if local_index < 0:
                return None

            return self.data.rows[local_index]

    def get_avail_index(self, ts=None):
        """
            Get the last available index of the current security data according to the time stamp.

            Args:
                ts(int): time stamp to get the last available index for. None if not found.

            Returns:
                int: index or None if not found.
        """
        # If time stamp is not specified, used the current time stamp from the main dataset.
        if ts is None:
            index = self.caller_index
            ts = self.caller.main_data.rows[index][Quotes.TimeStamp]

        idx = np.where(self.data.rows[Quotes.TimeStamp] <= ts)[-1]

        if len(idx):
            return idx[-1]

    ###########################################################
    # General methods with calculations for a particular symbol
    ###########################################################

    @property
    def total_profit(self):
        """
            Get the total profit of the operations with the security.

            Returns:
                float: the total profit since the last trade
        """
        profit = self._total_profit

        if self.is_long():
            # The positions of delisted and skipped securities are considered worthless (sell_price returns 0).
            value_price = self.sell_price

            for price_cash in self._portfolio_cash:
                profit += value_price - price_cash

        return profit

    @property
    def last_total_profit(self):
        """
            Get the total profit at the moment when the last trade was performed.

            Returns:
                float: the total profit since the last trade
        """
        # Guard against the division by zero (e.g. no position was opened yet so there is no reference value)
        if self._last_total_value == 0:
            return 0

        return (self.total_value - self._last_total_value) / self._last_total_value * 100

    def check_if_finished(self):
        """
            Check if the simulation is finished for this instance.

            Returns:
                bool: indicate if the calculation is finished
        """
        return self.caller_index + 1 == len(self.data.rows)

    # Fee calculated based on commission in percent of the trade
    def get_trade_percent_fee(self, price=None):
        """
            Get the percent fee for the current trade (1 instrument)

            Args:
                price(float): the execution price to calculate the fee for. market_price is used if None.

            Returns:
                float: percent fee for the trade for 1 instrument
        """
        if price is None:
            price = self.market_price

        return price * self.caller.commission_percent / 100

    # Fee for one security
    def get_security_fee(self, price=None):
        """
            Get the one security fee for the trade (expect commission for the trade).

            Args:
                price(float): the execution price to calculate the fee for. market_price is used if None.

            Returns:
                float: one security fee (excluding commission for the whole trade)
        """
        return self.get_trade_percent_fee(price) + self.caller.commission_security

    # Total fee for a trade (1 security)
    def get_total_fee(self, price=None):
        """
            Get the total fee for 1 security trade.

            Args:
                price(float): the execution price to calculate the fee for. market_price is used if None.

            Returns:
                float: the total fee for 1 security trade.
        """
        return self.get_security_fee(price) + self.caller.commission

    def is_long(self):
        """
            Indicated if currently opened positions are long.

            Returns:
                bool: True if there are at least one long position opened, false otherwise.
        """
        return self._long_positions > 0

    @property
    def last_total_value(self):
        """
            Get the total value at the moment of opening the last position.

            Returns:
                float: the total value at the moment of opening the last position.
        """
        return self._last_total_value

    def get_datetime_str(self, index=None):
        """
            Get the datetime string for the current or specified index

            Returns:
                str: datetime string for the current position or specified offset.

            Raises:
                BackTestError: no data for the symbol at the current cycle.
        """
        if index == None:
            index = self.get_index()

        if index is None:
            raise BackTestError(f"No data for {self.data.title} at the current cycle.")

        return self.data.rows[index][Quotes.DateTime]

    def get_datetime(self, index=None):
        """
            Get the datetime for the current or specified index

            Returns:
                DateTime: datetime for the current position or specified offset.

            Raises:
                BackTestError: incorrect datetime presents in the provided data.
        """
        if index == None:
            index = self.get_index()

        dt_str = self.get_datetime_str(index)

        try:
            dt = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            raise BackTestError(f"The date {dt_str} is incorrect: {e}") from e

        return dt

    @property
    def year(self):
        """
            Get the current year during the calculation.

            Returns:
                The current year of the calculation.
        """
        return self.get_datetime().year

    @property
    def open(self):
        """
            Get the open price.

            Returns:
                float: the open price at the current index of the calculation.

            Raises:
                BackTestError: no data for the symbol at the current cycle.
        """
        index = self.get_index()

        if index is None:
            raise BackTestError(f"No data for {self.data.title} at the current cycle.")

        return self.data.rows[index][Quotes.Open]

    @property
    def close(self):
        """
            Get the close price.

            Returns:
                float: the close price at the current index of the calculation.

            Raises:
                BackTestError: no data for the symbol at or before the current cycle.
        """
        row = self.get_avail_row()

        if row is None:
            raise BackTestError(f"No data for {self.data.title} at the current cycle.")

        return row[Quotes.Close]

    @property
    def market_price(self):
        """
            Get the current market price for decisions — the Open of the current bar.

            Decisions, sizing, fees, spread and market-order execution all use this price.
            Indicators/signals are computed on completed bars and compared against it.
            Returns None if no row is available.

            Returns:
                float: the open price of the current bar.
        """
        row = self.get_avail_row()

        if row is None:
            return None

        return row[Quotes.Open]

    @property
    def high(self):
        """
            Get the current cycle's highest price.

            Returns:
                float: the highest price at the current index of the calculation.

            Raises:
                BackTestError: no data for the symbol at the current cycle.
        """
        index = self.get_index()

        if index is None:
            raise BackTestError(f"No data for {self.data.title} at the current cycle.")

        return self.data.rows[index][Quotes.High]

    @property
    def low(self):
        """
            Get the current cycle's lowest price.

            Returns:
                float: the lowest price at the current index of the calculation.

            Raises:
                BackTestError: no data for the symbol at the current cycle.
        """
        index = self.get_index()

        if index is None:
            raise BackTestError(f"No data for {self.data.title} at the current cycle.")

        return self.data.rows[index][Quotes.Low]

    @property
    def charting_bar(self):
        """
            Get the charting OHLC bar for the current cycle.

            The base class returns raw prices. Subclasses for security types with adjusted prices
            (e.g. stocks) override the charting columns in the data class to return an adjusted bar.
            Used for reporting/charting and indicator calculation only - never for order execution
            or valuation.

            Returns:
                ChartingBar: the current OHLC bar, None if no row is available.
        """
        row = self.get_avail_row()

        if row is None:
            return None

        cols = self.data.charting

        return ChartingBar(row[cols.open], row[cols.high], row[cols.low], row[cols.close])

    @property
    def spread_deviation(self):
        """
            Get the current spread deviation. Buy price is the current market price (the bar's Open) plus the
            deviation, sell otherwise.

            Returns:
                float: the spread deviation for the corresponding symbol.
        """
        return self.market_price * self.data.spread / 100 / 2

    # TODO LOW Think of other ways to calculate a spread
    def get_execution_price(self, buy):
        """
            Get the market execution price for a trade: the current bar's Open (the market price for decisions
            and execution) plus/minus the spread deviation for buys/sells.

            Args:
                buy(bool): True for the buy price, False for the sell price.

            Returns:
                float: the execution price of the symbol
        """
        return self.market_price + _side_spec(buy).sign * self.spread_deviation

    @property
    def buy_price(self):
        """
            Get the buy price of the current symbol in the current cycle of the calculation. It is based on the
            current bar's Open (the market price for decisions and execution) plus the spread deviation.

            Returns:
                float: the buy price of the symbol
        """
        return self.get_execution_price(buy=True)

    @property
    def sell_price(self):
        """
            Get the sell price of the current symbol in the current cycle of the calculation. It is based on the
            current bar's Open (the market price for decisions and execution) minus the spread deviation.
            The positions of delisted and skipped securities are considered worthless: 0 is returned for them.

            Returns:
                float: the sell price of the symbol.
        """
        if self._state is not SecState.Active:
            return 0

        return self.get_execution_price(buy=False)

    @property
    def long_positions(self):
        """
            Get the number of long positions.

            Returns:
                int: the number of currently opened long positions for the symbol.
        """
        return self._long_positions

    def reset_trade_prices(self):
        """
            Reset trade prices used in the current cycle.
        """
        self._price_open_long = None
        self._price_close_long = None

    @property
    def caller(self):
        """
            Get the backtesting class instance which called the data class.

            Returns:
                BackTest: the 'main' backtesting class instance.
        """
        return self.__caller

    @property
    def caller_index(self):
        """
            Get the index of the current calculation.

            Returns:
                int: index of the record in the dataset used for calculation.
        """
        return self.__caller.index

    def apply_days_counter(self, days_delta):
        """
            Applies days counter for the calculation to a particular symbol.

            It may be used for a specific financial instruments as a countdown to some event. It needs to be overloaded
            in a derived class dedicated to a particular security type.

            Args:
                days_delta(int): number of calendar days passed since the previous calculation cycle.
        """
        # Track staleness to detect a delisting: reset on a fresh bar, accumulate on its absence.
        # Symbols which have never been quoted yet (e.g. data starts later) are not tracked.
        if self.get_index() is not None:
            self._days_since_last_bar = 0
        elif self.get_avail_index() is not None:
            self._days_since_last_bar += days_delta

        # Advance the age of the resting limit order (if any).
        if self._limit_order is not None:
            self._limit_order.elapsed += days_delta

    @property
    def total_value(self):
        """
            Get the total value of positions opened for the particular symbol.

            Returns:
                float: the total value of the all opened positions. Delisted and skipped securities are
                       kept in the portfolio but are considered worthless (0 via sell_price).
        """
        total_value = 0

        if self.is_long():
            total_value += self.sell_price * self._long_positions

        return total_value

    def add_symbol_result(self, result: BTSymbolRow | None = None):
        """
            Generate symbol-specific results for the current cycle.

            Args:
                result(BTSymbolRow): the result to add. Auto generated otherwise.
        """
        if result is None:
            bar = self.charting_bar

            if bar is not None:
                result = BTSymbolRow(
                    open=bar.open,
                    close=bar.close,
                    high=bar.high,
                    low=bar.low,
                    price_open_long=self._price_open_long,
                    price_close_long=self._price_close_long,
                    long_positions=self._long_positions,
                    trades_no=self._trades_no)
            else:
                result = BTSymbolRow()

        self._sym_results.append(result)

    @property
    def sym_results(self):
        """
            Get results of the current symbol's calculation.

            Returns:
                BTSymbol: symbol calculation results.
        """
        return self._sym_results

    def trend_changed(self, is_uptrend):
        """
            Checks if we consider that the trend has changed in the current cycle.

            Once the change is confirmed in a cycle, repeated calls in the same cycle
            return True again (consistently for both the period and the percent check).
            If the change was confirmed but not traded on, the confirmation persists
            in subsequent cycles.

            Returns:
                bool: True if the trend is considered as changed, false otherwise
        """
        quote = self.market_price
        index = self.caller.index

        if is_uptrend == self.is_long():
            self._signal_quote = None
            self._signal_index = None

            return False

        # The change was already confirmed earlier and is still not traded on
        if self._signal_quote is None and self._signal_index is not None:
            return True

        # No quote for the symbol in this cycle - cannot start or re-check a signal
        if quote is None:
            return False

        if self._signal_quote is None:
            self._signal_quote = quote
            self._signal_index = index

        changed = False

        if index - self._signal_index >= self.data.trend_change_period:
            changed = True
        else:
            max_quote = max(quote, self._signal_quote)
            min_quote = min(quote, self._signal_quote)

            if min_quote > 0 and max_quote / min_quote >= 1 + (self.data.trend_change_percent / 100):
                changed = True

        if changed:
            # Mark the change as confirmed at this cycle so a repeated call returns True again
            self._signal_quote = None
            self._signal_index = index

            return True

        return False

    ###################
    # Trades processing
    ###################

    def calc_weight(self):
        """
            Calculate and set the weight value only for the current security.
        """
        self._weight = 0

        weigher = self.caller.weigher
        if weigher is not None:
            self._weight = weigher.weight(self)

    def calc_weight_values(self, had_positions, has_positions, ex_total_value):
        """
            Calculate and set the weightening values after performing a trade.

            Args:
                had_positions(bool): indicates if the security had positions before the trade
                has_positions(bool): indicates if the security has positions after the trade
                ex_total_value(float): total value before making a trade
        """
        if self.caller.weigher is None and self.caller.grouping_attr is None:
            return

        mult_adj_num = 0

        if had_positions and has_positions is False:
            mult_adj_num = -1
        elif had_positions is False and has_positions:
            mult_adj_num = 1

        ex_weight = self.weight

        self.calc_weight()
        self.caller.adjust_weight_values(ex=self,
                                               mult_adj_num=mult_adj_num,
                                               ex_weight=ex_weight,
                                               ex_total_value=ex_total_value)

    def get_trade_num(self, deviation):
        """
            Get the securities num for a trade to keep the portfolio weighted. The value may be negative.

            Arguments:
                deviation(float): the maximum deviation for securities number calculation.

            Returns:
                int: the number of securities for a trade to keep the portfolio balanced.
        """
        num = 0

        weigher = self.caller.weigher
        if weigher is not None:
            num = weigher.get_trade_num(self, deviation)

        # Check if the number of positions is limited by grouping
        if self.group is not None and self.caller.grouping_invested:
            # Take cash into account as accumulated cash may prevent to open bigger position sizes otherwise.
            tv = self.caller.grouping_tv
            grouping_num = self.group_capacity * deviation * tv / self.market_price
            num = min(num, grouping_num)

        return num

    def _get_side_num(self, buy):
        """
            The number of securities to trade in the specified direction to keep the portfolio balanced.

            Args:
                buy(bool): True to get the number of securities to buy, False - to sell.

            Returns:
                int: the number of securities to trade in the specified direction
        """
        p = _side_spec(buy)

        deviation = getattr(self.caller, p.deviation_attr)

        # TODO Mid Think how to handle it better
        # Too low deviation in the beginning may block the portfolio
        if self.long_positions < 10:
            deviation = max(p.deviation_floor, deviation)

        num = self.get_trade_num(deviation)

        # Keep only the trades in the requested direction
        if p.sign * num < 0:
            num = 0

        return int(p.sign * num)

    @property
    def buy_num(self):
        """
            The number of securities to buy and keep the portfolio balanced as much as possible.

            Returns:
                int: the number of securities to buy
        """
        return self._get_side_num(buy=True)

    @property
    def sell_num(self):
        """
            The number of securities to sell and keep the portfolio balanced as much as possible.

            Returns:
                int: the number of securities to sell
        """
        return self._get_side_num(buy=False)

    # TODO LOW Implement using a higher resolution for order procesing.
    # TODO LOW Specifying an exact price (used in limit order exection) may be dangerous for errors in backtesting
    #      strategies. Think how to implement it in a safer way.
    def _order(self, buy, num=None, limit=None, limit_validity=2, exact=False, recalculate=True, price=None):
        """
            Perform a trade (the shared implementation for buy()/sell()).

            Args:
                buy(bool): the trade direction: True to buy, False to sell.
                num(int): the number of securities to trade. None if max.
                limit(float): price for a limit order. None for a market order (spread will be taken into account then).
                limit_validity(int): number of elapsed days for a limit order to be valid until it is cancelled
                                     (weekends and holidays are counted too).
                exact(bool): indicates if the exact number of requested positions should be opened.
                recalculate(bool): indicates if weightening values for portfolio should be recalculated after performing
                                   the trade. Use if in one cycle decision taking may happen after a trade.
                price(float): force the trade to be executed using this price.

            Returns:
                int: the number of positions traded
        """
        if self._state is not SecState.Active:
            return 0

        p = _side_spec(buy)

        total_num = 0

        # Process a market order
        if limit is None:
            if recalculate:
                had_positions = self.has_positions
                ex_total_value = self.total_value

            if num is None:
                if self.weighted and self.caller.weigher is not None and exact is False:
                    num = self._get_side_num(buy)
                else:
                    num = self.get_max_trade_size() if buy else self.long_positions
            else:
                if self.weighted and self.caller.weigher is not None and exact is False:
                    num = min(num, self._get_side_num(buy))

            if num > 0:
                if buy:
                    # open_long() may clamp the number if not enough cash and exact is False
                    total_num += self.open_long(num, price=price, exact=exact)
                else:
                    if self.long_positions:
                        num_close = min(self.long_positions, num)
                        total_num += self.close_long(num_close, price=price)

            if recalculate:
                self.calc_weight_values(had_positions=had_positions,
                                        has_positions=self.has_positions,
                                        ex_total_value=ex_total_value)
        else:
            # Ignore the order with a non-positive number of securities
            if num is not None and num <= 0:
                return total_num

            # Place a limit order
            if self._limit_order is not None:
                log = (f"Cancelling {_side_spec(self._limit_order.buy).label} limit order for {self.data.title} "
                       f"as the new order is being placed.")
                self.caller._lg.highlight(log)

            self.cancel_limit_order()

            # A marketable limit order: the close is within the limit price so the resting
            # order was crossed after the placement. It fills at the limit price.
            close_price = self.close

            # BUY is filled when the close is not higher than the limit, SELL - when it is not lower
            if p.sign * (close_price - limit) <= 0:
                log = (f"At {self.get_datetime_str()} {p.label} limit order for {self.data.title} with the price {round(limit, 2)} "
                       f"is marketable at the bar close - executing at {round(limit, 2)}.")

                self.caller._lg.plain(log)

                total_num = self._order(buy, num=num, price=limit, recalculate=recalculate, exact=exact)

                # The order is not working anymore if it was filled (at least partially)
                if total_num > 0:
                    return total_num

                # The order could not be filled at all (insufficient cash or weighing did not allow the trade)
                # so it is kept working as a resting order below
                log = (f"At {self.get_datetime_str()} {p.label} limit order for {self.data.title} with the price {round(limit, 2)} "
                       f"could not be filled and is kept working.")

                self.caller._lg.plain(log)

            self._limit_order = LimitOrder(buy=buy,
                                            limit=limit,
                                            num=num,
                                            validity=limit_validity,
                                            recalculate=recalculate,
                                            exact=exact)

            order_num = num if num is not None else 'max'

            log = (f"At {self.get_datetime_str()} {p.label} Limit order is placed for {self.data.title} for the {order_num} number of securities "
                   f"with the price {round(limit, 2)}.")

            self.caller._lg.highlight(log)

        return total_num

    def buy(self, num=None, limit=None, limit_validity=2, exact=False, recalculate=True, price=None):
        """
            Perform a buy trade. A market order is executed at the current bar's Open (+ half spread).

            Args:
                num(int): the number of securities to buy. None if max.
                limit(float): price for a limit order. None for a market order (spread will be taken into account then).
                limit_validity(int): number of elapsed days for a limit order to be valid until it is cancelled
                                     (weekends and holidays are counted too).
                exact(bool): indicates if the exact number of requested positions should be opened.
                recalculate(bool): indicates if weightening values for portfolio should be recalculated after performing
                                   the trade. Use if in one cycle decision taking may happen after a trade.
                price(float): force the trade to be executed using this price.

            Returns:
                int: the number of positions opened
        """
        return self._order(buy=True, num=num, limit=limit, limit_validity=limit_validity,
                           exact=exact, recalculate=recalculate, price=price)

    def sell(self, num=None, limit=None, limit_validity=2, exact=False, recalculate=True, price=None):
        """
            Perform a sell trade. A market order is executed at the current bar's Open (- half spread).

            Args:
                num(int): the number of securities to sell. None if max.
                limit(float): price for a limit order. None for a market order (spread will be taken into account then).
                limit_validity(int): number of elapsed days for a limit order to be valid until it is cancelled
                                     (weekends and holidays are counted too).
                exact(bool): indicates if the exact number of requested positions should be opened.
                recalculate(bool): indicates if weightening values for portfolio should be recalculated after performing
                                   the trade. Use if in one cycle decision taking may happen after a trade.
                price(float): force the trade to be executed using this price.

            Returns:
                int: the number of positions closed
        """
        return self._order(buy=False, num=num, limit=limit, limit_validity=limit_validity,
                           exact=exact, recalculate=recalculate, price=price)

    def cancel_limit_order(self):
        """
            Cancel a limit order.
        """
        self._limit_order = None

    def process_limit_order(self):
        """
            Execute limit order (if any).

            Some notes: when we process a limit order placed in the scope of a current bar, we consider that only Open
            price was known at the time or order placement and it had already passed when the order is placed. Also
            we do not know if the order was placed before or after when High/Low were achieved - so we ignore High/Low as well.
            The same bar order may be executed only based on the Close price which will for sure happen after the order placement.
            For example, the OHLC is 100/102/98/99 $, and the order was placed for $100 (note that it is up to user to determine the exact price) -
            Then that order will be executed at $100 based on the Close price only (as $99 likely wasn't achived instantly and we ignore High/Low).
            If the order wasn't executed in the scope of the initial (placement bar) - at the next bar we take the whole OHLC into account.
        """
        if self._state is not SecState.Active:
            return

        order = self._limit_order

        if order is None:
            return

        p = _side_spec(order.buy)

        # The price which may fill the order: Low for buys, High for sells
        price = self.get_row()[p.ref_field]

        if order.elapsed > order.validity:
            log = (f"At {self.get_datetime_str()} {p.label} limit order expired for {self.data.title} as in the {order.elapsed} days the desired price "
                   f"{order.limit} wasn't achieved or weighening did not allow to perform the trade. The last price is {price}.")

            self.caller._lg.highlight(log)

            self.cancel_limit_order()

            return

        num = 0

        # The open price of a subsequent bar is known to be a "passed" price: if the bar gapped
        # through the limit, the order fills at the open. Otherwise a touch of Low/High fills
        # the order at the resting limit price.
        open_price = self.get_row()[Quotes.Open]

        # The order is filled when the price crosses the limit: BUY - at or below it, SELL - at or above it
        if p.sign * (open_price - order.limit) <= 0:
            num = self._order(order.buy, num=order.num, price=open_price,
                              recalculate=order.recalculate, exact=order.exact)
        else:
            if p.sign * (price - order.limit) <= 0:
                num = self._order(order.buy, num=order.num, price=order.limit,
                                  recalculate=order.recalculate, exact=order.exact)

        # Limit order fills are always executed with an explicit price so no spread expense is added
        # (the spread is applied to market orders only).

        if num > 0:
            self.cancel_limit_order()

    #####################
    # Check for delisting
    #####################

    def check_delisting(self):
        """
            Track the lifecycle of the security and handle its delisting.

            A security is considered to be delisted if there were no consecutive quotes updates in N number of days
            (7 by default) after it has been quoted at least once. Only calendar days are counted (via the engine's
            days_delta based counter), so several intraday bars within the same day do not advance the staleness
            counter.

            On delisting the positions are kept in the portfolio but are considered worthless (valued at 0) and
            cannot be traded any more. The loss is valued at the last available past bar (no forward-looking data)
            and counted once - as an other expense.

            If the ticker is quoted again later it is assumed to belong to another company: the security is skipped
            (SecState.Skipped), a warning is displayed and further trading is disabled.
        """
        if self._state == SecState.Delisted and self.get_index() is not None:
            # The ticker has reappeared: treat it as belonging to a different company now.
            self._state = SecState.Skipped
            self.caller._lg.warning(f"At {self.caller.exec().get_datetime_str()} quotes for {self.data.title} resumed after "
                                    f"the security was considered as delisted. The ticker likely belongs to another "
                                    f"company now and trading is disabled for it.")
            return

        if self._state != SecState.Active:
            return

        if self._days_since_last_bar > 7:
            # Pay out everything owed to the holder while the security is still Active.
            self._on_delisting()

            if self.long_positions:
                # The value is read while the security is still Active - it becomes worthless on the state change.
                total_lost = self.total_value

                self.caller.add_other_expense(total_lost)

                self.caller._lg.highlight(f"At {self.caller.exec().get_datetime_str()} {self.data.title} was considered as delisted and "
                                    f"total positions of {self.long_positions} of total worth {total_lost} "
                                    f"({self.sell_price} per security) are now considered worthless.")

            self._state = SecState.Delisted

            self.cancel_limit_order()

    def _on_delisting(self):
        """
            Called right before the security is marked as delisted (while it is still Active).
            Overridden in subclasses to pay out anything owed to the holder (e.g. a pending yield).
        """

    #######################################
    # Methods related to opening positions.
    #######################################

    def get_max_trade_size_cash(self, price=None):
        """
            Get the maxumum number of securities which we can buy using the cash balance without going negative.

            Args:
                price(float): the execution price for the trade. If None, buy_price is used.

            Return:
                int: the maximum of positions to open using cash only.
                float: remaining cash.
        """
        lot = self.data.lot

        buy_price = price if price is not None else self.buy_price

        # Guard against invalid prices (a bad quote, e.g. zero, negative or NaN price)
        if buy_price is None or not np.isfinite(buy_price) or buy_price <= 0:
            log = (f"At {self.get_datetime_str()} can't estimate the maximum trade size of {self.data.title}: "
                   f"the execution price is invalid ({buy_price}). No positions will be opened.")

            self.caller._lg.error(log)

            return (0, self.caller.cash - self.caller.commission)

        # A single affordability model: the cash should cover the flat commission and, for each security,
        # the execution price plus the per-share fee. It mirrors the actual trade cost in _execute_trade().
        per_share_cost = buy_price + self.get_security_fee(buy_price)
        cash_available = self.caller.cash - self.caller.commission

        securities_num = math.floor(cash_available / per_share_cost / lot) * lot
        securities_num = round(max(0, securities_num), 6)

        remaining_cash = cash_available - securities_num * per_share_cost

        return (securities_num, remaining_cash)

    def get_max_trade_size(self, price=None):
        """
            Get total number of securities which we may buy.

            Args:
                price(float): the execution price for the trade. If None, buy_price is used.

            Returns:
                int: the total number of securities which we can buy using cash.
        """
        return self.get_max_trade_size_cash(price=price)[0]

    def _execute_trade(self, buy, num, price=None, exact=False):
        """
            Open (buy) or close (sell) the specified number of long positions
            (the shared implementation for open_long()/close_long()).

            Args:
                buy(bool): the trade direction: True to open, False to close positions.
                num(int): the number of securities to trade.
                price(float): force the trade to be executed using this price.
                exact(bool): indicates if the exact number of requested positions should be opened.

            Returns:
                int: the number of positions opened/closed

            Raises:
                BackTestError: can't open the negative number of positions, not enough cash to open positions
                               (with exact=True), the number of positions to close is bigger than opened.
        """
        if self._state is not SecState.Active:
            return 0

        p = _side_spec(buy)

        if buy:
            if num < 0:
                raise BackTestError(f"Can't open negative number of long positions: {num}")

            # Affordability check
            max_size_price = price if price is not None else self.buy_price

            if num > self.get_max_trade_size(price=max_size_price):
                if exact:
                    raise BackTestError(f"Not enough cash to open the position. {num} > {self.get_max_trade_size(price=max_size_price)}")
                else:
                    num = min(num, self.get_max_trade_size(price=max_size_price))

            if num == 0:
                return 0
        else:
            if num < 0:
                # A negative num is not a valid sell request (it is not a 'sell by sign' semantics) so the trade is skipped
                log = (f"At {self.get_datetime_str()} requested to close a negative number of long positions "
                       f"of {self.data.title}: {num}. The trade is skipped.")

                self.caller._lg.error(log)

                return 0

            if num > self._long_positions:
                raise BackTestError(f"Number of long positions to close is bigger than the number of actual positions: {num} > {self._long_positions}")

            if num <= 0 or self._long_positions == 0:
                return 0

        # Needed for logging
        ex_cash = self.caller.cash

        # Used only in charting. The real (non-adjusted) spread is applied to the charting bar
        # prices, so the markers stay scale-consistent with the chart.
        # On the other hand, note that the markers are just a visual indication of a trade (not exact price representation)
        # and they should correspond the close price (as the chart is built using the close price).
        bar = self.charting_bar
        spread_deviation = self.spread_deviation

        if buy:
            self._price_open_long = bar.close + spread_deviation
        else:
            self._price_close_long = bar.close - spread_deviation

        total_spread_expense = 0

        if price is None:
            total_spread_expense = self.spread_deviation * num
            price = self.buy_price if buy else self.sell_price

        # The percent commission is calculated based on the actual execution price
        total_commission = self.get_security_fee(price) * num + self.caller.commission

        if buy:
            self.caller.add_cash(-abs(total_commission + price * num))
        else:
            # Remove the sold lots (FIFO) from the cash portfolio (used for total profit calculations)
            sold_cash = sum(self._portfolio_cash[:num])
            self._portfolio_cash = self._portfolio_cash[num:]

            self.caller.add_cash(price * num)
            self.caller.add_cash(-abs(total_commission))

        # Add expenses for this trade
        self.caller.add_commission_expense(total_commission)

        self._long_positions += p.sign * num

        self.caller.add_spread_expense(total_spread_expense)

        self._trades_no += 1
        self.caller.add_total_trades(1)

        if buy:
            log = (f"At {self.get_datetime_str()} OPENED {num} LONG positions of {self.data.title} with price "
                   f"{round(price, 2)} for {round(total_commission + num * price, 2)} in total when "
                   f"cash was {round(ex_cash, 2)} and currently "
                   f"it is {round(self.caller.cash, 2)}")

            self.caller._lg.plain(log)

            self._last_total_value = self.total_value
            self._portfolio_cash.extend(repeat(price, num))
        else:
            # Realized profit: the cash delta minus the cost basis of the sold shares
            self._total_profit += self.caller.cash - ex_cash - sold_cash

            log = (f"At {self.get_datetime_str()} CLOSED {num} LONG positions of {self.data.title} with price "
                   f"{round(price, 2)} for {round(total_commission + num * price, 2)} in total and "
                   f"cash was {round(ex_cash, 2)} and currently "
                   f"it is {round(self.caller.cash, 2)}.")

            self.caller._lg.plain(log)

        return num

    def open_long(self, num, price=None, exact=False):
        """
            Open the specified number of long position.

            Args:
                num(int): the number of securities to buy.
                price(float): force the trade to be executed using this price.
                exact(bool): indicates if the exact number of requested positions should be opened.

            Returns:
                int: the number of positions opened

            Raises:
                BackTestError: not enough cash to open the position.
                BackTestError: Can't open the negative number of positions.
        """
        return self._execute_trade(buy=True, num=num, price=price, exact=exact)

    # Open maxumum possible positions
    def open_long_max(self):
        """
            Open maximum possible number of long positions.
        """
        self.open_long(self.get_max_trade_size())

    #######################################
    # Methods related to closing positions.
    #######################################

    def close_long(self, num, price=None):
        """
            Close the number of long positions.

            Args:
                num(int): the number of positions to close.
                price(float): force the trade to be executed using this price.

            Returns:
                int: the number of positions closed

            Raises:
                BackTestError: to many positions to close.
        """
        return self._execute_trade(buy=False, num=num, price=price)

    def close_all_long(self):
        """
            Close all long positions.
        """
        self.close_long(self._long_positions)

    def close_all(self):
        """
            Close all positions.
        """
        self.close_all_long()

#####################################################
# Classes for data structures of backtesting results.
#####################################################

class BTBaseData():
    """
        Base class to represent backtesting results.
    """
    _row_type = None

    def __init__(self):
        """Initialize the instance of the data class."""
        # Per-column storage of the results
        self._columns = [[] for _ in fields(self._row_type)]

    def append(self, row):
        """
            Append row to the results.

            Args:
                row: the row dataclass to add.
        """
        if not isinstance(row, self._row_type):
            raise BackTestError(f"{self._row_type.__name__} instance is expected, got {type(row).__name__}.")
        for col, value in zip(self._columns, astuple(row)):
            col.append(value)

    def __getitem__(self, point):
        """
            Get the item.

            Args:
                point(list): indexes of the item to get.
        """
        x, y = point
        return self._columns[y][x]

    def __setitem__(self, point, value):
        """
            Get the item.

            Args:
                point(list): indexes of the item to get.
                value: value to set.
        """
        x, y = point
        self._columns[y][x] = value

    def __str__(self):
        """
            Return the string representation of the underlying data.

            Returns:
                str: the string representation of the underlying data.
        """
        return str(self._columns)

class BTData(BTBaseData):
    """
        The class which represents the whole portfolio.
    """
    _row_type = BTDataRow

    def __init__(self):
        """Initialize the instance of the data class."""
        super().__init__()
        self.Symbols = []
        self._elapsed_time = None

    @property
    def DateTime(self):
        return self._columns[BTDataEnum.DateTime]

    @property
    def TotalValue(self):
        return self._columns[BTDataEnum.TotalValue]

    @property
    def Deposits(self):
        return self._columns[BTDataEnum.Deposits]

    @property
    def Cash(self):
        return self._columns[BTDataEnum.Cash]

    @property
    def OtherProfit(self):
        return self._columns[BTDataEnum.OtherProfit]

    @property
    def CommissionExpense(self):
        return self._columns[BTDataEnum.CommissionExpense]

    @property
    def SpreadExpense(self):
        return self._columns[BTDataEnum.SpreadExpense]

    @property
    def OtherExpense(self):
        return self._columns[BTDataEnum.OtherExpense]

    @property
    def TotalExpenses(self):
        return self._columns[BTDataEnum.TotalExpenses]

    @property
    def TotalTrades(self):
        return self._columns[BTDataEnum.TotalTrades]

    @property
    def ElapsedTime(self):
        """
            Get the time in seconds which was spent on the backtesting calculation.

            Returns:
                float: the elapsed time in seconds.
        """
        return self._elapsed_time

    def get_statistics(self, title="Strategy performance:"):
        """Get the statistics of the calculation in both structured and text form.

            The returned object exposes the metrics as fields (e.g. `stats.invested`)
            and supports `str(stats)` to get a formatted multi-line text representation
            of the same data that is rendered into a report image by
            `Report.add_annotations()`.

            Args:
                title(str): the title used as a heading when the text form is generated.

            Returns:
                Statistics: dataclass with the calculation statistics.
        """
        invested = self.Deposits[-1]
        total_value = self.TotalValue[-1]

        profit = total_value / invested * 100 - 100 if invested else 0

        return Statistics(
            title=title,
            invested=invested,
            total_value=total_value,
            profit=profit,
            other_profit=self.OtherProfit[-1],
            total_trades=self.TotalTrades[-1],
            total_expenses=self.TotalExpenses[-1],
            commission_expense=self.CommissionExpense[-1],
            spread_expense=self.SpreadExpense[-1],
            other_expense=self.OtherExpense[-1]
        )

class BTSymbol(BTBaseData):
    """
        The class which represents the particular symbol used in the strategy. More than one symbols may be used.
    """
    _row_type = BTSymbolRow

    def __init__(self, title=""):
        """Initialize the instance of symbol data class."""
        super().__init__()
        # Title of the symbol
        self.Title = title

    @property
    def Open(self):
        return self._columns[BTSymbolEnum.Open]

    @property
    def Close(self):
        return self._columns[BTSymbolEnum.Close]

    @property
    def High(self):
        return self._columns[BTSymbolEnum.High]

    @property
    def Low(self):
        return self._columns[BTSymbolEnum.Low]

    @property
    def PriceOpenLong(self):
        return self._columns[BTSymbolEnum.PriceOpenLong]

    @property
    def PriceCloseLong(self):
        return self._columns[BTSymbolEnum.PriceCloseLong]

    @property
    def LongPositions(self):
        return self._columns[BTSymbolEnum.LongPositions]

    @property
    def TradesNo(self):
        return self._columns[BTSymbolEnum.TradesNo]

########################
# Base backtesting class
########################

# TODO HIGH Add max position size for lump sum testing
class BackTest(metaclass=abc.ABCMeta):
    def __init__(self,
                 data,
                 commission=0,
                 commission_percent=0,
                 commission_security=0,
                 initial_deposit=0,
                 periodic_deposit=0,
                 deposit_interval=0,
                 cash_interest=0,
                 inflation=0,
                 weighted=None,
                 open_deviation=2,
                 close_deviation=3,
                 grouping_attr=None,
                 grouping_shares=None,
                 offset=0,
                 timeout=10,
                 verbosity=False
        ):
        """
            The main backtesting class.

            Args:
                data(list of BackTestData): the list of data classes for calculation.
                commission(float): commission per trade.
                commission_percent(float): commission in percent of the trade volume.
                commission_security(float): commission per security.
                initial_deposit(float): initial deposit to test the strategy.
                periodic_deposit(float): periodic deposit to the account.
                deposit_interval(int): interval (in days) to add a periodic deposit to the account.
                cash_interest(int): interest on cash balance.
                inflation(float): annual inflation used in the calculation.
                weighted(Weigher): portfolio weigher instance or None for no
                                   weighting. Pass an instance of a backtest.weigher.Weigher subclass.
                open_deviation(float): balance deviation multiplier for opening a position. 1 means that no deviation
                                       is acceptable. 2 means that the 'ideal' weight may be violated up to 2 times.
                close_deviation(float): balance deviation multiplier for closing a position.
                grouping_attr(string): the title of an attribute of BackTestData class to group securities
                grouping_share(dict):  the dictionary which indicates how securities should be grouped according to the
                                       attribute. Contains the attribute value and the desired share of each attribute
                                       in the portfolio.
                offset(int): the offset for the calculation.
                timeout(int): timeout in seconds to cancel the calculation if some thread can not finish in time.
                verbosity(bool): indicates if to print the debug information during calculation.

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

        # Commission per security
        if commission_security < 0:
            raise BackTestError(f"commission_security can't be less than 0. Specified value is {commission_security}")
        self._commission_security = commission_security

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

        # Interest on cash (it may be negative as well):
        self._cash_interest = cash_interest

        # Annual inflation (in percent) to correct the periodic deposit
        if inflation < 0 or inflation > 100:
            raise BackTestError(f"inflation can't be less than 0% or more than 100%. Specified value is {inflation}")
        self._inflation = inflation

        # Portfolio weigher
        if weighted is not None and not isinstance(weighted, Weigher):
            raise BackTestError(f"Unsupported weighted argument: {weighted!r}. Expected a Weigher instance or None.")
        self._weigher = weighted

        # Offset for calculation
        if offset < 0:
            raise BackTestError(f"offset can't be less than 0. Specified value is {offset}")
        self._offset = offset

        # Timeout for calculations
        if timeout < 0:
            raise BackTestError(f"timeout can't be less than 0. Specified value is {timeout}")
        self.__timeout = timeout

        # Indicate if we should print log entries to a console
        self._verbosity = verbosity
        self._lg = Log(verbosity=verbosity)

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
        # Expenses caused paying dividends of lended securities
        self._other_expense = 0

        #######################################
        # General data for backtesting strategy
        #######################################

        # Total number of trades
        self._total_trades = 0

        # Results of the calculation
        self._results = BTData()

        # Counter till deposit date
        self._deposit_counter = 0

        # Calendar days between the current and the previous calculated cycle (cached once per cycle)
        self._days_delta = 0
        # DateTime of the previous calculated cycle (skipped cycles do not advance it)
        self._prev_dt = None

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

        ###################################################
        # Properties related to multithreading calculations
        ###################################################

        # Event which indicates the finishing of calculation
        self.__event = None

        # Separate thread for calculation
        self.__thread = None

        # Moment of time when the calculation has started (time.perf_counter based)
        self.__calc_started = None

        self._compositions = None  # Index compositions in a moment of time

        self._all_symbols = None  # All symbols used in the back test
        self._current_cmp = None  # Current composition of symbols (depends on date time)

        self.__main_data_idx = self._get_biggest_data_idx()  # The index of the main dataset

        # The values used for a diversification
        self._multiplier = 0  # The current multiplier for portfolio weightening
        self._mean_weight = 0  # The mean weight value
        self._total_weighted_value = 0
        self._max_cap = 0
        self._max_price = 0

        if open_deviation > close_deviation:
            raise BackTestError(f"Opening balance deviation should be equal or less than closing one. {open_deviation} > {close_deviation}")

        self._open_deviation = open_deviation
        self._close_deviation = close_deviation

        # The values used for grouping securities
        if grouping_attr is not None and grouping_shares is None or grouping_attr is None and grouping_shares is not None:
            raise BackTestError(f"Both grouping attribute and shares should be specified or none of them. {grouping_attr} and {grouping_shares}")

        self._grouping_attr = grouping_attr
        self._grouping_shares = grouping_shares  # Pre-defined shares for groups
        self._grouping_values = None  # Total value per group
        self._grouping_mult = None  # Number of securities in each group
        self._grouping_means = None  # Mean weights per group
        self._grouping_max_cap = None  # Max capitalization in the group
        self._grouping_capacity = None  # Capacity of the groups

    ###################
    # Public properties
    ###################

    @property
    def composition(self):
        """
            Get the current composition (depends on the current date).
            Used in multy-symbol strategies if index composition changes over time.

            Returns:
                list: the current symbol composition
        """
        return self._current_cmp

    @property
    def all_symbols(self):
        """
            Get all the symbols used in the test.

            Returns:
                list: all the symbols used in the test.
        """
        return self._all_symbols

    @property
    def weigher(self):
        """
            Returns the portfolio weigher.

            Returns:
                Weigher: the portfolio weigher or None.
        """
        return self._weigher

    @property
    def multiplier(self):
        """
            The multiplier value equals the number of all securities with any positions opened.

            Returns:
                int: the multiplier.
        """
        return self._multiplier

    @property
    def mean_weight(self):
        """
            Get the mean weight value used for portfolio diversification.

            Returns:
                float: the mean weight value
        """
        return self._mean_weight

    @property
    def total_weighted_value(self):
        """
            The total value of the weighted symbols of the portfolio.

            Returns:
                float: the total value of the weighted securities.
        """
        return self._total_weighted_value

    @property
    def max_cap(self):
        """
            The maximum capitalization of the securities in the portfolio.

            Returns:
                float: the maximum capitalization of the securities in the portfolio
        """
        return self._max_cap

    @property
    def max_price(self):
        """
            The maximum price of a security in the portfolio.

            Returns:
                float: the maximum price of a security in the portfolio
        """
        return self._max_price

    @property
    def open_deviation(self):
        """
            The maximum portfolio balance deviation when opening a position. 1 means no deviation acceptable.

            Returns:
                float: the maximum possible opening position weight deviation
        """
        return self._open_deviation

    @property
    def close_deviation(self):
        """
            The maximum portfolio balance deviation when closing a position. 1 means no deviation acceptable.

            Returns:
                float: the maximum possible closing position weight deviation
        """
        return self._close_deviation

    @property
    def grouping_attr(self):
        """
            The attribute used for grouping securities in the portfolio.

            Returns:
                str: the attribute title
        """
        return self._grouping_attr

    @property
    def grouping_values(self):
        """
            The calculated values per group.

            Returns:
                str: the grouping values
        """
        return self._grouping_values

    # TODO MID Note that these two properties make sense only if it is possible that some positions may be outside of any
    # group even the grouping is specified. Need to determine if they are needed.
    @property
    def grouping_tv(self):
        """
            Total value of the groups plus cash.

            Returns:
                float: the total group value plus cash.
        """
        return (sum(self._grouping_values.values()) + self.cash
                if self._grouping_values else self.cash)

    @property
    def grouping_invested(self):
        """
            Total value invested into the groups (without cash).

            Returns:
                float: the invested group value (0 if grouping is disabled).
        """
        return sum(self._grouping_values.values()) if self._grouping_values else 0

    @property
    def min_group_capacity(self):
        """
            Get the minimum capacity of all groups.

            Returns:
                float: the minimum capacity of all groups
        """
        if self._grouping_attr:
            return min(self._grouping_capacity.values())
        else:
            return 1

    @property
    def max_group_capacity(self):
        """
            Get the maximum capacity of all groups.

            Returns:
                float: the maximum capacity of all groups
        """
        if self._grouping_attr:
            return max(self._grouping_capacity.values())
        else:
            return 1

    #############
    # Methods
    #############

    def get_grouping_share(self, attr):
        """
            Get the share of a grouping attribute.

            Args:
                attr: group to get its share

            Returns:
                float: the desired share of the group of the group
        """
        try:
            return self._grouping_shares[attr]
        except KeyError as e:
            raise BackTestError(f"The share of the attribute {attr} is not found: {e}")

    def get_group_mean(self, group):
        """
            Get the mean weight value of the group.

            Returns:
                float: mean weight value of the group
        """
        if self._grouping_attr:
            return self._grouping_means[group]
        else:
            return self._mean_weight

    def get_group_max_cap(self, group):
        """
            Get the max cap of the group.

            Returns:
                float: max cap of the group
        """
        if self._grouping_attr:
            return self._grouping_max_cap[group]
        else:
            return self._max_cap

    def get_group_capacity(self, group):
        """
            Get the capacity of the group.
            Highest positive capacity indicates that the group in underpresented in the portfolio. Negative capacity
            indicates that the group weight exceeds pre-defined weight for that group.

            Returns:
                float: capacity of the group
        """
        if self._grouping_attr:
            return self._grouping_capacity[group]
        else:
            return 1

    def calc_global_weight_values(self):
        """
            Calculate and set the global weight values for the current portfolio.
            The values are used by diversification functions.
        """
        if self.weigher is None:
            return

        self._multiplier = 0
        self._mean_weight = 0
        self._total_weighted_value = 0

        for ex in self.all_exec():
            if ex.weighted and ex.has_positions and ex.state is SecState.Active:
                self._multiplier += 1

                ex.calc_weight()
                self._mean_weight += ex.weight

                self._total_weighted_value += ex.total_value

        if self._multiplier:
            self._mean_weight = self._mean_weight / self._multiplier

    # TODO MID The grouping without weighter should be fully addressed
    def calc_grouping_values(self):
        """
            Calculate and set the grouping values (totals, means, capacities) per group.
            Driven by position values and cash only — works with any weigher or without one.
            Note: without a weigher only buy-side group selection works; trade-size capping and
            sell diversification do not apply (get_trade_num() returns 0, mean_weight stays 0).
        """
        if self.grouping_attr is None:
            return

        self._grouping_values = dict.fromkeys(self._grouping_shares.keys(), 0)
        self._grouping_means = dict.fromkeys(self._grouping_shares.keys(), 0)
        self._grouping_mult = dict.fromkeys(self._grouping_shares.keys(), 0)
        self._grouping_capacity = dict.fromkeys(self._grouping_shares.keys(), 0)

        for ex in self.all_exec():
            if ex.weighted and ex.has_positions and ex.state is SecState.Active:
                total_value = ex.total_value
                self._grouping_values[ex.group] += total_value
                self._grouping_mult[ex.group] += 1

        if self.grouping_tv:
            for key, value in self._grouping_values.items():
                self._grouping_capacity[key] = self._grouping_shares[key] - value / self.grouping_tv

                if self._grouping_mult[key]:
                    self._grouping_means[key] = value / self._grouping_mult[key]

    # TODO Adjust everything not just TV and group values
    def adjust_weight_values(self, ex, mult_adj_num, ex_weight, ex_total_value):
        """
            Calculate and set the mean weight of a portfolio.

            Args:
                ex(BackTestOperations): the security execulable
                mult_adj_num(int): the value to adjust the multiplier
                ex_weight(float): the previous weight
                ex_total_value(float): the total value before making a trade
        """
        if self.weigher is not None:
            old_mult = self._multiplier
            self._multiplier += mult_adj_num

            if self._multiplier:
                self._mean_weight = (self._mean_weight * old_mult - ex_weight + ex.weight) / self._multiplier
            else:
                self._mean_weight = 0

            self._total_weighted_value = self._total_weighted_value - ex_total_value + ex.total_value

        if self.grouping_attr is not None:
            # Grouping values are maintained intracycle even without a weigher (trades adjust them via this method).
            # Skip if before the first do_cycle() (grouping dicts not initialized yet).
            if self._grouping_values is None:
                return

            total_value = ex.total_value

            self._grouping_values[ex.group] = self._grouping_values[ex.group] - ex_total_value + total_value

    def _get_biggest_data_idx(self):
        """
            Choose the dataset which will be used as the main data.
            The chosen dataset is the biggest one.

            Returns:
                int: the index of the biggest dataset index.
        """
        num = 0
        target_idx = 0

        for i in range(len(self.data)):
            data = self.data[i]

            current_len = len(data.rows)

            if current_len > num:
                num = current_len
                target_idx = i

        self._lg.plain(f"Using dataset {self.data[target_idx].title} with the index {target_idx} as the main dataset.")

        return target_idx

    def is_finished(self):
        """
            Indicates if the calculation is finished.

            Returns:
                True if calculation is finished, False otherwise.
        """
        return self.__is_finished

    @property
    def long_positions_num(self):
        """
            Return the total number of opened long positions.

            Returns:
                int: the total number of opened long positions
        """
        long_positions_num = 0

        for ex in self.all_exec():
            if ex.state is SecState.Active:
                long_positions_num += ex.long_positions

        return long_positions_num

    @property
    def initial_deposit(self):
        """
            Get the initial deposit.

            Returns:
                float: the initial deposit for the calculation.
        """
        return self._initial_deposit

    @property
    def results(self):
        """
            Get the result list of the calculation.

            Returns:
                list: the results of the calculation.

            Reises:
                BackTestError: results were requested but calculation is not performed.
        """
        if self.__event == None:
            raise BackTestError("Calulation was not performed.")

        result = self.__event.wait(self.__event.time_left())

        if self.__thread != None:
            self.__thread.join()
            self.__thread = None

        if result == False:
            raise BackTestError(f"Timeout ({self.__timeout} sec) has happened. Calculation is not finished.")

        # Rebuild the symbol results on each call so that repeated calls do not duplicate them
        self._results.Symbols = [ex.sym_results for ex in self.__exec]

        return self._results

    @property
    def prev_dt(self):
        """
            Get the DateTime of the previous calculated cycle.

            Skipped cycles do not advance it, so the value always points to the last cycle which was
            actually calculated (None before the first one).

            Returns:
                DateTime: the DateTime of the previous calculated cycle or None.
        """
        return self._prev_dt

    @property
    def days_delta(self):
        """
            Get days delta betwen the current and previous calculated cycle.
            For example, days delta between two working days will be 1, days delta between two trading dates with a holiday
            between them may be much bigger. For intraday bars within the same day it is 0.

            The value is calculated once per cycle and cached, so it is safe to use it in any place of the cycle.

            Returns:
                int: Days delta between two calculation cycles (0 before the first calculated cycle).
        """
        return self._days_delta

    def adjust_days_delta(self):
        """
            Adjust days delta for counters.
        """
        for ex in self.__exec:
            ex.apply_days_counter(self._days_delta)

        self._deposit_counter += self._days_delta

    def deposit(self):
        """
            Check if periodic deposit should be added to the balance and adds it if there is a need.
        """
        # Check if the deposit should be inflation adjusted
        current_year = self.exec().year

        if self._inflation != 0 and self._year != current_year:
            self._year = current_year
            self._periodic_deposit = self._periodic_deposit + self._periodic_deposit * (self._inflation / 100)

        # Add a periodic deposit for each fully passed interval. interval == 0 means no periodic deposit.
        # The remainder is retained, so a deposit missed because of a day off is compensated later.
        if self._periodic_deposit != 0 and self._deposit_interval > 0:
            while self._deposit_interval <= self._deposit_counter:
                self._cash += self._periodic_deposit
                self._deposits += self._periodic_deposit
                self._deposit_counter -= self._deposit_interval

                self._lg.highlight(f"Added a periodic deposit of {self._periodic_deposit}. The cash balance is {round(self.cash, 2)}.")


    def cash_interest(self):
        """
            Check if we have any interest on the cash balance.

            The interest is accrued for each calendar day passed between the calculation cycles (days off included).
            The first cycle of the calculation accrues for one day. Intraday cycles within the same day accrue nothing.
        """
        if self._cash_interest:
            # The first calculated cycle accrues for one day, afterwards the real day delta is used
            days = 1 if self._prev_dt is None else self._days_delta

            if days > 0:
                amount = self._cash * (self._cash_interest / 100 / 365.25) * days

                self.add_other_profit(amount)

    def is_multi_symbol(self):
        """
            Check if data for several symbols was added during the initialization.

            Returns:
                True is data for several symbols presents, False otherwise.
        """
        return len(self.data) > 1

    @property
    def main_data(self):
        """
            Get the first BackTestData instance used in the calculation. It is considered as the 'main' one.

            Returns:
                BackTestData: the first instance added during initialization.
        """
        return self.data[self.__main_data_idx]

    @property
    def data(self):
        """
            Get all BackTestData instances used by the calculation.

            Retunrs:
                list of BackTestData: all the instances used in the calculation.
        """
        return self.__data

    @property
    def index(self):
        """
            Get current index for a calculations.

            Returns:
                int: index for calculation.
        """
        return self.__index

    @index.setter
    def index(self, index):
        """
            Set current index for a calculations.

            Args:
                int: index for calculation.

            Raises:
                BackTestError: index not found.
        """
        if index >= len(self.main_data.rows):
            raise BackTestError(f"Provided data does not have index {index}")

        self.__index = index

    def get_row_index(self, row):
        """
            Get the index of a data row.

            Args:
                row: data row.

            Returns:
                int: index of the row.

            Raises:
                BackTestError: the row is not found in the main dataset.
        """
        idx = np.where(self.main_data.rows[Quotes.TimeStamp] == row[Quotes.TimeStamp])

        if len(idx[0]) == 0:
            raise BackTestError("The row is not found in the main dataset.")

        return idx[0][0]

    def skipped(self, index=None):
        """
            Check if the current cycle should be skipped.
            Skipping criteria is set by the offset of by other factors used in the strategy.

            Returns:
                True if the cycle should be skipped, False otherwise.
        """
        if index == None:
            index = self.index

        if index < 0:
            return True

        return self.skip_criteria(index) or index < self.offset

    def to_skip(self):
        """
            Check if the cycle must be skipped and skip if it is neccessary.

            Returns:
                True if the cycle was skipped, False otherwise.
        """
        if self.skipped():
            self._results.append(BTDataRow(date_time=self.exec().get_datetime_str(), total_trades=0))

            for ex in self.all_exec():
                if ex.get_index() is not None:
                    bar = ex.charting_bar

                    ex.add_symbol_result(BTSymbolRow(
                        open=bar.open,
                        close=bar.close,
                        high=bar.high,
                        low=bar.low))

            return True

        return False

    @property
    def offset(self):
        """
            Get the offset of the calculation.

            Returns:
                int: the offset of the calculation.
        """
        return self._offset

    @offset.setter
    def offset(self, offset):
        """
            Set the offset for the calculation.

            Args:
                offset(int): offset for the calculation.
        """
        self._offset = offset

    @property
    def commission(self):
        """
            Get the commission per trade.

            Returns:
                float: the commission per trade used in the calculation.
        """
        return self._commission

    @property
    def commission_percent(self):
        """
            Get the commission in percent of a trade volume.

            Returns:
                float: the commission in percent of a trade volume. used in the calculation.
        """
        return self._commission_percent

    @property
    def commission_security(self):
        """
            Get the commission per security.

            Returns:
                float: the commission per security used in the calculation.
        """
        return self._commission_security

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
            Add other expenses to the statistics (e.g. dividend tax, delisting losses). Does not affect the cash balance.

            Args:
                other_expenses(float): other expenses
        """
        self._other_expense += other_expense

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

    @property
    def cash(self):
        """
            Get the cash balance.

            Returns:
                float: the current cash balance.
        """
        return self._cash

    @property
    def total_trades(self):
        """
            Get the total number of simulated trades at the moment.

            Retunrs:
                int: the current number of simulated trades.
        """
        return self._total_trades

    @property
    def total_deposits(self):
        """
            Get the total depositted money (initial deposit plus all periodic).

            Returns:
                float: the amount of total money depositted.

        """
        return self._deposits

    @property
    def other_profit(self):
        """
            Get the current other profit.

            Returns:
                float: the current other profit.
        """
        return self._other_profit

    @property
    def commission_expense(self):
        """
            Get the current commission expense.

            Returns:
                float: the current commission expense.
        """
        return self._commission_expense

    @property
    def spread_expense(self):
        """
            Get the current spread expense.

            Returns:
                float: the current spread expense.
        """
        return self._spread_expense

    @property
    def other_expense(self):
        """
            Get the current other expense (e.g. dividend tax, delisting losses).

            Returns:
                float: the current other expense.
        """
        return self._other_expense

    @property
    def total_expenses(self):
        """
            Get the total expenses.

            Returns:
                float: the current total expenses.
        """
        return self.commission_expense + self.spread_expense + self.other_expense

    @property
    def total_value(self):
        """
            Get the total value of the portfolio

            Returns:
                float: the total value of the portfolio.
        """
        total_value = self.cash

        for ex in self.__exec:
            total_value += ex.total_value

        return total_value

    @property
    def result(self):
        """
            Get result of the current cycle.

            Returns:
                BTDataRow: the result of the current cycle.
        """
        for ex in self.__exec:
            ex.add_symbol_result()

        return BTDataRow(
            date_time=self.exec().get_datetime_str(),
            total_value=self.total_value,
            deposits=self.total_deposits,
            cash=self.cash,
            other_profit=self.other_profit,
            commission_expense=self.commission_expense,
            spread_expense=self.spread_expense,
            other_expense=self.other_expense,
            total_expenses=self.total_expenses,
            total_trades=self.total_trades
        )

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

        # Get the initial year
        self._year = self.main_data.first_year

        for data in self.__data:
            self.__exec.append(data.create_exec(self))

        # Get all symbols values used in the test
        self._all_symbols = self._get_all_symbols()

        # Calculate technical data for each symbol
        self.calculate_all_tech()

        self.__is_setup = True

    def all_exec(self):
        """
            Get all BackTestOperations instances used in the strategy.

            Returns:
                list of BackTestOperations: all instances of operation classes used in the strategy.
        """
        return self.__exec

    def exec(self, num=None, data=None):
        """
            Get the BackTestOperations instance to execute the calculations specific to a particular data.

            Args:
                num(int): index for the instance to return. None for main data.
                data(BackTestData): data instance with the associated operations instance.

            Returns:
                BackTestOperations: operations instance.
        """
        instance = None

        if num is None:
            num = self.__main_data_idx

        if data != None:
            try:
                num = self.data.index(data)
            except ValueError as e:
                raise BackTestError(f"Can't find the specified data insance: {e}") from e

        try:
            instance = self.__exec[num]
        except IndexError as e:
            raise BackTestError(f"Can not find the instance with index {num}.") from e

        return instance

    def _get_all_symbols(self):
        """
            Get all the symbols used in the test.

            Returns:
                list: the list of all symbols.
        """
        symbols = []

        for ex in self.all_exec():
            symbols.append(ex.data.title)

        return symbols

    def _get_current_cmp(self):
        """
            Get current index compositions if it differs over time during the test.

            Returns:
                list: current index composition.
        """
        current_dt = get_dt(self.exec().get_row()[Quotes.TimeStamp])
        current_cmp = self._all_symbols

        if self._compositions is not None:
            # Take the first composition as the default value
            current_cmp = next(iter(self._compositions.values()))

            for key in self._compositions.keys():
                key_dt = get_dt(key)

                if current_dt >= key_dt:
                    current_cmp = self._compositions[key]

        return current_cmp

    def do_cycle(self, index):
        """
            Setup the current calculation cycle.

            Args:
                index(int, np.ndarray, np.void): the index of the cycle / row to determine an index.

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

        if isinstance(index, np.ndarray) or isinstance(index, np.void):
            index = self.get_row_index(index)

        if index >= len(self.main_data.rows):
            raise BackTestError(f"Provided data does not have index {index}")

        # do_cycle() was already called for this cycle.
        if self.index == index or self.__is_active:
            raise BackTestError("do_cycle was already called in this cycle.")

        self.index = index

        if self.to_skip():
            return False

        # Set this cycle as active
        self.__is_active = True

        # Reset symbol specific-data
        for ex in self.__exec:
            ex.reset_trade_prices()

        # Set the current compositions depends on the date
        self._current_cmp = self._get_current_cmp()

        # Calculate and set the global weightening data (if needed)
        if self.weigher is not None:
            self.weigher.update_globals(self)
        self.calc_global_weight_values()
        self.calc_grouping_values()

        # Calculate days delta between the cycles once and cache it for the whole cycle
        if self._prev_dt is None:
            self._days_delta = 0
        else:
            self._days_delta = (self.exec().get_datetime() - self._prev_dt).days

        # Apply the cached days delta to per-symbol and deposit counters
        self.adjust_days_delta()

        # Check if we need to make a deposit today. Deposit if we need.
        self.deposit()

        # Check if we have an interest on cash. Apply it if we need.
        self.cash_interest()

        for ex in self.__exec:
            ex.check_delisting()  # Check if a security was delisted (runs even without a fresh bar)

            if ex.get_index() is not None:
                ex.apply_other_balance_changes()  # Get current other profit/expense and apply it to the cash balance
                ex.process_limit_order()  # Execute limit order (if any)

        return True

    def apply_other_balance_changes(self):
        """
            Apply other balance change (like dividends in case of stock and so on).
        """

    def tear_down(self):
        """
            Tear down the current calculation cycle.

            Raises:
                BackTestError: the cycle is not active.
        """
        if self.__is_active == True:
            self._results.append(self.result)
            self.__is_active = False

            # Advance the reference point for days_delta. Skipped cycles never get here,
            # so they do not erase the last calculated cycle.
            self._prev_dt = self.exec().get_datetime()
        else:
            raise BackTestError("The current cycle is not active.")

        self.__is_finished = True

        # Check if the calculation is finished for all instances
        for ex in self.all_exec():
            if ex.check_if_finished() == False:
                self.__is_finished = False
                break

        dt = self.exec().get_row()[Quotes.DateTime]
        # TODO MID The existing data structures for reporting should be altered (replaced by pandas or np.array)
        #tv = round(self._results.TotalValue[-1], 2)
        length = len(self.main_data.rows)

        #self._lg.plain(f"Finished calculating row {self.index + 1} of {length} with datetime {dt} and total value {tv}")
        self._lg.plain(f"Finished calculating row {self.index + 1} of {length} with datetime {dt}")

    def calculate(self):
        """
            Perform the calculation of the entire strategy.
        """
        self.__event = BackTestEvent(self.__timeout)

        self.__calc_started = time.perf_counter()

        if thread_available():
            self.__thread = Thread(target=self.__do_calculation)
            self.__thread.start()
        else:
            self.__do_calculation()

    def __do_calculation(self):
        # Catch any exception which happens in a thread to finish the thread soon then.
        try:
            self.do_calculation()
        except Exception as e:
            raise BackTestError(e) from e
        finally:
            # Store the time actually spent on the calculation (not affected by a delayed results access)
            self._results._elapsed_time = time.perf_counter() - self.__calc_started
            self.__event.set()

    def calculate_all_tech(self):
        """
            Calculate all the required technical data.
        """
        for ex in self.all_exec():
            self.do_tech_calculation(ex)

    def signal_buy(self):
        """
            Determines if a signal to buy is true.

            Returns:
                True if the buy signal is true, False otherwise.
        """

        # In the default case there is no signal verification.
        return False

    def signal_sell(self):
        """
            Determines if a signal to sell is true.

            Returns:
                True if the sell signal is true, False otherwise.
        """

        # In the default case there is no signal verification.
        return False

    def signal_open_long(self):
        """
            Determines if opening a long position is permitted in the current cycle.

            Returns:
                bool: True if opening a long position is permitted, False otherwise.
        """

        # In the default case, opening is always allowed.
        return True

    def any_signal(self):
        """
            Indicates if buy/sell signal was considered as true.

            Returns:
                True/False depending on signal verification.
        """

        return self.signal_buy() or self.signal_sell()

    ##########################
    # Abstract methods
    ##########################

    @abc.abstractmethod
    def do_calculation(self):
        """
            Perform backtest calculation."
        """

    @abc.abstractmethod
    def skip_criteria(self, index):
        """
            Estimate if we should skip the current cycle (no data for calculation and so on).

            Args:
                index(int): index of the cycle to calculate.

            Returns:
                True is the cycle should be skipped, False otherwise.
        """

    @abc.abstractmethod
    def do_tech_calculation(self, ex):
        """
            Perform technical data calculation for the strategy.
        """
