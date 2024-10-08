"""Demo of the grouping simulation.

In this demo portfolio is split in three groups: US stocks (50% share), international stocks (25%) and bonds (25%).
Each group consists of several ETFs and their shares are equal weighted inside the group.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.index_periodic import IndexSim
from backtest.bh import BuyAndHold

from backtest.base import BackTestError
from backtest.stock import StockData
from backtest.reporting import Report

from data.fdata import FdataError
from data.yf import YF
from data.fvalues import Weighted

import plotly.graph_objects as go
import sys

first_date = "2014-08-14"  # The first date to fetch quotes
last_date = "2024-08-14"  # The last date to fetch quotes

etf = 'DIA'  # ETF which follows the index (used for comparison)

weighted = Weighted.Equal
mult_buy = 1.1  # Opening a position deviation multiplier. 2 means that the weight of a new position may be x2 than average
mult_sell = 1.2  # Closing position multiplier. Opened position may exceed the mean weight up to this value

symbols = {'SPY': 'US',
           'VTV': 'US',
           'VXUS': 'International',
           'VWO': 'International',
           'AGG': 'Bond',
           'HYG': 'Bond'}

deposit = 1000  # Monthly deposit

min_width = 2500  # Minimum width for charting
height = 250  # Height of each subchart in reporting

if __name__ == "__main__":
    # Array for the fetched data for all symbols
    allrows = []

    warning = "WARNING! Using yfinance data for the demonstration.\n" +\
              "Always keep yfinance up to date ( pip install yfinance --upgrade ) and use quotes obtained from this " +\
              "datasource only for demonstation purposes!\n"
    print(warning)

    print("Fetchig the required quotes for testing. Press CTRL-C and restart if it stucks.")

    for symbol_idx, group in symbols.items():
        try:
            print(f"Checking if quotes for {symbol_idx} is already fetched...")

            yfi = YF(symbol=symbol_idx, first_date=first_date, last_date=last_date)
            rows = yfi.get()
        except FdataError as e:
            sys.exit(e)

        print(f"The total number of quotes used for {symbol_idx} is {len(rows)}.\n")

        data = StockData(rows=rows,
                         title=symbol_idx,
                         spread=0.1)

        data.fund_group = group

        allrows.append(data)

    try:
        rows_etf = YF(symbol=etf, first_date=first_date, last_date=last_date).get()
    except FdataError as e:
        sys.exit(e)

    print(f"The total number of quotes used for {etf} is {len(rows)}.\n")

    etf_data = StockData(rows=rows_etf,
                         title=etf,
                         spread=0.1)

    # Create a dictionary with group weights
    group_weights = {'US': 0.5,
                     'International': 0.25,
                     'Bond': 0.25}

    # Test the index assembling
    idx_sim = IndexSim(data=allrows,
                       commission=2.5,
                       periodic_deposit=deposit,
                       deposit_interval=30,
                       inflation=2.5,  # Periodic deposit is adjusted by the inflation
                       initial_deposit=deposit,
                       weighted=weighted,
                       open_deviation=mult_buy,
                       close_deviation=mult_sell,
                       grouping_attr='fund_group',
                       grouping_shares=group_weights,
                       timeout=2000,
                       verbosity=True
                    )

    # Buy and hold strategy test of the ETF (for comparison)
    bh = BuyAndHold(data=[etf_data],
                    commission=2.5,
                    periodic_deposit=deposit,
                    deposit_interval=30,
                    inflation=2.5,
                    initial_deposit=deposit
                    )

    try:
        idx_sim.calculate()
        bh.calculate()
    except BackTestError as e:
        sys.exit(f"Can't perform backtesting calculation: {e}")

    results = idx_sim.get_results()
    results_cls = bh.get_results()

    #################################################
    # Create a text report of the resulting portfolio
    #################################################

    print('\n')

    if idx_sim.get_total_value() == 0:
        sys.exit("The portfolio has no value!")

    # Display information regarding each holding (shares num, value, % of portfolio, market cap)
    total_value = 0
    total_profit = 0

    # Need calculate all values at first
    for ex in idx_sim.all_exec():
        total_value += ex.get_total_value()
        total_profit += ex.get_total_profit()

    zero_positions = 0

    # Display information
    print("Symbol   Shares Num   Value         Share of Portfilio   Profit $   Group")
    print("------------------------------------------------------------------------------------------")
    for ex in idx_sim.all_exec():
        shares_num = ex.get_long_positions()
 
        title = ex.title
        value = round(ex.get_total_value(), 2)
        if total_value:
            portfolio_share = round(value / total_value * 100, 2)
        else:
            portfolio_share = 0
        group = ex.data().fund_group
        profit = round(ex.get_total_profit(), 2)

        stat = f"{title:<9}{shares_num:<13}{value:<14}{portfolio_share:<21}{profit:<11}{group}"
        print(stat)

        if shares_num == 0:
            zero_positions += 1

    print(f"\nTotal stocks in the current composition with 0 positions: {zero_positions}\n")

    # Display information regarding groups (title, value, % of value)

    groups = {}

    for title in ['US', 'International', 'Bond']:
        groups[title] = 0

    for ex in idx_sim.all_exec():
        if ex.data().fund_group:
            groups[ex.data().fund_group] += ex.get_total_value()

    print("Group                   Value            Share")
    print("----------------------------------------------")

    for key, value in groups.items():
        if value is None:
            continue

        if total_value:
            share = round(value / total_value * 100, 2)
        else:
            share = 0

        print(f"{key:<24}{round(value, 2):<17}{share}")

    print('\n')

    # Calculate stock/cash allocation ratio:

    stock_alloc = total_value
    cash = idx_sim.get_cash()

    if stock_alloc:
        stock_share = stock_alloc / (total_value + cash) * 100
    else:
        stock_share = 0

    if cash:
        cash_share = cash / (total_value + cash) * 100
    else:
        cash_share = 0

    print(f"\nStock allocation: ${round(stock_alloc, 2)}")
    print(f"Stock share: {round(stock_share, 2)} %")

    print(f"\nCash allocation: ${round(cash, 2)}")
    print(f"Cash share: {round(cash_share, 2)} %\n")

    #################
    # Create a report
    #################

    report = Report(data=results_cls, width=max(len(rows), min_width), margin=False)

    # Add charts for used symbols
    report.add_quotes_chart(title=f"B&H Testing for {etf}", height=450)

    # Add a chart to represent portfolio performance
    fig_portf = report.add_portfolio_chart(height=height)

    # Add index simulation comparison to the portfolio chart
    fig_portf.add_trace(go.Scatter(x=results.DateTime, y=results.TotalValue, mode='lines', name=f"Simulation TV"))
    fig_portf.add_trace(go.Scatter(x=results.DateTime, y=results.OtherProfit, mode='lines', name=f"Simulation Yield"))
    fig_portf.add_trace(go.Scatter(x=results.DateTime, y=results.Cash, mode='lines', name=f"Simulation Cash"))

    # Add a chart with expenses
    fig_exp = report.add_expenses_chart(height=height)
    fig_exp.add_trace(go.Scatter(x=results.DateTime, y=results.TotalExpenses, mode='lines', name=f"Simulation Exp"))
    fig_exp.add_trace(go.Scatter(x=results.DateTime, y=results.CommissionExpense, mode='lines', name=f"Simulation Comm"))
    fig_exp.add_trace(go.Scatter(x=results.DateTime, y=results.SpreadExpense, mode='lines', name=f"Simulation Spread"))

    # Add annotations with strategy results
    report.add_annotations(data=results_cls, title='B&H Performance:')
    report.add_annotations(data=results, title='Index Simulation Performance:')

    # Show image
    new_file = report.show_image()
    print(f"{new_file} is written.")
