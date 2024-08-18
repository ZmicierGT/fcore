"""Demo of market cap weight portfolio simulation.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.index_periodic import IndexSim
from backtest.bh import BuyAndHold

from backtest.base import BackTestError
from backtest.stock import StockData
from backtest.reporting import Report

from data.fdata import FdataError
from data.fmp import FmpStock, FmpSubquery
from data.fvalues import Weighted, sector_titles

import settings

import plotly.graph_objects as go
import sys

# Note that you can specify the date up to 30 Jun 2000 even prior to the date of EQL inception.
first_date = "2011-10-01"  # The first date to fetch quotes
last_date = "2022-10-01"  # The last date to fetch quotes

etf = 'SPY'  # ETF which follows the index for comparison. Use another one if comparing performance before 2009-07-10

weighted = Weighted.Cap
mult_buy = 1.1  # Opening a position deviation multiplier. 2 means that the weight of a new position may be x2 than average
mult_sell = 1.2  # Closing position multiplier. Opened position may exceed the mean weight up to this value

deposit = 1000  # Monthly deposit

min_width = 2500  # Minimum width for charting
height = 250  # Height of each subchart in reporting

if __name__ == "__main__":
    if settings.FMP.api_key is None:
        sys.exit("This test requires FMP api key. Get the free key at financialmodelingprep.com and put it in settings.py")

    # Array for the fetched data for all symbols
    allrows = []

    print("Fetchig the required quotes for testing. Press CTRL-C and restart if it stucks.")

    for symbol_idx in ['AAPL', 'IBM', 'BRK-B', 'JPM']:
        try:
            print(f"Checking if quotes for {symbol_idx} is already fetched...")

            fmpi = FmpStock(symbol=symbol_idx, first_date=first_date, last_date=last_date)
            fmpi.get()
            info = fmpi.get_info()
            fmpi.get_cap()

            fmpi.db_connect()
            rows = fmpi.get_quotes(queries=[FmpSubquery('fmp_capitalization', 'cap')])
            fmpi.db_close()
        except FdataError as e:
            sys.exit(e)

        print(f"The total number of quotes used for {symbol_idx} is {len(rows)}.\n")

        data = StockData(rows=rows,
                         title=symbol_idx,
                         spread=0.1,
                         info=info)

        allrows.append(data)

    try:
        rows_etf = FmpStock(symbol=etf, first_date=first_date, last_date=last_date).get()
    except FdataError as e:
        sys.exit(e)

    print(f"The total number of quotes used for {etf} is {len(rows)}.\n")

    etf_data = StockData(rows=rows_etf,
                         title=etf,
                         spread=0.1)

    # Test the index assembling
    idx_sim = IndexSim(data=allrows,
                       commission=2.5,
                       periodic_deposit=deposit,
                       deposit_interval=30,
                       inflation=2.5,  # Periodic deposit is adjusted to the inflation
                       initial_deposit=deposit,
                       weighted=weighted,
                       open_deviation=mult_buy,
                       close_deviation=mult_sell,
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
    not_in_composition = []

    # Display information
    print("Symbol   Shares Num   Value         Share of Portfilio   Market Cap   Profit $   Sector")
    print("-------------------------------------------------------------------------------------------------------")
    for ex in idx_sim.all_exec():
        shares_num = ex.get_long_positions()
 
        title = ex.title
        value = round(ex.get_total_value(), 2)
        if total_value:
            portfolio_share = round(value / total_value * 100, 2)
        else:
            portfolio_share = 0
        cap = round(ex.data().get_rows()[-1]['cap'] / 1000000000, 2)
        sector = ex.data().sector
        profit = round(ex.get_total_profit(), 2)

        stat = f"{title:<9}{shares_num:<13}{value:<14}{portfolio_share:<21}{cap:<13}{profit:<11}{sector}"

        if ex.title in idx_sim.composition:
            print(stat)

            if shares_num == 0:
                zero_positions += 1
        else:
            not_in_composition.append(stat)

    print("\nShares which are not in the current composition:\n")

    for share in not_in_composition:
        print(share)

    print(f"\nTotal stocks in the current composition with 0 positions: {zero_positions}\n")

    # Display information regarding sectors (title, value, % of value)

    sectors = {}

    for title in sector_titles:
        sectors[title] = 0

    for ex in idx_sim.all_exec():
        if ex.data().sector:
            sectors[ex.data().sector] += ex.get_total_value()

    print("Sector                  Value            Share")
    print("----------------------------------------------")

    for key, value in sectors.items():
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
