"""Unit tests for the FMP fundamentals flow (income statement, balance sheet, cash flow),
subquery joins back to quotes and market capitalization, using real FMP instances with
synthetic data injected at the _query_api boundary. Fully offline (:memory: DB).
Expected values are derived from the synthetic JSON fixtures.

FMP fetches both annual and quarterly reports for a statement via two API calls
(period=year / period=quarter); both go into the same table and are distinguished by the
reported_period column.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import bisect

import pytest

from data import fmp
from data.fdata import Subquery
from data.fvalues import StockQuotes
from data.stock import report_year, report_quarter

from conftest import load_fmp_json, fixture_filing_ts, make_fmp, eod_ts

BASE = {'first_date': '2024-08-19', 'last_date': '2026-08-18'}

# (fetched rows) = annual + quarterly fixture records
STATEMENTS = {
    fmp.FMPDataEntries.IncomeStatement: ('income-statement', 'get_income_statement'),
    fmp.FMPDataEntries.BalanceSheet: ('balance-sheet-statement', 'get_balance_sheet'),
    fmp.FMPDataEntries.CashFlow: ('cash-flow-statement', 'get_cash_flow'),
}

def statement_total(base):
    """Total stored records for a statement = annual + quarterly fixture records."""
    return len(load_fmp_json(f'{base}_annual.json')) + len(load_fmp_json(f'{base}_quarterly.json'))

STATEMENT_BASES = {entry: base for entry, (base, _) in STATEMENTS.items()}
STATEMENT_TOTALS = {entry: statement_total(base) for entry, (base, _) in STATEMENTS.items()}

CAP_FIXTURE = load_fmp_json('historical-market-capitalization.json')
CAP_TOTAL = len(CAP_FIXTURE)

def as_of_values(records, quote_ts, field, ts_fn=fixture_filing_ts):
    """Replicate the Subquery semantics: latest record (by ts_fn) which is <= each quote ts."""
    entries = sorted((ts_fn(r), r[field]) for r in records)
    entry_ts = [e[0] for e in entries]
    values = [e[1] for e in entries]
    result = []
    for q in quote_ts:
        i = bisect.bisect_right(entry_ts, q) - 1
        result.append(values[i] if i >= 0 else None)
    return result

def assert_period_columns(inst, quotes, entry, field):
    """Assert that the quarterly/annual subquery columns of `field` equal the as-of fixture values."""
    base = STATEMENT_BASES[entry]
    ts_all = [q[StockQuotes.TimeStamp] for q in quotes]

    expected_q = as_of_values(load_fmp_json(f'{base}_quarterly.json'), ts_all, field=field)
    expected_a = as_of_values(load_fmp_json(f'{base}_annual.json'), ts_all, field=field)

    quarter_subquery = Subquery(entry.title, field, condition=report_quarter, title=f'{field}_quarter')
    annual_subquery = Subquery(entry.title, field, condition=report_year, title=f'{field}_annual')

    rows = inst.get(queries=[quarter_subquery, annual_subquery])

    assert len(rows) == len(ts_all)
    got_any_q = got_any_a = False
    for row, ts, exp_q, exp_a in zip(rows, ts_all, expected_q, expected_a):
        got_q = row[f'{field}_quarter']
        got_a = row[f'{field}_annual']

        assert (got_q is None) == (exp_q is None), f"{field}_quarter mismatch at {ts}"
        assert (got_a is None) == (exp_a is None), f"{field}_annual mismatch at {ts}"

        if got_q is not None:
            got_any_q = True
            assert got_q == pytest.approx(exp_q)
        if got_a is not None:
            got_any_a = True
            assert got_a == pytest.approx(exp_a)

    assert got_any_q, f"no quarterly {field} obtained via the report_quarter subquery"
    assert got_any_a, f"no annual {field} obtained via the report_year subquery"

##############################
# A. Dual-period fetch & storage
##############################

def test_income_statement_fetched_both_periods(make_fmp):
    inst, fake = make_fmp(**BASE)
    inst.db_connect()

    assert inst.get_income_statement() == STATEMENT_TOTALS[fmp.FMPDataEntries.IncomeStatement]
    # One statement = two API calls: annual and quarterly reports fetched together
    assert fake.count('income-statement') == 2
    assert fake.count('period=year') == 1
    assert fake.count('period=quarter') == 1
    assert inst.get_income_statement_num() == STATEMENT_TOTALS[fmp.FMPDataEntries.IncomeStatement]
    assert inst._get_interval_ts(fmp.FMPDataEntries.IncomeStatement.title, is_max=True) is not None
    inst.db_close()

def test_income_statement_cached(make_fmp):
    inst, fake = make_fmp(**BASE)
    inst.db_connect()

    inst.get_income_statement()
    inst.get_income_statement()

    # The fetch marker prevents the second fetch
    assert fake.count('income-statement') == 2
    assert inst.get_income_statement_num() == STATEMENT_TOTALS[fmp.FMPDataEntries.IncomeStatement]
    inst.db_close()

def test_income_statement_refetch_upsert(make_fmp):
    inst, fake = make_fmp(**BASE, refetch=True)
    inst.db_connect()

    inst.get_income_statement()
    inst.get_income_statement()

    # refetch=True forces a re-fetch; UPSERT keeps a single set of rows
    assert fake.count('income-statement') == 4
    assert inst.get_income_statement_num() == STATEMENT_TOTALS[fmp.FMPDataEntries.IncomeStatement]
    inst.db_close()

def test_balance_sheet_and_cash_flow(make_fmp):
    inst, fake = make_fmp(**BASE)
    inst.db_connect()

    assert inst.get_balance_sheet() == STATEMENT_TOTALS[fmp.FMPDataEntries.BalanceSheet]
    assert inst.get_cash_flow() == STATEMENT_TOTALS[fmp.FMPDataEntries.CashFlow]
    # Same dual-period fetch for the other statements
    assert fake.count('balance-sheet-statement') == 2
    assert fake.count('cash-flow-statement') == 2
    assert inst.get_balance_sheet_num() == STATEMENT_TOTALS[fmp.FMPDataEntries.BalanceSheet]
    assert inst.get_cash_flow_num() == STATEMENT_TOTALS[fmp.FMPDataEntries.CashFlow]

    # Cached on the second call
    inst.get_balance_sheet()
    inst.get_cash_flow()
    assert fake.count('balance-sheet-statement') == 2
    assert fake.count('cash-flow-statement') == 2
    inst.db_close()

def test_empty_income_statement(make_fmp):
    # The API returns no reports (e.g. no data for the symbol): no error, just the interval marker
    inst, fake = make_fmp(routes={'income-statement': []})
    inst.db_connect()

    assert inst.get_income_statement() == 0
    assert fake.count('income-statement') == 2
    assert inst.get_income_statement_num() == 0
    assert inst._get_interval_ts(fmp.FMPDataEntries.IncomeStatement.title, is_max=True) is not None
    inst.db_close()

##############################
# B. Subquery joins (quarterly vs annual)
##############################

@pytest.mark.parametrize('field, entry', [
    ('netIncome', fmp.FMPDataEntries.IncomeStatement),
    ('revenue', fmp.FMPDataEntries.IncomeStatement),
    ('totalAssets', fmp.FMPDataEntries.BalanceSheet),
    ('netCashProvidedByOperatingActivities', fmp.FMPDataEntries.CashFlow),
])
def test_statement_values(make_fmp, field, entry):
    inst, _ = make_fmp(**BASE)
    inst.db_connect()

    getattr(inst, STATEMENTS[entry][1])()
    quotes = inst.get()

    # Stored values are validated through the subquery API (no raw SQL)
    assert_period_columns(inst, quotes, entry, field)
    inst.db_close()

##############################
# C. Market capitalization
##############################

def test_cap_values(make_fmp):
    inst, fake = make_fmp(**BASE)
    inst.db_connect()

    assert inst.get_cap() == CAP_TOTAL
    assert fake.count('historical-market-capitalization') == 1

    quotes = inst.get()
    ts_all = [q[StockQuotes.TimeStamp] for q in quotes]
    rows = inst.get(queries=[Subquery(fmp.FMPDataEntries.Capitalization.title, 'cap')])

    # Cap values are joined to the quotes as-of (latest cap <= quote ts)
    expected = as_of_values(CAP_FIXTURE, ts_all, field='marketCap', ts_fn=lambda r: eod_ts(r['date']))

    got_caps = 0
    for row, ts, exp in zip(rows, ts_all, expected):
        cap = row['cap']
        assert (cap is None) == (exp is None), f"cap presence mismatch at {ts}"
        if cap is not None:
            got_caps += 1
            assert cap == exp, f"cap mismatch at {ts}"

    assert got_caps > 0, "no cap values joined to any quote"
    inst.db_close()

def test_cap_cached(make_fmp):
    inst, fake = make_fmp(**BASE)
    inst.db_connect()

    inst.get_cap()
    inst.get_cap()

    assert fake.count('historical-market-capitalization') == 1
    assert inst.get_cap_num() == CAP_TOTAL
    inst.db_close()

def test_cap_refetch_upsert(make_fmp):
    inst, fake = make_fmp(**BASE, refetch=True)
    inst.db_connect()

    inst.get_cap()
    inst.get_cap()

    assert fake.count('historical-market-capitalization') == 2
    assert inst.get_cap_num() == CAP_TOTAL
    inst.db_close()
