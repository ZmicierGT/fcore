"""Structural guard tests for the synthetic FMP fixtures.

These checks keep the committed fixtures reproducible: valid JSON, exact key-sets,
expected record counts, newest-first ordering and a single fictional symbol. No value
logic is verified here - tests treat fixture values as ground truth.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import pytest

from conftest import FMP_SYNTHETIC_DATA_DIR, load_fmp_json

PROFILE_KEYS = ['symbol', 'price', 'marketCap', 'beta', 'lastDividend', 'range', 'change', 'changePercentage',
                'volume', 'averageVolume', 'companyName', 'currency', 'cik', 'isin', 'cusip',
                'exchangeFullName', 'exchange', 'industry', 'website', 'description', 'ceo', 'sector', 'country',
                'fullTimeEmployees', 'phone', 'address', 'city', 'state', 'zip', 'image', 'ipoDate',
                'defaultImage', 'isEtf', 'isActivelyTrading', 'isAdr', 'isFund']

INCOME_KEYS = ['date', 'symbol', 'reportedCurrency', 'cik', 'filingDate', 'acceptedDate', 'fiscalYear', 'period',
               'revenue', 'costOfRevenue', 'grossProfit', 'researchAndDevelopmentExpenses',
               'generalAndAdministrativeExpenses', 'sellingAndMarketingExpenses',
               'sellingGeneralAndAdministrativeExpenses', 'otherExpenses', 'operatingExpenses', 'costAndExpenses',
               'netInterestIncome', 'interestIncome', 'interestExpense', 'depreciationAndAmortization', 'ebitda',
               'ebit', 'nonOperatingIncomeExcludingInterest', 'operatingIncome', 'totalOtherIncomeExpensesNet',
               'incomeBeforeTax', 'incomeTaxExpense', 'netIncomeFromContinuingOperations',
               'netIncomeFromDiscontinuedOperations', 'otherAdjustmentsToNetIncome', 'netIncome',
               'netIncomeDeductions', 'bottomLineNetIncome', 'eps', 'epsDiluted', 'weightedAverageShsOut',
               'weightedAverageShsOutDil']

BALANCE_KEYS = ['date', 'symbol', 'reportedCurrency', 'cik', 'filingDate', 'acceptedDate', 'fiscalYear', 'period',
                'cashAndCashEquivalents', 'shortTermInvestments', 'cashAndShortTermInvestments', 'netReceivables',
                'accountsReceivables', 'otherReceivables', 'inventory', 'prepaids', 'otherCurrentAssets',
                'totalCurrentAssets', 'propertyPlantEquipmentNet', 'goodwill', 'intangibleAssets',
                'goodwillAndIntangibleAssets', 'longTermInvestments', 'taxAssets', 'otherNonCurrentAssets',
                'totalNonCurrentAssets', 'otherAssets', 'totalAssets', 'totalPayables', 'accountPayables',
                'otherPayables', 'accruedExpenses', 'shortTermDebt', 'capitalLeaseObligationsCurrent',
                'taxPayables', 'deferredRevenue', 'otherCurrentLiabilities', 'totalCurrentLiabilities',
                'longTermDebt', 'capitalLeaseObligationsNonCurrent', 'deferredRevenueNonCurrent',
                'deferredTaxLiabilitiesNonCurrent', 'otherNonCurrentLiabilities', 'totalNonCurrentLiabilities',
                'otherLiabilities', 'capitalLeaseObligations', 'totalLiabilities', 'treasuryStock',
                'preferredStock', 'commonStock', 'retainedEarnings', 'additionalPaidInCapital',
                'accumulatedOtherComprehensiveIncomeLoss', 'otherTotalStockholdersEquity', 'totalStockholdersEquity',
                'totalEquity', 'minorityInterest', 'totalLiabilitiesAndTotalEquity', 'totalInvestments',
                'totalDebt', 'netDebt']

CASH_FLOW_KEYS = ['date', 'symbol', 'reportedCurrency', 'cik', 'filingDate', 'acceptedDate', 'fiscalYear', 'period',
                  'netIncome', 'depreciationAndAmortization', 'deferredIncomeTax', 'stockBasedCompensation',
                  'changeInWorkingCapital', 'accountsReceivables', 'inventory', 'accountsPayables',
                  'otherWorkingCapital', 'otherNonCashItems', 'netCashProvidedByOperatingActivities',
                  'investmentsInPropertyPlantAndEquipment', 'acquisitionsNet', 'purchasesOfInvestments',
                  'salesMaturitiesOfInvestments', 'otherInvestingActivities',
                  'netCashProvidedByInvestingActivities', 'netDebtIssuance', 'longTermNetDebtIssuance',
                  'shortTermNetDebtIssuance', 'netStockIssuance', 'netCommonStockIssuance', 'commonStockIssuance',
                  'commonStockRepurchased', 'netPreferredStockIssuance', 'netDividendsPaid', 'commonDividendsPaid',
                  'preferredDividendsPaid', 'otherFinancingActivities',
                  'netCashProvidedByFinancingActivities', 'effectOfForexChangesOnCash', 'netChangeInCash',
                  'cashAtEndOfPeriod', 'cashAtBeginningOfPeriod', 'operatingCashFlow', 'capitalExpenditure',
                  'freeCashFlow', 'incomeTaxesPaid', 'interestPaid']

SCHEMAS = {
    'quotes_non-split-adjusted.json': (['symbol', 'date', 'adjOpen', 'adjHigh', 'adjLow', 'adjClose', 'volume'], 522),
    'dividends.json': (['symbol', 'date', 'recordDate', 'paymentDate', 'declarationDate', 'adjDividend',
                        'dividend', 'yield', 'frequency'], 8),
    'splits.json': (['symbol', 'date', 'numerator', 'denominator', 'splitType'], 2),
    'profile.json': (PROFILE_KEYS, 1),
    'profile_delisted.json': (PROFILE_KEYS, 1),
    'profile_etf.json': (PROFILE_KEYS, 1),
    'profile_unknown_exchange.json': (PROFILE_KEYS, 1),
    'profile_non_existent.json': (None, 0),
    'historical-market-capitalization.json': (['symbol', 'date', 'marketCap'], 68),
    'income-statement_annual.json': (INCOME_KEYS, 5),
    'income-statement_quarterly.json': (INCOME_KEYS, 5),
    'balance-sheet-statement_annual.json': (BALANCE_KEYS, 5),
    'balance-sheet-statement_quarterly.json': (BALANCE_KEYS, 5),
    'cash-flow-statement_annual.json': (CASH_FLOW_KEYS, 5),
    'cash-flow-statement_quarterly.json': (CASH_FLOW_KEYS, 5),
    'recent_quote.json': (None, 1),
}

ORDERED_DESC_FILES = ['quotes_non-split-adjusted.json', 'dividends.json', 'historical-market-capitalization.json']

@pytest.mark.parametrize('name', list(SCHEMAS))
def test_fixture_schema_and_count(name):
    expected_keys, expected_count = SCHEMAS[name]
    data = load_fmp_json(name)

    assert len(data) == expected_count, f"{name}: expected {expected_count} records, got {len(data)}"

    if expected_keys is None:
        return

    assert list(data[0].keys()) == expected_keys, f"{name}: key-set mismatch"
    for record in data:
        assert record['symbol'] == 'FFFF', f"{name}: non-fictional symbol {record.get('symbol')}"

@pytest.mark.parametrize('name', ORDERED_DESC_FILES)
def test_fixture_newest_first(name):
    data = load_fmp_json(name)
    dates = [r['date'] for r in data]
    assert dates == sorted(dates, reverse=True), f"{name}: records must be newest-first"

def test_fixtures_directory_present():
    assert FMP_SYNTHETIC_DATA_DIR.is_dir()
    for name in SCHEMAS:
        assert (FMP_SYNTHETIC_DATA_DIR / name).is_file(), f"missing fixture {name}"
