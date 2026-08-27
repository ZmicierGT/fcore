"""Unit tests for the FMP security info flow (get_info, _fetch_info, delisted/non-existent
marking) using real FMP instances with synthetic data injected at the _query_api boundary.
Fully offline (:memory:/tmp DB).

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import pytest

from data.fdata import FdataError, SecData
from data.fvalues import SecType, Sector, Currency

from conftest import load_fmp_json, make_fmp

PROFILE = load_fmp_json('profile.json')[0]
# time_zone comes from production's exchange mapping table; currency/sector map from the profile directly
EXPECTED_BASE_INFO = {'time_zone': 'America/New_York',
                      'sec_type': SecType.Stock,
                      'currency': Currency(PROFILE['currency'])}
EXPECTED_SECTOR = Sector(PROFILE['sector'])

##############################
# A. Base SecData.get_info()
##############################

def test_base_info_valid_stock(make_fmp):
    inst, fake = make_fmp()
    inst.db_connect()

    assert SecData.get_info(inst) == EXPECTED_BASE_INFO
    assert fake.count('profile') == 1
    assert inst._get_data_num('sec_info') == 1
    inst.db_close()

def test_base_info_cached(make_fmp):
    inst, fake = make_fmp()
    inst.db_connect()

    first = SecData.get_info(inst)
    second = SecData.get_info(inst)

    assert first == second
    # Cached _info: no second fetch
    assert fake.count('profile') == 1
    inst.db_close()

def test_fetch_info_keys_added(make_fmp):
    inst, _ = make_fmp()
    inst.db_connect()

    info = inst._fetch_info()

    # FMP maps the exchange/currency flags onto the fc_* keys
    assert info['fc_time_zone'] == EXPECTED_BASE_INFO['time_zone']
    assert info['fc_sec_type'] == EXPECTED_BASE_INFO['sec_type']
    assert info['fc_currency'] == EXPECTED_BASE_INFO['currency']
    inst.db_close()

##############################
# B. StockData.get_info() - sector handling
##############################

def test_stock_info_sector_stored(make_fmp):
    inst, fake = make_fmp()
    inst.db_connect()

    info = inst.get_info()
    assert info['sec_type'] == SecType.Stock
    assert info['sector'] == EXPECTED_SECTOR

    assert inst._get_data_num('stock_info') == 1
    assert fake.count('profile') == 1
    inst.db_close()

def test_stock_info_cached_at_stock_layer(make_fmp):
    inst, fake = make_fmp()
    inst.db_connect()

    first = inst.get_info()
    second = inst.get_info()

    assert first['sec_type'] == SecType.Stock
    assert first['sector'] == EXPECTED_SECTOR
    # The sector block is skipped on the second call; the fetch happens exactly once
    assert fake.count('profile') == 1
    assert inst._get_data_num('stock_info') == 1
    inst.db_close()

##############################
# C. Delisted / non-existent handling
##############################

def test_delisted_info_raises_and_marks_db(make_fmp):
    inst, fake = make_fmp(profile='profile_delisted.json')
    inst.db_connect()

    with pytest.raises(FdataError, match='delisted or incorrect'):
        SecData.get_info(inst)

    # The row IS persisted and marked as NotExist
    assert inst._info['sec_type'] == SecType.NotExist
    assert inst._get_data_num('sec_info') == 1
    assert fake.count('profile') == 1
    inst.db_close()

def test_non_existent_info_raises_and_marks_db(make_fmp):
    inst, fake = make_fmp(profile='profile_non_existent.json')
    inst.db_connect()

    with pytest.raises(FdataError, match='delisted or incorrect'):
        SecData.get_info(inst)

    assert inst._info['sec_type'] == SecType.NotExist
    assert inst._get_data_num('sec_info') == 1
    assert fake.count('profile') == 1
    inst.db_close()

def test_non_existent_not_retried_on_cached_call(make_fmp):
    inst, fake = make_fmp(profile='profile_non_existent.json')
    inst.db_connect()

    with pytest.raises(FdataError):
        SecData.get_info(inst)
    with pytest.raises(FdataError):
        SecData.get_info(inst)

    # Attempted once; second call served from cached non-existent _info
    assert fake.count('profile') == 1
    inst.db_close()

def test_non_existent_not_retried_across_instances(make_fmp, shared_mem_db):
    inst1, _ = make_fmp(profile='profile_non_existent.json', db_name=shared_mem_db)
    inst1.db_connect()
    with pytest.raises(FdataError):
        SecData.get_info(inst1)
    assert inst1._info['sec_type'] == SecType.NotExist

    # Second instance on the same DB: reads the persisted NotExist record without re-fetching.
    # It must connect before inst1 disconnects: the shared in-memory cache exists only
    # while at least one connection to it is open.
    inst2, fake = make_fmp(profile='profile_non_existent.json', db_name=shared_mem_db)
    inst2.db_connect()
    inst1.db_close()

    with pytest.raises(FdataError):
        SecData.get_info(inst2)
    assert fake.count('profile') == 0
    inst2.db_close()

def test_non_existent_recovery_with_refetch(make_fmp, shared_mem_db):
    inst1, _ = make_fmp(profile='profile_non_existent.json', db_name=shared_mem_db)
    inst1.db_connect()
    with pytest.raises(FdataError):
        SecData.get_info(inst1)
    assert inst1._info['sec_type'] == SecType.NotExist

    # refetch=True + valid injected profile repairs the record in place (UPSERT, same row count).
    # inst2 connects before inst1 disconnects to keep the shared cache alive.
    inst2, _ = make_fmp(db_name=shared_mem_db, refetch=True)
    inst2.db_connect()
    inst1.db_close()

    info = SecData.get_info(inst2)
    assert info['sec_type'] == SecType.Stock
    assert inst2._get_data_num('sec_info') == 1
    inst2.db_close()

def test_non_existent_sectype_and_timezone_raise(make_fmp):
    inst, _ = make_fmp(profile='profile_non_existent.json')
    inst.db_connect()

    with pytest.raises(FdataError):
        _ = inst.sectype
    with pytest.raises(FdataError):
        _ = inst.timezone
    inst.db_close()

def test_get_early_abort_on_non_existent(make_fmp):
    inst, fake = make_fmp(profile='profile_non_existent.json')
    inst.db_connect()

    with pytest.raises(FdataError):
        inst.get()

    # No quote fetch happened and no interval is recorded for a non-existent ticker
    assert fake.count('historical-price-eod') == 0
    assert inst.get_quotes_num(dt=False) == 0
    assert inst._get_interval_ts(inst.timespan, is_max=False) is None
    inst.db_close()

##############################
# D. ETF detection
##############################

def test_base_info_valid_etf(make_fmp):
    inst, fake = make_fmp(profile='profile_etf.json')
    inst.db_connect()

    info = SecData.get_info(inst)
    assert info['sec_type'] == SecType.ETF
    assert info['currency'] == Currency.USD
    assert info['time_zone'] == 'America/New_York'
    assert fake.count('profile') == 1
    inst.db_close()

def test_etf_get_fetches_dividends_and_splits(make_fmp):
    inst, _ = make_fmp(profile='profile_etf.json')
    inst.db_connect()

    inst.get()

    # ETF type also triggers dividend/split fetching in StockData.get
    assert inst.sectype == SecType.ETF
    assert inst.get_dividends_num() > 0
    assert inst.get_split_num() > 0
    inst.db_close()

def test_etf_has_no_stock_info(make_fmp):
    inst, _ = make_fmp(profile='profile_etf.json')
    inst.db_connect()

    info = inst.get_info()

    # The stock sector block is limited to SecType.Stock only
    assert info['sec_type'] == SecType.ETF
    assert 'sector' not in info
    assert inst._get_data_num('stock_info') == 0
    inst.db_close()

##############################
# E. Exchange/currency fallbacks
##############################

def test_unknown_exchange_falls_back_to_new_york(make_fmp):
    inst, fake = make_fmp(profile='profile_unknown_exchange.json')
    inst.db_connect()

    # An unmapped exchange must not mark a valid, actively traded security as NotExist
    info = SecData.get_info(inst)
    assert info['sec_type'] == SecType.Stock
    assert info['time_zone'] == 'America/New_York'
    assert fake.count('profile') == 1
    inst.db_close()

def test_unknown_currency_mapped_to_unknown(make_fmp):
    inst, _ = make_fmp(routes={'profile': [dict(PROFILE, currency='XYZ')]})
    inst.db_connect()

    info = SecData.get_info(inst)
    assert info['sec_type'] == SecType.Stock
    assert info['currency'] == Currency.Unknown
    inst.db_close()

##############################
# F. isEtf/isFund/isAdr sec-type mapping
##############################

def test_fund_mapped_to_etf(make_fmp):
    inst, _ = make_fmp(routes={'profile': [dict(PROFILE, isEtf=False, isFund=True, isAdr=False)]})
    inst.db_connect()

    info = inst.get_info()
    assert info['sec_type'] == SecType.ETF
    assert 'sector' not in info
    assert inst._get_data_num('stock_info') == 0
    inst.db_close()

def test_adr_mapped_to_stock(make_fmp):
    inst, _ = make_fmp(routes={'profile': [dict(PROFILE, isEtf=False, isFund=False, isAdr=True)]})
    inst.db_connect()

    info = SecData.get_info(inst)
    assert info['sec_type'] == SecType.Stock
    inst.db_close()

def test_all_security_flags_false_mapped_to_stock(make_fmp):
    inst, _ = make_fmp(routes={'profile': [dict(PROFILE, isEtf=False, isFund=False, isAdr=False)]})
    inst.db_connect()

    info = SecData.get_info(inst)
    assert info['sec_type'] == SecType.Stock
    inst.db_close()
