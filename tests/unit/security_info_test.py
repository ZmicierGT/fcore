"""Unit tests for the security info flow (get_info, _add_info, non-existent marking) using real YF
instances with synthetic data injected at the yfinance boundary. Fully offline (:memory:/tmp DB)."""
import pytest

from data.fdata import FdataError, SecData
from data.fvalues import SecType, Sector

from conftest import FakeTicker

# TODO MID Add get_info cases for timezones, security types.

# Synthetic info payloads (shapes exactly like yfinance ticker.info dicts)
EQUITY_INFO = {'quoteType': 'EQUITY', 'symbol': 'FFFF', 'exchangeTimezoneName': 'America/New_York', 'sector': 'Technology'}
EQUITY_NO_SECTOR = {'quoteType': 'EQUITY', 'symbol': 'FFFF', 'exchangeTimezoneName': 'America/New_York'}
# Degenerate placeholder returned by yfinance for non-existent tickers (all expected keys missing).
# Delisted symbols are a particular case of a non-existent ticker.
NON_EXISTENT_INFO = {'trailingPegRatio': None}

##############################
# A. Base SecData.get_info()
##############################

def test_base_info_valid_stock(make_yf):
    inst, fake = make_yf(FakeTicker(info=EQUITY_INFO))
    inst.db_connect()

    assert SecData.get_info(inst) == {'time_zone': 'America/New_York',
                                      'sec_type': SecType.Stock,
                                      'currency': 'Unknown'}
    assert fake.info_calls == 1
    assert inst._get_data_num('sec_info') == 1
    inst.db_close()


def test_base_info_cached(make_yf):
    inst, fake = make_yf(FakeTicker(info=EQUITY_INFO))
    inst.db_connect()

    first = SecData.get_info(inst)
    second = SecData.get_info(inst)

    assert first == second
    # Cached _info: no second fetch
    assert fake.info_calls == 1
    inst.db_close()


##############################
# B. Base get_info() - non-existent
##############################

def test_non_existent_info_raises_and_marks_db(make_yf):
    inst, _ = make_yf(FakeTicker(info=NON_EXISTENT_INFO))
    inst.db_connect()

    with pytest.raises(FdataError, match='delisted or incorrect'):
        SecData.get_info(inst)

    # The row IS persisted and marked as NotExist (served from the cached _info set before the raise)
    assert inst._info['sec_type'] == SecType.NotExist
    assert inst._get_data_num('sec_info') == 1
    inst.db_close()


def test_non_existent_not_retried_on_cached_call(make_yf):
    inst, fake = make_yf(FakeTicker(info=NON_EXISTENT_INFO))
    inst.db_connect()

    with pytest.raises(FdataError):
        SecData.get_info(inst)
    with pytest.raises(FdataError):
        SecData.get_info(inst)

    # Attempted once; second call served from cached non-existent _info
    assert fake.info_calls == 1
    inst.db_close()


def test_non_existent_not_retried_across_instances(make_yf, tmp_path):
    db = str(tmp_path / 'test.sqlite')

    inst1, _ = make_yf(FakeTicker(info=NON_EXISTENT_INFO), db_name=db)
    inst1.db_connect()
    with pytest.raises(FdataError):
        SecData.get_info(inst1)
    assert inst1._info['sec_type'] == SecType.NotExist
    inst1.db_close()

    # Second instance on the same DB: reads the persisted NotExist record without re-fetching
    inst2, fake = make_yf(FakeTicker(info=NON_EXISTENT_INFO), db_name=db)
    inst2.db_connect()
    with pytest.raises(FdataError):
        SecData.get_info(inst2)
    assert fake.info_calls == 0
    inst2.db_close()


def test_non_existent_recovery_with_refetch(make_yf, tmp_path):
    db = str(tmp_path / 'test.sqlite')

    inst1, _ = make_yf(FakeTicker(info=NON_EXISTENT_INFO), db_name=db)
    inst1.db_connect()
    with pytest.raises(FdataError):
        SecData.get_info(inst1)
    assert inst1._info['sec_type'] == SecType.NotExist
    inst1.db_close()

    # refetch=True + valid injected info repairs the record in place (UPSERT, same row count)
    inst2, _ = make_yf(FakeTicker(info=EQUITY_INFO), db_name=db, refetch=True)
    inst2.db_connect()

    info = SecData.get_info(inst2)
    assert info['sec_type'] == SecType.Stock
    assert inst2._get_data_num('sec_info') == 1
    inst2.db_close()


def test_non_existent_sectype_and_timezone_raise(make_yf):
    inst, _ = make_yf(FakeTicker(info=NON_EXISTENT_INFO))
    inst.db_connect()

    with pytest.raises(FdataError):
        _ = inst.sectype
    with pytest.raises(FdataError):
        _ = inst.timezone
    inst.db_close()


##############################
# C. get() early-abort on non-existent
##############################

def test_get_early_abort_on_non_existent(make_yf):
    inst, fake = make_yf(FakeTicker(info=NON_EXISTENT_INFO))
    inst.db_connect()

    with pytest.raises(FdataError):
        inst.get()

    # No quote fetch happened and no interval is recorded for a non-existent ticker
    assert fake.info_calls >= 1
    assert inst.get_quotes_num(dt=False) == 0
    assert inst._get_interval_ts(inst.timespan, is_max=False) is None
    inst.db_close()


##############################
# D. StockData.get_info() - sector handling
##############################

def test_stock_info_sector_stored(make_yf):
    inst, fake = make_yf(FakeTicker(info=EQUITY_INFO))
    inst.db_connect()

    info = inst.get_info()
    assert info['sec_type'] == SecType.Stock
    assert info['sector'] == Sector.Technology

    assert inst._get_data_num('stock_info') == 1
    # Sector written during the base get_info _add_info call (StockData._add_info is polymorphic)
    assert fake.info_calls == 1
    inst.db_close()


def test_stock_info_cached_at_stock_layer(make_yf):
    inst, fake = make_yf(FakeTicker(info=EQUITY_INFO))
    inst.db_connect()

    first = inst.get_info()
    second = inst.get_info()

    assert first['sec_type'] == SecType.Stock
    assert first['sector'] == Sector.Technology
    # _stock_info is cached after the first call; the sector block is skipped on the second
    # call, so the base (sector-less) dict is returned. The fetch itself happens exactly once.
    assert fake.info_calls == 1
    assert inst._get_data_num('stock_info') == 1
    inst.db_close()


def test_stock_info_unknown_sector_when_missing(make_yf):
    inst, _ = make_yf(FakeTicker(info=EQUITY_NO_SECTOR))
    inst.db_connect()

    info = inst.get_info()
    assert info['sector'] == Sector.Unknown
    inst.db_close()


def test_non_existent_stock_info_leaves_no_stock_info(make_yf):
    inst, _ = make_yf(FakeTicker(info=NON_EXISTENT_INFO))
    inst.db_connect()

    with pytest.raises(FdataError):
        inst.get_info()

    assert inst._get_data_num('stock_info') == 0
    inst.db_close()


##############################
# E. Robustness / guards
##############################

def test_add_info_missing_required_keys(make_yf):
    inst, _ = make_yf(FakeTicker(info=EQUITY_INFO))
    inst.db_connect()

    with pytest.raises(FdataError, match='Key is not found'):
        SecData._add_info(inst, {'fc_currency': 'USD'})
    inst.db_close()


def test_base_default_fetch_info(make_yf):
    inst, _ = make_yf(FakeTicker(info=EQUITY_INFO))

    info = SecData._fetch_info(inst)
    assert info == {'fc_sec_type': SecType.Unknown,
                    'fc_currency': 'Unknown',
                    'fc_time_zone': 'America/New_York'}
