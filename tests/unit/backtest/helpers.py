"""Shared helpers for deterministic backtesting unit tests.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from unittest.mock import patch

import backtest.base as btbase

from scripted import ScriptedBackTest


def run_backtest(backtest):
    """Run a calculation inline and return its results.

    The engine normally starts a worker thread. Forcing the inline path makes exceptions
    propagate to the test and removes timing/threading nondeterminism.
    """
    with patch.object(btbase, 'thread_available', lambda: False):
        backtest.calculate()

    return backtest.results


def run_scripted(data,
                 actions=None,
                 skip=None,
                 **backtest_kwargs):
    """Build and run a ScriptedBackTest over the supplied data."""
    backtest = ScriptedBackTest(data=data, actions=actions, skip=skip, **backtest_kwargs)
    return backtest, run_backtest(backtest)


def symbol(results, index=0):
    """Get a per-symbol result by data index."""
    return results.Symbols[index]
