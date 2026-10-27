"""Deterministic scripted strategy for backtesting unit tests.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
from backtest.base import BackTest


class ScriptedBackTest(BackTest):
    """Backtest strategy which performs configured actions at specified bar indexes.

    Actions are callables receiving the backtest instance. A value of the actions mapping may be
    either one callable or a sequence of callables executed in order. This keeps test scenarios
    explicit and independent of the built-in strategy indicators.
    """

    def __init__(self, actions=None, skip=None, **kwargs):
        super().__init__(**kwargs)
        self._actions = actions or {}
        self._skip = skip or (lambda index: False)

    def skip_criteria(self, index):
        """Skip only bars selected by the test scenario."""
        return self._skip(index)

    def do_tech_calculation(self, ex):
        """No technical indicators are needed for scripted scenarios."""

    def do_calculation(self):
        """Run the scripted actions cycle by cycle."""
        self.setup()

        for index in range(len(self.main_data.rows)):
            if not self.do_cycle(index):
                continue

            action = self._actions.get(index)

            if action is not None:
                if isinstance(action, (list, tuple)):
                    for item in action:
                        item(self)
                else:
                    action(self)

            self.tear_down()
