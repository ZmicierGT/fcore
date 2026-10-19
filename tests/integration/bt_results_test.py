"""Backtesting engine integration check script.

Runs the demo/backtest backtests (GUI disabled) using the real data from the main
database (data.sqlite) and verifies that the produced statistics correspond to the
hardcoded expected values below.

Prerequisite: the main database (data.sqlite) must already be populated with the
historical data used by the demos. Otherwise the demos will attempt to fetch the
data from the external data source which changes the tested conditions and the
failures caused by missing data may be misleading.

The elapsed time is measured by the backtesting engine itself (BTData.ElapsedTime)
and printed at the end of each demo, so the time spent on data fetching from an
external data source is excluded and can't cause a false positive failure. If a
calculation takes more than 15% longer than the hardcoded expected time, only a
warning is emitted (not an error) because the calculation time may differ
significantly on various machines.

Each demo also prints the tested period (the requested date range reported by the
YF data instance). If the period does not match the hardcoded expected dates, a
warning is emitted because the other hardcoded values used for the validation may
not be relevant any more (as a different data range was requested).

The data for historical periods is not supposed to change, so the expected values
are hardcoded here and are not meant to be refreshed.

Usage:
    python -m tests.integration.bt_results_test

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
import re
import subprocess
import sys
import os
from pathlib import Path

from data import lg

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

TIMING_DEVIATION = 0.15  # 15% longer calculation time triggers a timing warning
COMPARE_DECIMALS = 2  # Precision to compare the money values

# The labels to parse from the statistics block (mirrors Statistics.__str__ in backtest/base.py)
METRIC_LABELS = ('Invested',
                 'Total expenses',
                 'Total value',
                 'Commission expense',
                 'Profit',
                 'Spread expense',
                 'Other profit',
                 'Other expense',
                 'Total trades')

# Label may appear anywhere in a line: the statistics layout has two columns (two metrics per line).
# The lookbehind prevents matching a label which is a part of a longer word.
METRIC_PATTERN = re.compile(rf"(?<![A-Za-z])({'|'.join(METRIC_LABELS)}):\s+(-?[\d.]+)%?")

# Expected values for each backtest scenario.
# 'elapsed' is the expected time of a calculation (in seconds) measured by the engine itself.
# 'period_start'/'period_end' is the tested period (start and finish dates) reported by the engine.
EXPECTED = {
    'djia': {
        'title': 'Index Simulation Performance:',
        'Invested': 65046.42,
        'Total expenses': 575.0,
        'Total value': 84197.47,
        'Commission expense': 575.0,
        'Profit': 29.44,
        'Spread expense': 0.0,
        'Other profit': 3700.36,
        'Other expense': 0.0,
        'Total trades': 230,
        'elapsed': 3.12,
        'period_start': '2019-08-14',
        'period_end': '2024-08-14',
    },
    'grouping': {
        'title': 'Grouping Performance:',
        'Invested': 138696.47,
        'Total expenses': 4237.5,
        'Total value': 200359.86,
        'Commission expense': 4237.5,
        'Profit': 44.46,
        'Spread expense': 0.0,
        'Other profit': 23080.43,
        'Other expense': 0.0,
        'Total trades': 1695,
        'elapsed': 2.30,
        'period_start': '2014-08-14',
        'period_end': '2024-08-14',
    },
    'eql': {
        'title': 'Index Simulation Performance:',
        'Invested': 221490.73,
        'Total expenses': 1470.0,
        'Total value': 539714.22,
        'Commission expense': 1470.0,
        'Profit': 143.67,
        'Spread expense': 0.0,
        'Other profit': 63582.2,
        'Other expense': 0.0,
        'Total trades': 588,
        'elapsed': 6.28,
        'period_start': '2009-07-10',
        'period_end': '2024-07-10',
    },
}

DEMOS = (('djia', 'demo.backtest.djia_test'),
         ('grouping', 'demo.backtest.grouping_test'),
         ('eql', 'demo.backtest.eql_test'))


def failure(text):
    """
        Report an error using the built-in logging and exit.
    """
    lg.error(text)
    sys.exit(1)


def parse_statistics_and_elapsed(stdout, expected_title):
    """
        Parse the backtesting statistics, the elapsed time and the tested period from the demo output.

        The statistics block is located by its title (the last occurrence to stay
        after any additional logging which may be added to the demos) and the
        metric lines after it are parsed until all the metrics are collected.
        The elapsed time and the tested period lines are searched in the whole
        output.

        Args:
            stdout(str): the stdout of the demo process.
            expected_title(str): the title of the statistics block to identify.

        Returns:
            metrics(dict): the parsed metrics.
            elapsed(float): the elapsed time in seconds.
            period(tuple): the tested period (start and finish dates).
    """
    lines = stdout.splitlines()

    title_idx = None
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].strip() == expected_title:
            title_idx = i
            break

    if title_idx is None:
        failure(f"Expected statistics title '{expected_title}' was not found in the output.")

    metrics = {}

    for line in lines[title_idx + 1:]:
        for match in METRIC_PATTERN.finditer(line):
            label, value = match.groups()

            if label not in metrics:
                metrics[label] = int(float(value)) if label == 'Total trades' else float(value)

        if len(metrics) == len(METRIC_LABELS):
            break

    missing = [label for label in METRIC_LABELS if label not in metrics]

    if missing:
        failure(f"The following metrics were not parsed from the output: {missing}")

    elapsed = None
    match = re.search(r"Backtesting calculation time:\s+([\d.]+)\s+seconds", stdout)

    if match:
        elapsed = float(match.group(1))

    period = (None, None)
    match = re.search(r"Tested period:\s+(\d{4}-\d{2}-\d{2})\s*-\s*(\d{4}-\d{2}-\d{2})", stdout)

    if match:
        period = (match.group(1), match.group(2))

    return metrics, elapsed, period


def compare_statistics(name, metrics, expected_metrics):
    """
        Compare the parsed metrics to the hardcoded expected values.
    """
    problems = []

    for label, exp_value in expected_metrics.items():
        act_value = metrics[label]

        if label == 'Total trades':
            ok = act_value == exp_value
        else:
            ok = round(act_value, COMPARE_DECIMALS) == round(float(exp_value), COMPARE_DECIMALS)

        if not ok:
            problems.append(f"{label}: expected {exp_value}, got {act_value}")

    if problems:
        failure(f"{name} statistics mismatch:\n" + "\n".join(problems))


def check_timing(name, elapsed, expected_elapsed):
    """
        Check the elapsed time against the hardcoded expected value.

        Only a warning is emitted on a mismatch (including an unparsed value)
        because the calculation time may differ significantly on various
        machines and can't be used as a reliable failure indicator.
    """
    if elapsed is None:
        lg.warning(f"{name}: elapsed time was not parsed from the output.")
        return

    if elapsed > expected_elapsed * (1 + TIMING_DEVIATION):
        lg.warning(f"{name} calculation took {elapsed:.2f}s which is more than "
                   f"{TIMING_DEVIATION * 100:.0f}% longer than expected ({expected_elapsed:.2f}s). "
                   f"Note that the calculation time may differ significantly on various machines.")


def check_period(name, period, exp_start, exp_end):
    """
        Check the tested period against the hardcoded expected dates.

        A mismatch is reported as a warning (not as a failure) because the other
        hardcoded values used for the validation may become irrelevant if a
        different data range was actually tested.
    """
    start, end = period

    if start is None or end is None:
        lg.warning(f"{name}: tested period was not parsed from the output.")
        return

    if start != exp_start or end != exp_end:
        lg.warning(f"{name}: tested period {start} - {end} does not match the expected "
                   f"{exp_start} - {exp_end}. The hardcoded validation values may not be "
                   f"relevant for this data range.")


def run_demo(name, module, expected):
    """
        Run a demo in a subprocess (GUI disabled) and check its results.
    """
    env = os.environ.copy()
    env['SSH_CONNECTION'] = '1'  # Disable chart display

    try:
        proc = subprocess.run([sys.executable, '-m', module],
                              cwd=REPO_ROOT,
                              env=env,
                              capture_output=True,
                              text=True,
                              timeout=3000)
    except subprocess.TimeoutExpired:
        failure(f"{name} demo has timed out and was terminated.")

    if proc.returncode != 0:
        stderr_tail = '\n'.join(proc.stderr.splitlines()[-15:])
        failure(f"{name} demo has failed to run (rc={proc.returncode}).\n{stderr_tail}")

    metrics, elapsed, period = parse_statistics_and_elapsed(proc.stdout, expected['title'])

    compare_statistics(name, metrics, {label: expected[label] for label in METRIC_LABELS})
    check_timing(name, elapsed, expected['elapsed'])
    check_period(name, period, expected['period_start'], expected['period_end'])


def main():
    """
        Run checks for all the backtest scenarios.
    """
    lg.highlight("\nTesting backtesting engine results:\n")

    for name, module in DEMOS:
        print(f"Testing {name} backtest...")
        run_demo(name, module, EXPECTED[name])
        lg.success(f"{name} backtest passed.")

    lg.success("All backtest scenarios passed!")


if __name__ == "__main__":
    main()
