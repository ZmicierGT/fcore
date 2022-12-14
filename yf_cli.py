#!/usr/bin/python3

"""CLI for Yahoo Finance.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import sys
import getopt

from data import yf, futils, fdata

# Parse ini-file section related to YF
def yf_parse_config(query):
    # Call base function
    futils.parse_config(query)

# Process command line arguments

def arg_parser(argv):
    usage = (f"\nUsage: {argv[0]} [-h] [-d data file] -s symbol [-t timespan] [-f from] [-l to] [-r]\n"
             f"Example: {argv[0]} -s AAPL -t day -f 2019-07-22 -l 2021-07-22\n"
             "Use -h command line option to see detailed help.\n")

    if len(argv) == 1:
        sys.exit(usage)

    try:
        arguments, values = getopt.getopt(argv[1:],"hd:s:t:f:l:r", ["help", "file=", "symbol=", "timespan=", "first_date=", "last_date=", "--replace"])
    except getopt.GetoptError:
        sys.exit(usage)

    query = yf.YFQuery()

    # No need to execute it in UT
    if ('unittest' in sys.modules) == False:
        yf_parse_config(query)

    for argument, value in arguments:
        if argument in ("-h", "--help", ""):
            print("\nAvailable command line options are:\n\n"
                  f"-d or --db_name    - set db_name to store quotes (defauls is {query.db_name})\n"
                   "-s or --symbol     - symbol to retreive quotes\n"
                   "-t or --timespan   - timespan (Day, Week, Month), default is day.\n"
                  f"-f or --first_date - the first date of the data to get. Default is 1970-01-01.\n"
                   "-l or --last_date  - the last date to get data. Default is today (local date).\n"
                   "-r or --replace    - indicates if the existing records (based on timestamp) will be replaced.\n")
            sys.exit()

        elif argument in ("-d", "--file"):
            query.db_name = value
            print(f"Data file is set to: {query.db_name}")

        elif argument in ("-s", "--symbol"):
            query.symbol = value
            print(f"The symbol is set to {query.symbol}")

        elif argument in ("-t", "--timespan"):
            if value not in ("day", "week", "month"):
                sys.exit(usage)
            query.timespan = value
            print(f"The timespan is set to {query.timespan}")

        elif argument in ("-f", "--first_date"):
            try:
                query.first_date = value
            except ValueError as e:
                print("\n" + usage)
                sys.exit(f"\nThe date is incorrect: {e}")

            print(f"The first date is {query.first_date_str}")

        elif argument in ("-l", "--last_date"):
            try:
                query.last_date = value
            except ValueError as e:
                print("\n" + usage)
                sys.exit(f"\nThe date is incorrect: {e}")

            print(f"The last date is {query.first_date_str}")

        elif argument in ("-r", "--replace"):
            query.update = "REPLACE"
            print("Existing quotes will be updated.")
        else:
            sys.exit(usage)

        if hasattr(query, "symbol") is False:
            sys.exit("Symbol is not specified.")

    return query

if __name__ == "__main__":
    query = arg_parser(sys.argv)

    try:
        source = yf.YF(query)
        query.db_connect()

        num_before, num_after = source.insert_quotes(source.fetch_quotes())
    except fdata.FdataError as e:
        query.db_close()
        sys.exit(e)

    query.db_close()

    print(f"The database is updated. The number of entries before update is {num_before}, the number of entries after the update is {num_after}")
