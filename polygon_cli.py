"""CLI for Polygon.IO API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""

import sys
import getopt

from data import polygon, fdata

# Process command line arguments

def arg_parser(argv):
    usage = (f"\nUsage: {argv[0]} [-h] [-d data file] -s symbol [-t timespan] [-f from] [-l to] [-r]\n"
             f"Example: {argv[0]} -s AAPL -t Day -f 2021-07-22 -l 2022-07-22\n"
             "Use -h command line option to see detailed help.\n")

    if len(argv) == 1:
        sys.exit(usage)

    try:
        arguments, _ = getopt.getopt(argv[1:],"hd:s:t:f:l:r", ["help", "file=", "symbol=", "timespan=", "first_date=", "last_date=", "--replace"])
    except getopt.GetoptError:
        sys.exit(usage)

    source = polygon.Polygon()

    for argument, value in arguments:
        if argument in ("-h", "--help", ""):
            print("\nAvailable command line options are:\n\n"
                  f"-d or --db_name    - set db_name to store quotes (defauls is {source.db_name})\n"
                   "-s or --symbol     - symbol to retreive quotes\n"
                   "-t or --timespan   - timespan (either Day or Minute), default is Day.\n"
                  f"-f or --first_date - the first date of the data to get. Default is {source.year_delta} years ago (local date).\n"
                   "-l or --last_date  - the last date to get data. Default is today (local date).\n"
                   "-r or --replace    - indicates if the existing records (based on timestamp) will be replaced.\n")
            sys.exit()

        elif argument in ("-d", "--file"):
            source.db_name = value
            print(f"Data file is set to: {source.db_name}")

        elif argument in ("-s", "--symbol"):
            source.symbol = value
            print(f"The symbol is set to {source.symbol}")

        elif argument in ("-t", "--timespan"):
            if value.capitalize() not in ("Day", "Minute"):
                sys.exit(usage)
            source.timespan = value.capitalize()
            print(f"The timespan is set to {source.timespan}")

        elif argument in ("-f", "--first_date"):
            try:
                source.first_date = value
            except ValueError as e:
                print("\n" + usage)
                sys.exit(f"\nThe date is incorrect: {e}")

            print(f"The first date is {source.first_datetime_str}")

        elif argument in ("-l", "--last_date"):
            try:
                source.last_date = value
            except ValueError as e:
                print("\n" + usage)
                sys.exit(f"\nThe date is incorrect: {e}")

            print(f"The last date is {source.last_datetime_str}")

        elif argument in ("-r", "--replace"):
            source.update = True
            print("Existing quotes will be updated.")
        else:
            sys.exit(usage)

        if hasattr(source, "symbol") is False:
            sys.exit("Symbol is not specified.")

    return source

if __name__ == "__main__":
    data_source = arg_parser(sys.argv)

    try:
        data_source.db_connect()

        num_before, num_after = data_source.add_quotes(data_source.fetch_quotes())
    except fdata.FdataError as err:
        data_source.db_close()
        sys.exit(err)

    data_source.db_close()

    print(f"The database is updated. The number of entries before update is {num_before}, the number of entries after the update is {num_after}")
