#!/usr/bin/python3

"""CLI for Polygon.IO API wrapper.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import sys
import getopt
import configparser

from data import polygon, futils, fdata

# Parse ini-file section related to Polygon.IO
def polygon_parse_config(query):
    # Call base function
    futils.parse_config(query)

    # Read/write additional values from the settings.ini file
    config_parser = configparser.ConfigParser()

    # Get values from ini.file
    ini_file = "settings.ini"

    try:
        config_parser.read(ini_file)
        settings = config_parser[query.source_title]

        query.year_delta = settings['year_delta']
        query.api_key = settings['api_key']
    except:
        # Using default values from PolygonQuery constructor if configuration can't be read
        # and save settings for future use.

        config_parser.set(query.source_title, "year_delta", query.year_delta)
        config_parser.set(query.source_title, "api_key", query.api_key)

        with open(ini_file, 'w') as config_file:
            config_parser.write(config_file)
        config_file.close()

    return query

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

    query = polygon.PolygonQuery()

    # No need to execute it in UT
    if ('unittest' in sys.modules) == False:
        query = polygon_parse_config(query)

    for argument, value in arguments:
        if argument in ("-h", "--help", ""):
            print("\nAvailable command line options are:\n\n"
                  f"-d or --db_name    - set db_name to store quotes (defauls is {query.db_name})\n"
                   "-s or --symbol     - symbol to retreive quotes\n"
                   "-t or --timespan   - timespan (either Day or Intraday, in case of intraday minute quotes are requested), default is day.\n"
                  f"-f or --first_date - the first date of the data to get. Default is {query.year_delta} years ago (local date).\n"
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
            if value.capitalize() not in ("Day", "Intraday"):
                sys.exit(usage)
            query.timespan = value.capitalize()
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

            print(f"The last date is {query.last_date_str}")

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
        source = polygon.Polygon(query)
        query.db_connect()

        num_before, num_after = source.insert_quotes(source.fetch_quotes())
    except fdata.FdataError as e:
        query.db_close()
        sys.exit(e)

    query.db_close()

    print(f"The database is updated. The number of entries before update is {num_before}, the number of entries after the update is {num_after}")
