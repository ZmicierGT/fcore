"""Quotes database manager.

The author is Zmicier Gotowka

Distributed under Fcore License 1.0 (see license.md)
"""

import sys
import getopt

from data import fdata, futils
from data.fvalues import Timespans

class QuotesQuery(fdata.Query):
    def __init__(self):
        super().__init__()
        self.source_title = "Quotes"

        # Action should be only one
        self.to_print_all     = False
        self.to_print_quotes  = False
        self.to_build_chart   = False
        self.to_remove_quotes = False

class QuotesData(fdata.ReadWriteData):
    def print_all_symbols(self, rows):
        print("Ticker          ISIN          Description")
        print("-------------------------------------------")

        for row in rows:
            isin = row[1]
            desc = row[2]

            if isin is None:
                isin = "Not specified"
            if desc is None:
                desc = "Not specified"
            
            print(f"{row[0]:<16}{isin:<14}{desc}")

    def print_quotes(self, rows):
        print("Symbol, ISIN, Source, Date/Time, Timespan, Open, High, Low, Close, AdjClose, Volume, Dividends, Transactions, VWAP")

        for row in rows:
            print(row)

# Process command line arguments

def arg_parser(argv):
    usage = (f"Usage: {argv[0]} [-h] [-d data file] [-s symbol] [-f from] [-l to] [-q] [-c] [-r] [-a]\n"
            f"Example: {argv[0]} -s AAPL -f 2019-07-22 -l 2021-07-22 -q"
             "Use -h command line option to see detailed help.")

    if len(argv) == 1:
        sys.exit(usage)

    try:
        arguments, values = getopt.getopt(argv[1:],"hd:s:f:l:qcra", 
                                                    ["help", 
                                                    "db_name=",
                                                    "symbol=",
                                                    "first_date=",
                                                    "last_date=",
                                                    "quotes",
                                                    "chart",
                                                    "remove"
                                                    "all_symbols"])
    except getopt.GetoptError:
        sys.exit(usage)

    query = QuotesQuery()
    query.timespan = Timespans.All

    # No need to execute it in UT
    if ('unittest' in sys.modules) == False:
        futils.parse_config(query)
        query.db_connect()

    for argument, value in arguments:
        if argument in ("-h", "--help", ""):
            print("\nAvailable command line options are:\n\n"
                  f"-d or --db_name     - set db_name with the quotes (defauls is {query.db_name})\n"
                   "-s or --symbol      - set symbol for the query\n"
                   "-f or --first_date  - the first date of the data to get. Default is the earliest available.\n"
                   "-l or --last_date   - the last date to get data. Default is today (local date).\n"
                   "-q or --quotes      - get quotes list for specified symbol for specified dates\n"
                   "-c or --chart       - build a chart for specified symbol using specified dates\n"
                   "-r or --remove      - remove specified symbol for specified dates\n"
                   "-a or --all_symbols - list all symbol in the data file\n")
            query.db_close()
            sys.exit()

        elif argument in ("-d", "--db_name"):
            query.db_name = value
            print(f"Data file is set to: {query.db_name}")

        elif argument in ("-s", "--symbol"):
            query.symbol = value
            print(f"Chosen symbol is {query.symbol}")

        elif argument in ("-f", "--first_date"):
            result, ts = futils.check_datetime(value)
            if result is False:
                print("\n" + usage)
                query.db_close()
                sys.exit("\nThe date is incorrect.")
            else:
                print(f"The first date is {value}")
                query.first_date = ts

        elif argument in ("-l", "--last_date"):
            result, ts = futils.check_datetime(value)
            if result is False:
                print("\n" + usage)
                query.db_close()
                sys.exit("\nThe date is incorrect.")
            else:
                print(f"The last date is {value}")
                if len(value) <= 10:
                    # Set the time to 23:59:59 for end of day quotes
                    query.last_date = ts + 86399
                else:
                    query.last_date = ts

        elif argument in ("-q", "--quotes"):
            query.to_print_quotes = True

        elif argument in ("-c", "--chart"):
            query.to_build_chart = True

        elif argument in ("-r", "--remove"):
            query.to_remove_quotes = True

        elif argument in ("-a", "--all_symbols"):
            query.to_print_all = True

        else:
            sys.exit(usage)

    return query

if __name__ == "__main__":
    query = arg_parser(sys.argv)
    quotes = QuotesData(query)

    if query.to_print_all == True:
        quotes.print_all_symbols(quotes.get_all_symbols())
        query.db_close()
        sys.exit()

    if query.to_print_quotes == True:
        if query.symbol == "":
            query.db_close()
            sys.exit("No symbol specified")
        
        quotes.print_quotes(quotes.get_quotes())
        query.db_close()
        sys.exit()

    if query.to_build_chart == True:
        if query.symbol == "":
            query.db_close()
            sys.exit("No symbol specified")

        new_file = futils.build_chart(quotes.get_quotes())
        print(f"{new_file} is written.")
        query.db_close()
        sys.exit()

    if query.to_remove_quotes == True:
        if query.symbol == "":
            query.db_close()
            sys.exit("No symbol specified")

        print(f"Number of quotes before removal: {quotes.get_quotes_num()}")
        quotes.remove_quotes()
        print(f"Number of quotes after removal: {quotes.get_quotes_num()}")
        query.db_close()
