"""Quotes database manager.

The author is Zmicier Gotowka

Distributed under Fcore License 1.1 (see license.md)
"""
# TODO MID Check if we have enough data management methods (with the focus on deleting) and remove CLI tools.
import sys
import getopt

from data import stock, futils
from data.fvalues import Timespans

class QuotesData(stock.RWStockData):
    """
        Base class for CLI quotes manipulation.
    """
    def __init__(self, **kwargs):
        """
            Initialize the base class of CLI quotes manipulations.
        """
        super().__init__(**kwargs)

        # Action should be only one
        self.to_print_all     = False
        self.to_print_quotes  = False
        self.to_build_chart   = False
        self.to_remove_quotes = False

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
        print("Date, Open, High, Low, AdjClose, RawClose, Volume, Dividends, SplitCoefficient, TransactionsNo")

        for row in rows:
            print(list(row))

# Process command line arguments

def arg_parser(argv):
    usage = (f"Usage: {argv[0]} [-h] [-d data file] [-s symbol] [-f from] [-l to] [-q] [-c] [-r] [-a]\n"
            f"Example: {argv[0]} -s AAPL -f 2021-07-22 -l 2022-07-22 -q"
             "Use -h command line option to see detailed help.")

    if len(argv) == 1:
        sys.exit(usage)

    try:
        arguments, _ = getopt.getopt(argv[1:],"hd:s:f:l:qcra",
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

    source = QuotesData()
    source.timespan = Timespans.All

    for argument, value in arguments:
        if argument in ("-h", "--help", ""):
            print("\nAvailable command line options are:\n\n"
                  f"-d or --db_name     - set db_name with the quotes (defauls is {source.db_name})\n"
                   "-s or --symbol      - set symbol for the query\n"
                   "-f or --first_date  - the first date of the data to get. Default is the earliest available.\n"
                   "-l or --last_date   - the last date to get data. Default is today (local date).\n"
                   "-q or --quotes      - get quotes list for specified symbol for specified dates\n"
                   "-c or --chart       - build a chart for specified symbol using specified dates\n"
                   "-r or --remove      - remove specified symbol for specified dates\n"
                   "-a or --all_symbols - list all symbol in the data file\n")
            sys.exit()

        elif argument in ("-d", "--db_name"):
            source.db_name = value
            print(f"Data file is set to: {source.db_name}")

        elif argument in ("-s", "--symbol"):
            source.symbol = value
            print(f"Chosen symbol is {source.symbol}")

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

            source.last_date_set_eod()

            print(f"The last date is {source.last_datetime_str}")

        elif argument in ("-q", "--quotes"):
            source.to_print_quotes = True

        elif argument in ("-c", "--chart"):
            source.to_build_chart = True

        elif argument in ("-r", "--remove"):
            source.to_remove_quotes = True

        elif argument in ("-a", "--all_symbols"):
            source.to_print_all = True

        else:
            sys.exit(usage)

    return source

if __name__ == "__main__":
    data_source = arg_parser(sys.argv)

    data_source.db_connect()

    if data_source.to_print_all == True:
        data_source.print_all_symbols(data_source.get_all_symbols())
        data_source.db_close()
        sys.exit()

    if data_source.to_print_quotes == True:
        if data_source.symbol == "":
            data_source.db_close()
            sys.exit("No symbol specified")
        
        data_source.print_quotes(data_source.get_quotes())
        data_source.db_close()
        sys.exit()

    if data_source.to_build_chart == True:
        if data_source.symbol == "":
            data_source.db_close()
            sys.exit("No symbol specified")

        new_file = futils.build_chart(data_source.get_quotes())
        # TODO LOW Open the chart in a default image viewer
        print(f"{new_file} is written.")
        data_source.db_close()
        sys.exit()

    if data_source.to_remove_quotes == True:
        if data_source.symbol == "":
            data_source.db_close()
            sys.exit("No symbol specified")

        print(f"Number of quotes before removal: {data_source.get_quotes_num()}")
        data_source.remove_quotes()
        print(f"Number of quotes after removal: {data_source.get_quotes_num()}")
        data_source.db_close()
