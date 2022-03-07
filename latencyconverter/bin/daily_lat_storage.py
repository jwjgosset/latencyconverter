'''
This command line tool collects a list of all csv and json files for a day and
stores the contained latency data in a compressed hdf5 formatted file for each.

usage: daily_lat_store [-h] [-t DATE] [-s SOURCE] -d DESTINATION

  -h, --help            show this help message and exit
  -t DATE, --date DATE  The date to find latency files for.
  -s SOURCE, --source SOURCE
                        Source directory to search for latency files in.
  -d DESTINATION, --destination DESTINATION
                        The destination folder in which to store compressed
                        hdf5 files.
'''

import logging
import argparse
from latencyconverter.utilities.file_search import get_date, get_files
from latencyconverter.utilities.bulk_store import bulk_store


def main():

    # Define arguments
    argsparser = argparse.ArgumentParser()
    argsparser.add_argument(
        '-t',
        '--date',
        help='The date to find latency files for.',
        default=None,
        type=str
    )

    argsparser.add_argument(
        '-s',
        '--source',
        help='Source directory to search for latency files in.',
        default='.',
        type=str
    )
    argsparser.add_argument(
        '-d',
        '--destination',
        help='The destination folder in which to store compressed hdf5 files.',
        required=True,
        type=str
    )
    argsparser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='Sets logging level to DEBUG.'
    )

    args = argsparser.parse_args()

    # Set logging parameters
    logging.basicConfig(
        format='%(asctime)s:%(levelname)s:%(message)s',
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG if args.verbose else logging.INFO)

    # Collect files that need to be stored in hdf5 format
    working_date = get_date(args.date)
    files = get_files(working_date, args.source)

    # Convert the files
    bulk_store(files, args.destination)
