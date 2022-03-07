'''
This command line tool is used to convert a single json or csv file to a
compressed HDF5 format.

usage: lat_to_hdf5 [-h] [-c CSV] [-j JSON] -d DESTINATION

  -h, --help            show this help message and exit
  -c CSV, --csv CSV     Specify path to source csv file
  -j JSON, --json JSON  Specify path to source json file
  -d DESTINATION, --destination DESTINATION
                        Specify path to destination hdf5 file.

'''
import logging
import argparse
from latencyconverter.utilities.csv_to_hdf5 import store_csv
from latencyconverter.utilities.json_to_hdf5 import store_json


def main():
    # Initialize argument parser
    argsparser = argparse.ArgumentParser()

    # Add argument for specifying csv file
    argsparser.add_argument(
        '-c',
        '--csv',
        help='Specify path to source csv file',
        default=None,
        type=str
    )

    # Add argument for specifying json file
    argsparser.add_argument(
        '-j',
        '--json',
        help='Specify path to source json file',
        default=None,
        type=str
    )

    # Add argument for specifying destination file
    argsparser.add_argument(
        '-d',
        '--destination',
        help='Specify path to destination hdf5 file.',
        required=True,
        type=str
    )
    argsparser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='Sets logging level to DEBUG.'
    )

    # Parse the args!
    args = argsparser.parse_args()

    # Set logging parameters
    logging.basicConfig(
        format='%(asctime)s:%(levelname)s:%(message)s',
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG if args.verbose else logging.INFO)

    # Fail if the user tried to specify two different source files
    if args.csv is not None and args.json is not None:
        raise ValueError("Can't specify csv file AND json file. Pick one!")
    elif args.csv is not None:
        store_csv(args.csv, args.destination)
    elif args.json is not None:
        store_json(args.json, args.destination)
    else:
        raise FileNotFoundError("No source file specified!")


if __name__ == '__main__':
    main()
