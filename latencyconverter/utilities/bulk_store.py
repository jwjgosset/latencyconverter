'''
Module for converting a list of csv and json files to HDF5 format.

Functions
---------

bulk_store:
    Accepts a list of json and csv files and converts them to an hdf5 format.
'''

from latencyconverter.utilities.csv_to_hdf5 import store_csv
from latencyconverter.utilities.json_to_hdf5 import store_json
import logging


def bulk_store(
    files: list,
    destination: str
):
    '''
    Converts a list of csv and json latency files to a unified hdf5 format.

    Parameters
    ----------
    Files: list
        A list of json or csv files of latency data for Nanometrics and Guralp
        devices respectively.
    '''
    for filename in files:
        if '.csv' in filename:
            store_csv(filename, destination)
        elif '.json' in filename:
            store_json(filename, destination)
        else:
            logging.warning(f'Non csv/json file in list of files: {filename}')
