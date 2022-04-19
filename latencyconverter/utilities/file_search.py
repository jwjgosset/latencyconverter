'''
Module for finding a list of json and csv files of latency data for a specific
date.

Functions
---------

get_date:
    Converts the supplied date string into a datetime.date object. If none is
    supplied, yesterday's date is returned.

get_files:
    Finds all csv and json files in a specified folder and returns them as a
    list.
'''
from datetime import date, datetime, timedelta
import logging
import subprocess


def get_date(
    date_string: str = None
) -> date:
    '''
    Converts the supplied date string into a datetime.date object. If none is
    supplied, yesterday's date is returned.

    Parameters
    ----------

    date_string: str
        A string in YYYY-MM-DD date format to be converted to a datetime.date
        object. Default: None

    Returns
    -------
    datetime.date
        A datetime.date object representing the supplied date, or yesterday's
        date.
    '''
    # Return yesterday's date and time if none is specified
    if date_string is None:
        return date.today() - timedelta(days=1)
    else:
        try:
            selected_date = datetime.strptime(date_string, '%Y-%m-%d').date()
        except ValueError as e:
            logging.error('Failed to convert date. Date must be in YYYY-MM-DD \
                          format.')
            raise e
        return selected_date


def get_files(
    working_date: date,
    source: str = '.'
) -> list:
    '''
    Finds all csv and json files in a specified folder and returns them as a
    list.

    Parameters
    ----------
    source: str
        The parent directory to start searching for latency files in.
    working_date: date
        Datetime date object representing the date to find latency files for.
        Only used to find a subdirectory within the source directory with a
        folder name in YYYY-MM-DD format. Does not verify that the files
        inside are for the correct date. (Maybe add this later)

    Returns
    -------

    list:
        A list containing the path to all json and csv files found
    '''
    # Initialize list of files
    files: list = []

    # Try to find csv files in the specified directory
    cmd = (f'ls {source}/' +
           f'{working_date.strftime("%Y/%m/%d")}/*_HN*.csv 2>/dev/null')
    output: list = (subprocess.getoutput(cmd)).split('\n')
    logging.info(output)
    if '' not in output:
        files.extend(output)
        logging.info(f'{len(output)} csv files found.')
    else:
        logging.warning('No CSV files found.')

    # Try to find json files in the specified directory
    cmd = f'ls {source}/{working_date.strftime("%Y/%m/%d")}/*.json 2>/dev/null'
    output = (subprocess.getoutput(cmd)).split('\n')
    logging.info(output)
    if '' not in output:
        files.extend(output)
        logging.info(f'{len(output)} json files found.')
    else:
        logging.warning('No json files found.')

    # Raise an error if no files are loaded
    if len(files) == 0:
        raise FileNotFoundError(f'No csv or json files found in {source}/' +
                                f'{working_date.strftime("%Y/%m/%d")}/')

    logging.debug(files)
    return files
