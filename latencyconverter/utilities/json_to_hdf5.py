'''
This module is used to store latency information from a Nanomatrics
availability json file in a compressed hdf5 format.

Functions
---------

load_json:
    Loads a json file and returns its contents as a dictionary object.

json_to_table:
    This function parses through json formatted data and extracts the values
    that we wish to store, rearranging them into a pandas dataframe.

json_to_hdf5:
    This function takes the latency data from a DataFrame and stores it as an
    HDF5 file.
store_json:
    This function loads a Nanometrics availability json file, extracts latency
    information from it and stores it in a compressed hdf5 format.
'''

from typing import Dict
import json
import pandas as pd
import h5py
import datetime


def load_json(
    source: str
) -> Dict:
    '''
    Attempts to load a json file, and validate that it is in the format of a
    Nanometrics Apollo Server Availability API query.

    Parameters
    ----------

    source: str
        The path to the Nanometrics json file that should be loaded.

    Returns
    -------
    Dict
        Dictionary object containing the json format data.
    '''

    # Open the json file and load it into a dictionary
    with open(source) as json_file:
        availability = json.load(json_file)
        if 'availability' not in availability:
            raise ValueError(f'Invalid json file: {source} does not contain \
                               availability information.')
    return availability


def json_to_table(
    latency_dict: Dict
) -> pd.DataFrame:
    '''
    This function parses through json formatted data and extracts the values
    that we wish to store, rearranging them into a pandas dataframe.

    Parameters
    ----------

    latency_dict: Dict
        Dictionary object containing json format availability information to
        extract values from.

    Returns
    -------
    DataFrame
        Pandas dataframe object containing the values deemed important for
        long term storage.
    '''

    # Unpack the value of 'availability' because it should be the only key at
    # the highest level of the dictionary
    latency_dict = latency_dict['availability']

    # Initialize index
    df_index = 0

    # Initialize a table-format
    df_dict: Dict = {'channel': [], 'starttime': [], 'latency': []}

    # Loop through each channel in the list
    for channel_dict in latency_dict:
        channel = channel_dict['id']

        # Loop through each interval for the channel
        for interval_dict in channel_dict['intervals']:

            # Unpack all the values we want
            df_dict['channel'].append(channel)
            df_dict['starttime'].append(interval_dict['startTime'])
            df_dict['latency'].append(interval_dict['latency']['maximum'])

            df_index += 1

    latency_df = pd.DataFrame(df_dict)
    return latency_df


def json_to_h5py(
    filename: str,
    df: pd.DataFrame
):
    '''
    This function takes the latency data from a DataFrame and stores it as an
    HDF5 file

    Parameters
    ----------

    filename: Str
        This is the location and filename to save the data in hdf5 format

    df: DataFrame
        Pandas DataFrame object containing data extracted from a
        json-formatted Nanometrics Availability API query.
    '''
    # Open the HDF5 file
    with h5py.File(filename, 'w') as hdf5file:

        # Loop through each channel in the dataframe
        for channel in df['channel'].unique():
            channel_df = df.loc[df['channel'] == channel]

            channel_group = hdf5file.create_group(channel)

            # Add the sample rate attribue
            if 'sample rate' in df:
                channel_group.attrs.create('sample rate',
                                           data=df['sample rate'][0],
                                           dtype='uint16')
            else:
                # Default to 100 if no sample rate present
                channel_group.attrs.create('sample rate', data='100',
                                           dtype='uint16')

            # Convert the timestamps to unix timestamps
            end_timestamps = channel_df['starttime'].tolist()
            end_timestamps = list(map(
                lambda item: datetime.datetime.strptime(
                    item[:26], "%Y-%m-%dT%H:%M:%S.%f").timestamp(),
                end_timestamps))

            # Create a compressed dataset containing timestamps
            channel_group.create_dataset(
                name='timestamp',
                data=end_timestamps,
                dtype='float64',
                compression='gzip',
                compression_opts=9)

            # Create a compressed dataset for network latency
            channel_group.create_dataset(name='network latency',
                                         data=df['latency'].tolist(),
                                         dtype='int32',
                                         compression='gzip',
                                         compression_opts=9)

            # Create a dataset for the number of samples in each packet
            if 'samples' in df:
                channel_group.create_dataset(name='samples',
                                             data=df['samples'].tolist(),
                                             dtype='uint16')
            # The Nanometrics Availability API doesn't return this value so
            # generate an empty dataset
            else:
                channel_group.create_dataset(name='samples', data=[],
                                             dtype='uint16')


def store_json(
    filename: str,
    destination_dir: str
):
    '''
    This function loads a Nanometrics availability json file, extracts latency
    information from it and stores it in a compressed hdf5 format.

    Parameters
    ----------

    filename: str
        The file to extract station latency information from.

    destination_dir: str
        The directory to store the compressed hdf5 file in.
    '''
    availability = load_json(filename)

    latency_df = json_to_table(availability)

    json_to_h5py(f'{destination_dir}/{filename}.hdf5', latency_df)
