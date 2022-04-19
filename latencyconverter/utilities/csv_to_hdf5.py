import pandas as pd
from pandas.core.frame import DataFrame
import h5py
import datetime
import logging


def load_csv(
    source: str,
) -> DataFrame:
    '''
    This function loads a CSV file of latency data from Guralp into a DataFrame

    Parameters
    ----------
    source: Str
        The path the CSV file to load

    Returns
    -------
    DataFrame
        A Pandas DataFrame containing the loaded latency information.
    '''
    csvDF = pd.read_csv(source)
    return csvDF


def csv_to_h5py(
    filename: str,
    df: DataFrame
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

            # Convert the timestamps to unix timestamps
            timestamps = channel_df['timestamp'].tolist()
            timestamps = list(map(lambda item: datetime.datetime.strptime(
                item, "%Y/%m/%d %H:%M:%S.%f").timestamp(), timestamps))

            # Create a compressed dataset containing timestamps
            channel_group.create_dataset(
                name='timestamp',
                data=timestamps,
                dtype='float64',
                compression='gzip',
                compression_opts=9)

            # Create a compressed dataset for network latency
            channel_group.create_dataset(name='network latency',
                                         data=df['network latency'].tolist(),
                                         dtype='float32',
                                         compression='gzip',
                                         compression_opts=9)

            # Try sample rate as property instead?
            sample_rates = list(map(
                lambda item: item.strip('=').split('/')[1].split('+')[0],
                channel_df['data latency']))

            channel_group.attrs.create('sample rate',
                                       data=sample_rates[0],
                                       dtype='uint16')

            # Create a compressed dataset for number of samples
            samples = list(map(
                lambda item: item.strip('=').split('/')[0],
                channel_df['data latency']))
            channel_group.create_dataset(name='samples',
                                         data=samples,
                                         dtype='uint16',
                                         compression='gzip',
                                         compression_opts=9)


def store_csv(
    filename: str,
    destination_dir: str
):
    '''
    This function loads a specified csv file, and then converts the data
    inside to compressed hdf5 format for long term sorage.

    Parameters
    ----------
    filename: str
        Path to the csv file to load and convert to hdf5 format.
    destination_dir: str
        The directory to store the compressed hdf5 files in.
    '''
    file_name_parts = filename.split('/')
    dest_file = f'{file_name_parts[len(file_name_parts) - 1]}.hdf5'

    logging.debug(f'Loading {filename}')
    csvDF = load_csv(filename)
    csv_to_h5py(f'{destination_dir}/{dest_file}', csvDF)
