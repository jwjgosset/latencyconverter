import pandas as pd
from pandas.core.frame import DataFrame
import h5py
import datetime


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


def main():
    '''
    THIS IS FOR TESTING ONLY.

    This 'main' function converts sample csv data to HDF5

    This was meant for messing around with options to see what optimizes
    compression!
    '''
    csvDF = load_csv('../sampledata/QW_QCN08_9J_HNZ_2022_44.csv')

    csv_to_h5py('../sampledata/test/test.hdf5', csvDF)


if __name__ == '__main__':
    main()
