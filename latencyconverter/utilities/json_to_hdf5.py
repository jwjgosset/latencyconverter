#!/usr/bin/python3.8

from typing import Dict
import json
import pandas as pd
import h5py
import datetime


def load_json(
    source: str
) -> Dict:

    # Open the json file and load it into a dictionary
    with open(source) as json_file:
        availability = json.load(json_file)
    return availability


def init_dataframe() -> pd.DataFrame:
    latency_df = pd.DataFrame(columns=['channel', 'startTime', 'endTime',
                                       'minLatency', 'maxLatency',
                                       'avgLatency', 'allPackets',
                                       'retxPackets'])
    return latency_df


def json_to_table(
    latency_dict: Dict
) -> pd.DataFrame:

    # Unpack the value of 'availability' because it should be the only key at
    # the highest level of the dictionary
    latency_dict = latency_dict['availability']

    # Initialize index
    df_index = 0

    # Initialize a table-format
    df_dict: Dict = {'channel': [], 'startTime': [], 'endTime': [],
                     'minLatency': [], 'maxLatency': [], 'avgLatency': [],
                     'allPackets': [], 'retxPackets': []}

    # Loop through each channel in the list
    for channel_dict in latency_dict:
        channel = channel_dict['id']

        # Loop through each interval for the channel
        for interval_dict in channel_dict['intervals']:

            # Unpack all the values we want
            '''
            start_time = interval_dict['startTime']
            end_time = interval_dict['endTime']
            min_latency = interval_dict['latency']['minimum']
            max_latency = interval_dict['latency']['maximum']
            avg_latency = interval_dict['latency']['average']
            all_packets = interval_dict['retx']['allPackets']
            retx_packets = interval_dict['retx']['retxPackets']
            '''

            df_dict['channel'].append(channel)
            df_dict['startTime'].append(interval_dict['startTime'])
            df_dict['endTime'].append(interval_dict['endTime'])
            df_dict['minLatency'].append(interval_dict['latency']['minimum'])
            df_dict['maxLatency'].append(interval_dict['latency']['maximum'])
            df_dict['avgLatency'].append(interval_dict['latency']['average'])
            df_dict['allPackets'].append(interval_dict['retx']['allPackets'])
            df_dict['retxPackets'].append(interval_dict['retx']['retxPackets'])

            df_index += 1

    latency_df = pd.DataFrame(df_dict)
    return latency_df


def json_to_hdf5(
    filename: str,
    df: pd.DataFrame
):
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
            end_timestamps = channel_df['endTime'].tolist()
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
                                         data=df['avgLatency'].tolist(),
                                         dtype='int32',
                                         compression='gzip',
                                         compression_opts=9)

            if 'samples' in df:
                channel_group.create_dataset(name='samples',
                                             data=df['samples'].tolist(),
                                             dtype='uint16')
            else:
                channel_group.create_dataset(name='samples', data=[],
                                             dtype='uint16')


def writehdf5(
    df: pd.DataFrame,
    destination: str
):
    df.to_hdf(destination,
              key='latency',
              mode='w',
              complevel=9,
              format='table')


def main():
    availability = load_json('../sampledata/QW.QCC01.2022.044.json')

    latency_df = json_to_table(availability)

    json_to_hdf5('../sampledata/QW.QCC01.2022.044.hdf5', latency_df)

    # df2 = pd.read_hdf('../sampledata/QW.QCC01.2022.044.hd5',
    #                  key='latency',
    #                  mode='r',
    #                  )


if __name__ == '__main__':
    main()
