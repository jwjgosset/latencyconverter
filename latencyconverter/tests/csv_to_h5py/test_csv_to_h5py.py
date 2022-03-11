from latencyconverter.utilities import csv_to_hdf5
from pandas import DataFrame
from latencyconverter.tests import TESTDATA
import h5py
import datetime


def test_csv_to_h5py():
    data = {
                'timestamp': ['2022/02/13 00:00:05.430000'],
                'channel': ['QW.QCN08.9J.HNZ'],
                'network latency': ['2.3'],
                'data latency': ['=556/100+2.3']
           }
    test_df = DataFrame(data)
    csv_to_hdf5.csv_to_h5py(f'{TESTDATA}/output/test.hdf5', test_df)

    with h5py.File(f'{TESTDATA}/output/test.hdf5', mode='r') as f:
        assert f.name == '/'
        assert list(f.keys())[0] == 'QW.QCN08.9J.HNZ'
        assert f['QW.QCN08.9J.HNZ'].name == '/QW.QCN08.9J.HNZ'
        assert 'sample rate' in list(f['QW.QCN08.9J.HNZ'].attrs.keys())

        expected_timestamp = datetime.datetime.strptime(
            '2022/02/13 00:00:05.430000', "%Y/%m/%d %H:%M:%S.%f").timestamp()

        assert f['/QW.QCN08.9J.HNZ/timestamp'][0] == expected_timestamp
