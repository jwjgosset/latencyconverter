from pandas import DataFrame
from latencyconverter.utilities import csv_to_hdf5
from latencyconverter.tests import TESTDATA


# Test that the sample csv is successfully loaded as a dataframe
def test_load_csv():
    test_df = csv_to_hdf5.load_csv(
        f'{TESTDATA}/2021/01/01/QW_QCN08_9J_HNZ_2022_43.csv')
    assert type(test_df) == DataFrame
    assert 'QW.QCN08.9J.HNZ' in test_df['channel'][0]
    assert '=560/100+3.0' in test_df['data latency'][4]
    assert 'timestamp' in test_df
