from latencyconverter.utilities import json_to_hdf5
from latencyconverter.tests import TESTDATA
import pytest


def test_load_json():
    availability = json_to_hdf5.load_json(
        f'{TESTDATA}/2021/01/01/QW.QCC01.2022.043.json')
    assert 'availability' in availability


def test_load_json_fail():
    with pytest.raises(ValueError):
        json_to_hdf5.load_json(f'{TESTDATA}/2021/01/03/dummy_json.json')
