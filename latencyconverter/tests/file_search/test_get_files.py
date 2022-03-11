from latencyconverter.utilities import file_search
from datetime import date
import pytest
from latencyconverter.tests import TESTDATA


# Test if the 4 files inside the 2021-01-01 subdirectory of the testdata
# folder will be found.
def test_get_files():
    test_date = date(2021, 1, 1)
    files = file_search.get_files(test_date, TESTDATA)

    assert len(files) == 4
    print(files)


# Test that a FileNotFoundError will be raised if no files are found.
def test_get_files_fail():
    test_date = date(2021, 1, 2)

    with pytest.raises(FileNotFoundError):
        file_search.get_files(test_date, TESTDATA)
