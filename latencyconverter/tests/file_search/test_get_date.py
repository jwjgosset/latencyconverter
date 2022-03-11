import pytest
from latencyconverter.utilities import file_search
from datetime import date, timedelta


# Test that the function returns the expected values.
# A None parameter should return yesterday's date.
@pytest.mark.parametrize('param1, returned',
                         [
                             (None, date.today() - timedelta(days=1)),
                             ('2021-01-01', date(2021, 1, 1))
                         ]
                         )
def test_get_date(param1, returned):
    assert file_search.get_date(param1) == returned


# Test that a ValueError is raised if an invalid parameter is supplied.
def test_get_date_fail():
    with pytest.raises(ValueError):
        file_search.get_date('Invalid')
