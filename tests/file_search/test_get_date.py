import pytest
from latencyconverter.utilities import file_search
from datetime import date, timedelta


@pytest.mark.parametrize('param1, returned',
                         [
                             (None, date.today() - timedelta(days=1)),
                             ('2021-01-01', date(2021, 1, 1))
                         ]
                         )
def test_get_date(param1, returned):
    assert file_search.get_date(param1) == returned


def test_get_date_fail():
    with pytest.raises(ValueError):
        file_search.get_date('Invalid')
