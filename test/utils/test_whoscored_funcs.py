from unittest.mock import patch, MagicMock, sentinel
import pytest


def test_get_xthreat_grid():

    with patch("requests.get", return_value=MagicMock()) as requests_mock:
        requests_mock.return_value.status_code = 200
        requests_mock.return_value.json = MagicMock(return_value=sentinel.json)

        from footmav.utils.whoscored_funcs import get_xthreat_grid

        xthread_grid = get_xthreat_grid()
        assert xthread_grid == sentinel.json
        requests_mock.assert_called_once_with(
            "https://karun.in/blog/data/open_xt_12x8_v1.json"
        )


def test_get_xthreat_grid_error():

    with patch("requests.get", return_value=MagicMock()) as requests_mock:
        requests_mock.return_value.status_code = 400
        from footmav.utils.whoscored_funcs import get_xthreat_grid

        with pytest.raises(Exception, match="Could not retrieve xthreat grid"):
            get_xthreat_grid()
