from unittest.mock import patch
import pandas as pd


def test_net_pass_xt():
    mock_xthreat = [
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        [9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
        [13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24],
        [17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28],
        [21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32],
        [25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36],
        [29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40],
    ]
    with patch(
        "footmav.utils.whoscored_funcs.get_xthreat_grid", return_value=mock_xthreat
    ) as xthread_grid_mock:
        from footmav.utils.xthreat import net_pass_xt
        from footmav.data_definitions.whoscored import constants as C
        from footmav.data_definitions.whoscored import whoscored_columns as wc

        events = pd.DataFrame(
            {
                wc.X.N: [10, 25, 15, 20],
                wc.Y.N: [10, 2, 20, 12],
                wc.END_X.N: [10, 29, 20, 12],
                wc.END_Y.N: [87, 2, 36, 4],
                wc.EVENT_TYPE.N: [
                    C.EventType.Pass,
                    C.EventType.Pass,
                    C.EventType.Pass,
                    C.EventType.MissedShots,
                ],
            }
        )
        xthread = net_pass_xt(events)
        assert xthread.tolist() == [24, 1, 5, 0]
        xthread_grid_mock.assert_called_once()
