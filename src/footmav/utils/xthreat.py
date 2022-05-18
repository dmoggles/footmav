import pandas as pd

from footmav.utils.whoscored_funcs import get_xthreat_grid
from footmav.data_definitions.whoscored import whoscored_columns as wc
import numpy as np
from footmav.data_definitions.whoscored.constants import EventType


def net_pass_xt(events: pd.DataFrame):
    xt_grid = get_xthreat_grid()
    xt_idx_x = pd.cut(events[wc.X.N], bins=np.linspace(0, 100, 13), labels=range(12))
    xt_idx_y = pd.cut(events[wc.Y.N], bins=np.linspace(0, 100, 9), labels=range(8))
    xt_idx_x_end = pd.cut(
        events[wc.END_X.N], bins=np.linspace(0, 100, 13), labels=range(12)
    )
    xt_idx_y_end = pd.cut(
        events[wc.END_Y.N], bins=np.linspace(0, 100, 9), labels=range(8)
    )
    event_types = events[wc.EVENT_TYPE.N]
    xt_start = np.array(
        [
            xt_grid[j][i] if i < 12 and j < 8 and event_type == EventType.Pass else 0
            for i, j, event_type in zip(xt_idx_x, xt_idx_y, event_types)
        ]
    )

    xt_end = np.array(
        [
            xt_grid[j][i] if i < 12 and j < 8 and event_type == EventType.Pass else 0
            for i, j, event_type in zip(xt_idx_x_end, xt_idx_y_end, event_types)
        ]
    )
    xt_pass_net = xt_end - xt_start
    return xt_pass_net
