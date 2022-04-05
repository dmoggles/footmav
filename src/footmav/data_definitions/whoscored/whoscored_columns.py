from footmav.data_definitions import base
from footmav.data_definitions.data_sources import DataSource

EVENT_TYPE = base.IntDataAttribute(
    "event_type", normalizable=False, source=DataSource.WHOSCORED
)

X = base.FloatDataAttribute("x", normalizable=False, source=DataSource.WHOSCORED)
Y = base.FloatDataAttribute("y", normalizable=False, source=DataSource.WHOSCORED)
END_X = base.FloatDataAttribute("endX", normalizable=False, source=DataSource.WHOSCORED)
END_Y = base.FloatDataAttribute("endY", normalizable=False, source=DataSource.WHOSCORED)
