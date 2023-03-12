from types import MethodType
from enum import Enum
from footmav.utils import whoscored_funcs as WF


class EventAggregationProcessor:
    aggregators = {}

    def __init__(self, name, f, suffix, persistent):
        self._name = name
        self._suffix = suffix
        self._f = f
        self._extra_functions = dict()
        self._persistent = persistent

    def __call__(self, dataframe):
        if self.col_name not in dataframe.columns:

            dataframe[self.col_name] = self._f(dataframe)
        return dataframe[self.col_name]

    @property
    def name(self):
        return self._name

    @property
    def col_name(self):
        return (
            self._name
            if not self.extra_functions or not self._suffix
            else f"{self._name}_{self._suffix}"
        )

    @property
    def extra_functions(self):
        return self._extra_functions

    @property
    def persistent(self):
        return self._persistent


class VerticalAreas(Enum):
    DefBox = "def_pen_area"
    Def = "def_3rd"
    Mid = "mid_3rd"
    Att = "att_3rd"
    AttBox = "att_pen_area"


def vertical_area_function_maker(
    instance, area: VerticalAreas, end_coordinate: bool = False
):
    name = f"{instance.name}_{area.value}"
    x = "endX" if end_coordinate else "x"
    y = "endY" if end_coordinate else "y"
    if area == VerticalAreas.DefBox:

        def _f(self, dataframe):
            return self(dataframe) & (WF.in_defensive_box(dataframe, ~end_coordinate))

    if area == VerticalAreas.Def:

        def _f(self, dataframe):
            return self(dataframe) & (dataframe[x] <= 33.0)

    if area == VerticalAreas.Mid:

        def _f(self, dataframe):
            return self(dataframe) & (dataframe[x] > 33.0) & (dataframe[x] <= 66.0)

    if area == VerticalAreas.Att:

        def _f(self, dataframe):
            return self(dataframe) & (dataframe[x] > 66.0)

    if area == VerticalAreas.AttBox:

        def _f(self, dataframe):
            return self(dataframe) & (WF.in_attacking_box(dataframe, ~end_coordinate))

    setattr(instance, area.value, MethodType(_f, instance))
    instance.extra_functions[name] = getattr(instance, area.value)


def event_aggregator(
    f_cal=None, suffix="attempted", success: str = "", vertical_areas=0, persistent=True
):
    assert callable(f_cal) or f_cal is None

    def _decorator(f):

        EventAggregationProcessor.aggregators[f.__name__] = EventAggregationProcessor(
            f.__name__, f, suffix, persistent
        )
        instance = EventAggregationProcessor.aggregators[f.__name__]
        if success:

            def _success(self, dataframe):
                return self(dataframe) & WF.success(dataframe)

            instance.success = MethodType(_success, instance)
            instance.extra_functions[f"{instance.name}_{success}"] = instance.success
        if vertical_areas == 3 or vertical_areas == 5:
            vertical_area_function_maker(instance, VerticalAreas.Def, False)
            vertical_area_function_maker(instance, VerticalAreas.Mid, False)
            vertical_area_function_maker(instance, VerticalAreas.Att, False)
            if vertical_areas == 5:
                vertical_area_function_maker(instance, VerticalAreas.DefBox, False)
                vertical_area_function_maker(instance, VerticalAreas.AttBox, False)

        return instance

    return _decorator(f_cal) if callable(f_cal) else _decorator
