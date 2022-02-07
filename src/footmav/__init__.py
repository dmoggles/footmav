from .version import __version__

__all__ = ["__version__"]


from footmav.operations.filter import filter
from footmav.operations import filter_objects as filters
from footmav.operations.filter_objects import Filter
from footmav.operations.aggregations import aggregate_by
from footmav.operations.normalize import per_90
from footmav.data_definitions.fbref import fbref_columns as fc
