from typing import Any, Callable, List
from footmav.data_definitions.base import DataAttribute
from footmav.odm.data import Data
from functools import wraps
import inspect


def pipeable(
    origin_function: Callable[..., "Data"] = None,
    *,
    required_unique_keys: List[DataAttribute] = None,
) -> Callable[..., "Data"]:
    """
    Decorator to make a function pipeable to a Data object
    """

    def _inner_pipeable(func):
        class WrappedFunction:
            def __init__(self, f: Callable, req_keys: List[DataAttribute]):
                self.f = f
                self.req_keys = req_keys

            @wraps(func)
            def __call__(self, data: Data, *args: Any, **kwds: Any) -> Any:
                if self.req_keys is not None and data.unique_keys != self.req_keys:
                    raise ValueError(
                        f"The unique keys for the data object do not match the required unique keys.\n"
                        f"Required unique keys: {self.req_keys}\n"
                        f"Data unique keys: {data.unique_keys}"
                    )
                arg_list = list(args)
                for i, arg in enumerate(arg_list):
                    if isinstance(arg, Data):
                        arg_list[i] = arg.df
                for key, value in kwds.items():
                    if isinstance(value, Data):
                        kwds[key] = value.df
                f_args = inspect.getfullargspec(self.f).args
                if len(f_args) > 1 and f_args[1] == "full_data":
                    df = self.f(data.df, data.original_data, *arg_list, **kwds)
                else:
                    df = self.f(data.df, *arg_list, **kwds)
                if (
                    func.__name__ == "aggregate_by"
                ):  # Not pretty, but will have to do for now
                    if "aggregate_cols" in kwds:
                        return Data(df, data.original_data, kwds["aggregate_cols"])
                    else:
                        return Data(df, data.original_data, arg_list[0])
                else:
                    return Data(
                        df,
                        original_data=data.original_data,
                        unique_keys=data.unique_keys,
                    )

        return WrappedFunction(func, required_unique_keys)

    if origin_function:
        return _inner_pipeable(origin_function)
    return _inner_pipeable
