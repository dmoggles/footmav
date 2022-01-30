from typing import Callable, Union
from footmav.data_definitions import data_sources
import pandas as pd


class DataAttribute:
    """
    Base class for a DataAttribute.  A DataAttribute is a representation of a particular data feature,
    normally implemented as a column in a DataFrame. The base class is meant to be descriptive, to provide
    a concrete object that can be identified through pylance and other developer and reflection tools.

    It also provides some additional information that can be used by calculation, such as the data type,
    the aggregation function, and the source of the data.

    Attributes:
        name (str): The name of the DataAttribute
        data_type (Union[str, type]): The data type of the DataAttribute
        agg_function (Callable): The aggregation function that describes how the data should be aggregated
        source (DataSource): The source of the data (e.g. FBRef, Understat, etc)
    """

    def __init__(
        self,
        name: str,
        data_type: Union[str, type],
        agg_function: Union[Callable, str],
        source: data_sources.DataSource,
    ):
        self._name = name
        self._data_type = data_type
        self._agg_function = agg_function
        self._source = source

    @property
    def N(self):
        """
        Return the name of the DataAttribute

        Returns:
            str: The name of the DataAttribute
        """
        return self._name

    @property
    def source(self):
        """
        Return the source of the DataAttribute

            Returns:
                DataSource: The source of the DataAttribute
        """

        return self._source

    @property
    def data_type(self):
        """
        Return the data type of the DataAttribute

        Returns:
            Union[str, type]: The data type of the DataAttribute
        """
        return self._data_type

    @property
    def agg_function(self):
        """
        Return the aggregation function of the DataAttribute

        Returns:
            Callable: The aggregation function of the DataAttribute
        """
        return self._agg_function


class NativeDataAttribute(DataAttribute):
    """
    Describes a DataAttribute that's natively loaded from one of the core data sources (ie, not derived data).

    Attributes:
        name (str): The name of the DataAttribute
        data_type (Union[str, type]): The data type of the DataAttribute
        source (DataSource): The source of the data (e.g. FBRef, Understat, etc)
        transform_function (Callable): A function that is applied to the data when it is loaded. (this is more applicable to ETL and is not generally used by the user)
        rename_to (str): The name that the data should be renamed to after it is loaded. (this is more applicable to ETL and is not generally used by the user)
        agg_function (Callable): The aggregation function that describes how the data should be aggregated.
    """

    def __init__(
        self,
        name: str,
        data_type: Union[str, type],
        source: data_sources.DataSource,
        transform_function: Callable = None,
        rename_to: str = None,
        agg_function: Union[Callable, str] = "sum",
    ):
        super().__init__(
            name=name, data_type=data_type, agg_function=agg_function, source=source
        )
        self._transform_function = transform_function
        self._rename_to = rename_to

    def user_transform(self, column: pd.Series) -> pd.Series:
        """User-defined data transform function.  This is typically applied during the ETL step of the process and isn't really used by the user.

        Args:
            column (pd.Series): Original Data column being loaded

        Returns:
            pd.Series: Transformed data column
        """
        if self._transform_function:
            return column.apply(self._transform_function)
        else:
            return column

    def pre_type_conversion_transform(self, column: pd.Series) -> pd.Series:
        """Transform function that is applied before the data is converted to the data type.

        Args:
            column (pd.Series): Original Data column being loaded

        Returns:
            pd.Series: Transformed data column
        """
        return column

    def post_type_conversion_transform(self, column: pd.Series) -> pd.Series:
        """Transform function that is applied after the data is converted to the data type.

        Args:
            column (pd.Series): Original Data column being loaded

        Returns:
            pd.Series: Transformed data column
        """
        return column

    def apply(self, column: pd.Series) -> pd.Series:
        """Applies the full transformation suite to the data.  This is generally done during the ETL step of the process and isn't really used by the user.
        The steps in this functon are:
        1. Apply user-defined transform function
        2. Apply pre-type-conversion transform function
        3. Convert data to specified data type
        4. Apply post-type-conversion transform function

        Args:
            column (pd.Series): Original Data column being loaded

        Returns:
            pd.Series: Transformed data column

        """
        column = self.user_transform(column)
        column = self.pre_type_conversion_transform(column)
        column = column.astype(self.data_type)
        column = self.post_type_conversion_transform(column)

        return column

    @property
    def N(self) -> str:
        """Return the name of the DataAttribute, or the rename_to if it is set.

        Returns:
            str: The name of the DataAttribute
        """
        if self._rename_to:
            return self._rename_to
        else:
            return super().N


class NumericDataAttribute(NativeDataAttribute):
    """
    Describes a natively-loaded numeric DataAttribute.  Numeric attributes have a default aggregation function of sum, which obviously doesn't work
    for percentage-type data.  This class provides a way to specify a different aggregation function.  However, it should be noted that
    percentage and ratio type data is best defined as a `DerivedDataAttribute`.

    Attributes:
        name (str): The name of the DataAttribute
        data_type (Union[str, type]): The data type of the DataAttribute
        source (DataSource): The source of the data (e.g. FBRef, Understat, etc)
        transform_function (Callable): A function that is applied to the data when it is loaded. (this is more applicable to ETL and is not generally used by the user)
        rename_to (str): The name that the data should be renamed to after it is loaded. (this is more applicable to ETL and is not generally used by the user)
        agg_function (Callable): The aggregation function that describes how the data should be aggregated.
    """

    def __init__(
        self,
        name: str,
        data_type: Union[str, type],
        source: data_sources.DataSource,
        transform_function: Callable = None,
        rename_to: str = None,
        agg_function: Union[Callable, str] = "sum",
    ):
        super().__init__(
            name=name,
            data_type=data_type,
            transform_function=transform_function,
            rename_to=rename_to,
            agg_function=agg_function,
            source=source,
        )

    def pre_type_conversion_transform(self, column: pd.Series) -> pd.Series:
        """Replace any blank strings with 0, prior to converting to a float type.  This is necessary for some data sources, such as Understat, where

        Args:
            column (pd.Series): Original Data column being loaded

        Returns:
            pd.Series: Transformed data column
        """
        return column.replace("", 0)

    def post_type_conversion_transform(self, column: pd.Series) -> pd.Series:
        """Fill any left over nan values with 0.

        Args:
            column (pd.Series): Data column after its been converted to float type

        Returns:
            pd.Series: Transformed data column
        """
        return column.fillna(0)


class FloatDataAttribute(NumericDataAttribute):
    """
    Describes a natively-loaded float DataAttribute.  Float attributes have a default aggregation function of sum, which obviously doesn't work
    for percentage-type data.  This class provides a way to specify a different aggregation function.  However, it should be noted that
    percentage and ratio type data is best defined as a `DerivedDataAttribute`.

    Attributes:
        name (str): The name of the DataAttribute
        source (DataSource): The source of the data (e.g. FBRef, Understat, etc)
        transform_function (Callable): A function that is applied to the data when it is loaded. (this is more applicable to ETL and is not generally used by the user)
        rename_to (str): The name that the data should be renamed to after it is loaded. (this is more applicable to ETL and is not generally used by the user)
        agg_function (Callable): The aggregation function that describes how the data should be aggregated.

    """

    def __init__(
        self,
        name: str,
        source: data_sources.DataSource,
        transform_function: Callable = None,
        rename_to: str = None,
        agg_function: Union[Callable, str] = "sum",
    ):
        super().__init__(
            name,
            "float",
            transform_function=transform_function,
            rename_to=rename_to,
            agg_function=agg_function,
            source=source,
        )


class IntDataAttribute(NumericDataAttribute):
    """
    Describes a natively-loaded int DataAttribute.  Int attributes have a default aggregation function of sum, which obviously doesn't work
    for percentage-type data.  This class provides a way to specify a different aggregation function.  However, it should be noted that
    percentage and ratio type data is best defined as a `DerivedDataAttribute`.

    Attributes:
        name (str): The name of the DataAttribute
        source (DataSource): The source of the data (e.g. FBRef, Understat, etc)
        transform_function (Callable): A function that is applied to the data when it is loaded. (this is more applicable to ETL and is not generally used by the user)
        rename_to (str): The name that the data should be renamed to after it is loaded. (this is more applicable to ETL and is not generally used by the user)
        agg_function (Callable): The aggregation function that describes how the data should be aggregated.

    """

    def __init__(
        self,
        name: str,
        source: data_sources.DataSource,
        transform_function: Callable = None,
        rename_to: str = None,
        agg_function: Union[Callable, str] = "sum",
    ):
        super().__init__(
            name,
            "int",
            transform_function=transform_function,
            rename_to=rename_to,
            agg_function=agg_function,
            source=source,
        )


class StrDataAttribute(NativeDataAttribute):
    """
    Describes a natively-loaded str DataAttribute.  String attributes have a default aggregation function of first.

    Attributes:
        name (str): The name of the DataAttribute
        source (DataSource): The source of the data (e.g. FBRef, Understat, etc)
        transform_function (Callable): A function that is applied to the data when it is loaded. (this is more applicable to ETL and is not generally used by the user)
        rename_to (str): The name that the data should be renamed to after it is loaded. (this is more applicable to ETL and is not generally used by the user)
        agg_function (Callable): The aggregation function that describes how the data should be aggregated.

    """

    def __init__(
        self,
        name: str,
        source: data_sources.DataSource,
        transform_function: Callable = None,
        rename_to: str = None,
        agg_function: Union[Callable, str] = "first",
    ):
        super().__init__(
            name,
            "str",
            transform_function=transform_function,
            rename_to=rename_to,
            agg_function=agg_function,
            source=source,
        )


class DateDataAttribute(NativeDataAttribute):
    """
    Describes a natively-loaded date DataAttribute.  Date attributes have a default aggregation function of first.

    Attributes:
        name (str): The name of the DataAttribute
        source (DataSource): The source of the data (e.g. FBRef, Understat, etc)
        transform_function (Callable): A function that is applied to the data when it is loaded. (this is more applicable to ETL and is not generally used by the user)
        rename_to (str): The name that the data should be renamed to after it is loaded. (this is more applicable to ETL and is not generally used by the user)
        agg_function (Callable): The aggregation function that describes how the data should be aggregated.

    """

    def __init__(
        self,
        name: str,
        source: data_sources.DataSource,
        transform_function: Callable = None,
        rename_to: str = None,
        agg_function: Union[Callable, str] = "first",
    ):
        super().__init__(
            name,
            pd.Timestamp,
            transform_function=transform_function,
            rename_to=rename_to,
            agg_function=agg_function,
            source=source,
        )
