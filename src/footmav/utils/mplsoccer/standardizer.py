from footmav.utils.mplsoccer import dimensions
import numpy as np


class Standardizer:
    """ Convert from one set of coordinates to another.
    Parameters
    ----------
    pitch_from, pitch_to : str, default 'statsbomb'
        The pitch to convert the coordinates from (pitch_from) and to (pitch_to).
        The supported pitch types are: 'opta', 'statsbomb', 'tracab',
        'wyscout', 'uefa', 'metricasports', 'custom', 'skillcorner' and 'secondspectrum'.
    length_from, length_to : float, default None
        The pitch length in meters. Only used for the 'tracab' and 'metricasports',
        'skillcorner', 'secondspectrum' and 'custom' pitch_type.
    width_from, width_to : float, default None
        The pitch width in meters. Only used for the 'tracab' and 'metricasports',
        'skillcorner', 'secondspectrum' and 'custom' pitch_type
    Examples
    --------
    >>> from mplsoccer import Standardizer
    >>> standard = Standardizer(pitch_from='statsbomb', pitch_to='custom', \
                                length_to=105, width_to=68)
    >>> x = [20, 30]
    >>> y = [50, 80]
    >>> x_std, y_std = standard.transform(x, y)
    """

    def __init__(
        self,
        pitch_from,
        pitch_to,
        length_from=None,
        width_from=None,
        length_to=None,
        width_to=None,
    ):

        if pitch_from not in dimensions.valid:
            raise TypeError(
                f"Invalid argument: pitch_from should be in {dimensions.valid}"
            )
        if (
            length_from is None or width_from is None
        ) and pitch_from in dimensions.size_varies:
            raise TypeError(
                "Invalid argument: width_from and length_from must be specified."
            )

        if pitch_to not in dimensions.valid:
            raise TypeError(
                f"Invalid argument: pitch_to should be in {dimensions.valid}"
            )
        if (
            length_to is None or width_to is None
        ) and pitch_to in dimensions.size_varies:
            raise TypeError(
                "Invalid argument: width_to and length_to must be specified."
            )

        self.pitch_from = pitch_from
        self.pitch_to = pitch_to
        self.length_from = length_from
        self.width_from = width_from
        self.length_to = length_to
        self.width_to = width_to

        self.dim_from = dimensions.create_pitch_dims(
            pitch_type=pitch_from, pitch_length=length_from, pitch_width=width_from
        )
        self.dim_to = dimensions.create_pitch_dims(
            pitch_type=pitch_to, pitch_length=length_to, pitch_width=width_to
        )

    def transform(self, x, y, reverse=False):
        """Transform the coordinates.
        Parameters
        ----------
        x, y : array-like or scalar.
            Commonly, these parameters are 1D arrays.
        reverse : bool, default False
            If reverse=True then reverse the transform. Therefore, the coordinates
            are converted from pitch_to to pitch_from.
        Returns
        ----------
        x_standardized, y_standardized : np.array 1d
            The coordinates standardized in pitch_to coordinates (or pitch_from if reverse=True).
        """
        # to numpy arrays
        x = np.asarray(x)
        y = np.asarray(y)

        if reverse:
            dim_from, dim_to = self.dim_to, self.dim_from
        else:
            dim_from, dim_to = self.dim_from, self.dim_to

        # clip outside to pitch extents
        x = x.clip(min=dim_from.left, max=dim_from.right)
        y = y.clip(min=dim_from.pitch_extent[2], max=dim_from.pitch_extent[3])

        # for inverted axis flip the coordinates
        if dim_from.invert_y:
            y = dim_from.bottom - y

        x_standardized = self._standardize(
            dim_from.x_markings_sorted, dim_to.x_markings_sorted, x
        )
        y_standardized = self._standardize(
            dim_from.y_markings_sorted, dim_to.y_markings_sorted, y
        )

        # for inverted axis flip the coordinates
        if dim_to.invert_y:
            y_standardized = dim_to.bottom - y_standardized

        return x_standardized, y_standardized

    @staticmethod
    def _standardize(markings_from, markings_to, coordinate):
        """ " Helper method to standardize the data"""
        # to deal with nans set nans to zero temporarily
        mask_nan = np.isnan(coordinate)
        coordinate[mask_nan] = 0
        pos = np.searchsorted(markings_from, coordinate)
        low_from = markings_from[pos - 1]
        high_from = markings_from[pos]
        proportion_of_way_between = (coordinate - low_from) / (high_from - low_from)
        low_to = markings_to[pos - 1]
        high_to = markings_to[pos]
        standardized_coordinate = low_to + (
            (high_to - low_to) * proportion_of_way_between
        )
        # then set nans back to nan
        standardized_coordinate[mask_nan] = np.nan
        return standardized_coordinate

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"pitch_from={self.pitch_from}, pitch_to={self.pitch_to}, "
            f"length_from={self.length_from}, width_from={self.width_from}, "
            f"length_to={self.length_to}, width_to={self.width_to})"
        )
