from typing import Union

from functools import reduce
from operator import add

from geopandas import GeoSeries

from shapely.geometry import LineString, MultiLineString
from shapely.geometry import MultiPoint


def _linestring_to_multipoint(geom: Union[LineString, MultiLineString]) -> MultiPoint:
    """
    Converts LineString/MultiLineString geometries into MultiPoints.
    Function used as an apply function on GeoSeries.

    >>> geoseries = geoseries.apply(_linestring_to_multipoint)
    """
    if isinstance(geom, MultiLineString):
        coords = reduce(add, [list(linestring.coords) for linestring in geom])
        return MultiPoint(coords)
    elif isinstance(geom, LineString):
        return MultiPoint(geom.coords)
    else:
        return MultiPoint()


def extract_nodes(geoseries: GeoSeries,
                  explode: bool = False) -> GeoSeries:
    """
    Perform (Multi)Linestring to (Multi)Point conversion.

    Currently only supports GeoSeries with polylines.
    By default returns multigeometries.

    Parameters
    ----------
    geoseries : GeoSeries
        GeoSeries with MultiLineString or LineString geometries
    explode : bool, default False
        Parameter is used to define type of the output geometries.

    Returns
    -------
    geoseries : GeoSeries
        GeoSeries with new set of geometries
        resulting from the conversion
    """
    if not all(isinstance(geom, (LineString, MultiLineString)) for geom in geoseries):
        raise NotImplementedError("All geometries have to be line objects")
    geoseries = geoseries.apply(_linestring_to_multipoint)
    if explode:
        return geoseries.explode()
    else:
        return geoseries
