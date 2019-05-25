from typing import Union
from operator import add
from functools import reduce
from collections import Counter
from shapely.geometry import LineString, MultiLineString, MultiPoint

import geonurse


def has_duplicates(geom: Union[LineString, MultiLineString]) -> bool:
    """
    Function used to filter (Multi)LineString
    geometries that contain duplicated vertices.

    Parameters
    ----------
    geom : LineString or MultiLineString

    Returns
    -------
    bool

    Examples
    --------
    geonurse (pyspark under the hood):
    >>> import geonurse
    >>> from geonurse.tools.linestring import has_duplicates
    >>> geometries = geonurse.read_file(spark, filename).geometries('shapely')
    >>> lines_with_duplicates = geometries.map(has_duplicates)

    GeoPandas:
    >>> from geonurse.tools.linestring import has_duplicates
    >>> geoseries = geonurse.return_affected_geoms(geoseries, func=has_duplicates)
    """
    if isinstance(geom, MultiLineString):
        return any(has_duplicates(linestring) for linestring in geom)
    else:
        if geom.is_ring:
            # slice list to get rid of first point which has
            # the same coordinates as the last one
            all_vertices = geom.coords[1:]
        else:
            all_vertices = geom.coords
        unique_vertices = set(all_vertices)
        if len(all_vertices) != len(unique_vertices):
            return True
        else:
            return False


def _duplicated_coordinates_list(geom: Union[LineString, MultiLineString]) -> list:
    """
    Function returns a list of duplicated (Multi)LineString vertices.

    Parameters
    ----------
    geom : LineString or MultiLineString

    Returns
    -------
    list of Points

    Examples
    --------
    geonurse (pyspark under the hood):
    >>> import geonurse
    >>> from geonurse.tools.linestring import _duplicated_coordinates_list
    >>> geometries = geonurse.read_file(spark, filename).geometries('shapely')
    >>> duplicated_coords = geometries.map(_duplicated_coordinates_list).map(MultiPoint)

    GeoPandas:
    >>> from geonurse.tools.linestring import _duplicated_coordinates_list
    >>> geoseries = geoseries.apply(_duplicated_coordinates_list).apply(MultiPoint)
    """
    if isinstance(geom, MultiLineString):
        error_coords = reduce(
            add, [_duplicated_coordinates_list(linestring) for linestring in geom]
        )
        return error_coords
    else:
        if geom.is_ring:
            # slice list to get rid of first point which has
            # the same coordinates as the last one
            coords_list = list(geom.coords)[1:]
        else:
            coords_list = list(geom.coords)
        error_coords = [item for item, count in Counter(coords_list).items() if count > 1]
        if len(error_coords) > 0:
            return error_coords
        else:
            return []


def extract_vertices(geom: Union[LineString, MultiLineString]) -> MultiPoint:
    """
    Function extracts vertices as a MultiPoints
    from (Multi)LineString features.

    Parameters
    ----------
    geom : LineString or MultiLineString

    Returns
    -------
    MultiPoint

    Examples
    --------
    geonurse (pyspark under the hood):
    >>> import geonurse
    >>> from geonurse.tools.linestring import extract_vertices
    >>> geometries = geonurse.read_file(spark, filename).geometries('shapely')
    >>> line_vertecies = geometries.map(extract_vertices)

    GeoPandas:
    >>> geoseries = geoseries.apply(extract_vertices)
    """
    if isinstance(geom, MultiLineString):
        coords = reduce(add, [list(linestring.coords) for linestring in geom])
        return MultiPoint(coords)
    elif isinstance(geom, LineString):
        return MultiPoint(geom.coords)
    else:
        return MultiPoint()
