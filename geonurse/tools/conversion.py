from typing import Union
from operator import add
from functools import reduce
from geopandas import GeoSeries
from shapely.geometry import MultiPoint
from shapely.geometry import LineString, MultiLineString


# TODO raise NotImplementedError when not isinstance(LineString, MultiLineString)
def _linestring_to_multipoint(geom: Union[LineString, MultiLineString]) -> MultiPoint:
    """
    Converts LineString/MultiLineString geometries into MultiPoints.
    Function used as an apply function on GeoSeries.

    Parameters
    ----------
    geom : LineString or MultiLineString

    Returns
    -------
    MultiPoint

    Examples
    --------
    GeoPandas:
    >>> geoseries = geoseries.apply(_linestring_to_multipoint)

    Geonurse:
    >>> import geonurse
    >>> geoRdd = geonurse.read_file(spark, data)
    >>> geometry_rdd = geoRdd.geometries('shapely')
    >>> line_to_point_rdd = geometry_rdd.map(lambda x: geonurse.tools.conversion._linestring_to_multipoint(x))
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
    GeoSeries
        GeoSeries with new set of geometries
        resulting from the conversion

    Examples
    --------
    GeoPandas:
    >>> geoseries = extract_nodes(geoseries, explode=False)
    """
    if not all(isinstance(geom, (LineString, MultiLineString)) for geom in geoseries):
        raise NotImplementedError("All geometries have to be line objects")
    geoseries = geoseries.apply(_linestring_to_multipoint)
    if explode:
        return geoseries.explode()
    else:
        return geoseries
