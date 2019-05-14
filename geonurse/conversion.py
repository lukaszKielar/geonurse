from typing import Union

from functools import reduce
from operator import add

from geopandas import GeoSeries

from shapely.geometry import LineString, MultiLineString
from shapely.geometry import MultiPoint


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

    GeoPySpark:
    >>> input_rdd = geopyspark.geotools.shapefile.get("path/to/shapefile.shp")
    >>> output_rdd = input_rdd.map(lambda x: _linestring_to_multipoint(getattr(x, 'geometry')))
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
