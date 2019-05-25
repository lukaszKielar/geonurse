from typing import Union
from operator import add
from functools import reduce
from collections import Counter
from shapely.geometry import Polygon, MultiPolygon, MultiPoint

from geonurse.utils import return_affected_geoms


def has_exterior_duplicates(geom: Union[Polygon, MultiPolygon]) -> bool:
    """
    Function used to filter (Multi)Polygon geometries
    that contain duplicated vertecies on its exterior.

    Parameters
    ----------
    geom : Polygon or MultiPolygon

    Returns
    -------
    GeoSeries
        GeoSeries with MultiPoint geometries and
        attributes resulting from exterior checking.

    Examples
    --------
    geonurse (pyspark under the hood):
    >>> import geonurse
    >>> from geonurse.tools.polygon import has_exterior_duplicates
    >>> geometries = geonurse.read_file(spark, filename).geometries('shapely')
    >>> exterior_with_duplicates = geometries.map(has_exterior_duplicates)

    GeoPandas:
    >>> from geonurse.tools.polygon import has_exterior_duplicates
    >>> geoseries = return_affected_geoms(geoseries, func=has_exterior_duplicates)
    """
    if isinstance(geom, MultiPolygon):
        return any(has_exterior_duplicates(polygon) for polygon in geom)
    else:
        all_vertices = len(list(geom.exterior.coords))
        unique_vertices = len(set(geom.exterior.coords))
        # polygon's first and last point is the same
        # so we expect at least one duplicate
        if unique_vertices + 1 != all_vertices:
            return True
        else:
            return False


def _duplicated_exterior_coordinates_list(geom: Union[Polygon, MultiPolygon]) -> list:
    """
    Function returns a list of duplicated vertices
    on (Multi)Polygon exterior(s).

    Parameters
    ----------
    geom : Polygon or MultiPolygon

    Returns
    -------
    list of Points

    Examples
    --------
    geonurse (pyspark under the hood):
    >>> import geonurse
    >>> from geonurse.tools.polygon import _duplicated_exterior_coordinates_list
    >>> geometries = geonurse.read_file(spark, filename).geometries('shapely')
    >>> duplicated_exterior_coords = geometries.map(_duplicated_exterior_coordinates_list).map(MultiPoint)

    GeoPandas:
    >>> from geonurse.tools.polygon import _duplicated_exterior_coordinates_list
    >>> geoseries = geoseries.apply(_duplicated_exterior_coordinates_list).apply(MultiPoint)
    """
    if isinstance(geom, MultiPolygon):
        error_coords = reduce(
            add, [_duplicated_exterior_coordinates_list(polygon) for polygon in geom]
        )
        return error_coords
    else:
        # slice list to get rid of first point which has
        # the same coordinates as the last one
        coords = list(geom.exterior.coords)[1:]
        error_coords = [item for item, count in Counter(coords).items() if count > 1]
        if len(error_coords) > 0:
            return error_coords
        else:
            return []


def has_interior(geom: Union[Polygon, MultiPolygon]) -> bool:
    """
    Function used to filter (Multi)Polygon
    geometries that have interior.

    Parameters
    ----------
    geom : Polygon or MultiPolygon

    Returns
    -------
    bool

    Examples
    --------
    geonurse (pyspark under the hood):
    >>> import geonurse
    >>> from geonurse.tools.polygon import has_interior
    >>> geometries = geonurse.read_file(spark, filename).geometries('shapely')
    >>> polygons_with_interior = geometries.map(has_interior)

    GeoPandas:
    >>> from geonurse.tools.polygon import has_interior
    >>> geoseries = geonurse.return_affected_geoms(geoseries, func=has_interior)
    """
    if isinstance(geom, MultiPolygon):
        return any(has_interior(polygon) for polygon in geom)
    else:
        if len(geom.interiors) > 0:
            return True
        else:
            return False


def has_interior_duplicates(geom: Union[Polygon, MultiPolygon]) -> bool:
    """
    Function used to filter (Multi)Polygon geometries
    that contain duplicated vertecies on its interior.

    Parameters
    ----------
    geom : Polygon or MultiPolygon

    Returns
    -------
    bool

    Examples
    --------
    geonurse (pyspark under the hood):
    >>> import geonurse
    >>> from geonurse.tools.polygon import has_interior_duplicates
    >>> geometries = geonurse.read_file(spark, filename).geometries('shapely')
    >>> interior_with_duplicates = geometries.map(has_interior_duplicates)

    GeoPandas:
    >>> from geonurse.tools.polygon import has_interior_duplicates
    >>> geoseries = return_affected_geoms(geoseries, func=has_interior_duplicates)
    """
    if isinstance(geom, MultiPolygon):
        return any(has_interior_duplicates(polygon) for polygon in geom)
    else:
        interiors = geom.interiors
        dupl_interior_coords = [
            interior for interior in interiors
            if len(interior.coords) != len(set(interior.coords)) + 1
        ]
        if len(dupl_interior_coords) > 0:
            return True
        else:
            return False


def _duplicated_interior_coordinates_list(geom: Union[Polygon, MultiPolygon]) -> list:
    """
    Function returns a list of duplicated
    vertices on (Multi)Polygon interior.

    Parameters
    ----------
    geom : Polygon or MultiPolygon

    Returns
    -------
    list of Points

    Examples
    --------
    geonurse (pyspark under the hood):
    >>> import geonurse
    >>> from geonurse.tools.polygon import _duplicated_interior_coordinates_list
    >>> geometries = geonurse.read_file(spark, filename).geometries('shapely')
    >>> duplicated_interior_coords = geometries.map(_duplicated_interior_coordinates_list).map(MultiPoint)

    GeoPandas:
    >>> from geonurse.tools.polygon import _duplicated_interior_coordinates_list
    >>> geoseries = geoseries.apply(_duplicated_interior_coordinates_list).apply(MultiPoint)
    """
    if isinstance(geom, MultiPolygon):
        error_coords = reduce(add,
                              [_duplicated_interior_coordinates_list(polygon) for polygon in geom])
        return error_coords
    else:
        def _get_coords(interior):
            # slice list to get rid of first point which has
            # the same coordinates as the last one
            coords_list = list(interior.coords)[1:]
            dupl_coords = [item for item, count in Counter(coords_list).items() if count > 1]
            return dupl_coords

        interiors = geom.interiors
        bad_interiors = [interior for interior in interiors if
                         len(interior.coords) != len(set(interior.coords)) + 1]

        duplicated_vertices = []

        for interior in bad_interiors:
            dupl_coords = _get_coords(interior)
            duplicated_vertices.extend(dupl_coords)

        return duplicated_vertices
