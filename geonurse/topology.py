# TODO create separate modules for specific types of geometries

from typing import Union

from geonurse.utils import _return_affected_geoms, set_precision

from collections import Counter
import numpy as np
from functools import reduce
from operator import add
from shapely.geometry import MultiPoint
from shapely.geometry import LineString, MultiLineString
from shapely.geometry import Polygon, MultiPolygon
import geopandas as gpd
from geopandas import GeoDataFrame, GeoSeries


"""POLYGON'S EXTERIOR DUPLICATES"""


def _exterior_duplicates_bool(geom: Union[Polygon, MultiPolygon]) -> bool:
    """
    Function used as an func argument in _return_affected_geoms.
    It returns True/False whether polygon's exterior has/hasn't
    duplicated vertices.

    >>> geoseries = _return_affected_geoms(geoseries, func=_exterior_duplicates_bool)
    """
    if isinstance(geom, MultiPolygon):
        return any(_exterior_duplicates_bool(polygon) for polygon in geom)
    else:
        all_vertices = len(list(geom.exterior.coords))
        unique_vertices = len(set(geom.exterior.coords))
        # polygon's first and last point is the same
        # so we expect at least one duplicate
        if unique_vertices + 1 != all_vertices:
            return True
        else:
            return False


def _return_duplicated_exterior_coords(geom: Union[Polygon, MultiPolygon]) -> list:
    """
    Function returns list of points duplicated on polygon's exterior
    """
    if isinstance(geom, MultiPolygon):
        error_coords = reduce(add,
                              [_return_duplicated_exterior_coords(polygon) for polygon in geom])
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


def exterior_duplicates(geoseries: GeoSeries) -> GeoSeries:
    """
    Function returns GeoSeries object with
    MultiPoint geometries for Polygons which have
    duplicated vertices on it's exterior.

    Parameters
    ----------
    geoseries : GeoSeries
        GeoSeries with (Multi)Polygon geometries

    Returns
    -------
    geoseries : GeoSeries
        GeoSeries with MultiPoint geometries and
        attributes resulting from exterior checking.

    >>> geoseries = exterior_duplicates(geoseries)
    """
    geoseries = _return_affected_geoms(geoseries, func=_exterior_duplicates_bool)
    if len(geoseries) > 0:
        return geoseries.apply(_return_duplicated_exterior_coords).apply(MultiPoint)


"""POLYGON'S INTERIOR DUPLICATES"""


def _geom_with_interiors(geom: Union[Polygon, MultiPolygon]) -> bool:
    """
    Function used as an func argument in _return_affected_geoms.
    It returns True/False whether polygon has/hasn't interior(s).

    >>> geoseries = _return_affected_geoms(geoseries, func=_geom_with_interiors)
    """
    if isinstance(geom, MultiPolygon):
        return any(_geom_with_interiors(polygon) for polygon in geom)
    else:
        if len(geom.interiors) > 0:
            return True
        else:
            return False


def _interior_duplicates_bool(geom: Union[Polygon, MultiPolygon]) -> bool:
    """
    Function used as an func argument in _return_affected_geoms.
    It returns True/False whether polygon has/hasn't duplicated interior vertices.

    >>> geoseries = _return_affected_geoms(geoseries, func=_interior_duplicates_bool)
    """
    if isinstance(geom, MultiPolygon):
        return any(_interior_duplicates_bool(polygon) for polygon in geom)
    else:
        interiors = geom.interiors
        dupl_interior_coords = [interior for interior in interiors
                                if len(interior.coords) != len(set(interior.coords)) + 1]
        if len(dupl_interior_coords) > 0:
            return True
        else:
            return False


def _return_duplicated_interior_coords(geom: Union[Polygon, MultiPolygon]) -> list:
    """
    Function returns list of points duplicated on polygon's interior
    """
    if isinstance(geom, MultiPolygon):
        error_coords = reduce(add,
                              [_return_duplicated_interior_coords(polygon) for polygon in geom])
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


def interior_duplicates(geoseries: GeoSeries) -> GeoSeries:
    """
    Function returns GeoSeries object with
    MultiPoint geometries for Polygons which have
    duplicated vertices on it's interior(s).

    Parameters
    ----------
    geoseries : GeoSeries
        GeoSeries with (Multi)Polygon geometries

    Returns
    -------
    geoseries : GeoSeries
        GeoSeries with MultiPoint geometries and
        attributes resulting from interior checking.

    >>> geoseries = interior_duplicates(geoseries)
    """
    geoseries = _return_affected_geoms(geoseries, func=_geom_with_interiors)
    geoseries = _return_affected_geoms(geoseries, func=_interior_duplicates_bool)
    if len(geoseries) > 0:
        return geoseries.apply(_return_duplicated_interior_coords).apply(MultiPoint)


"""LINESTRING DUPLICATES"""


def _linestring_duplicates_bool(geom: Union[LineString, MultiLineString]) -> bool:
    """
    Function used as an func argument in _return_affected_geoms.
    It returns True/False whether (multi)linestring
    has/hasn't duplicated vertices.

    >>> geoseries = _return_affected_geoms(geoseries, func=_linestring_duplicates_bool)
    """
    if isinstance(geom, MultiLineString):
        return any(_linestring_duplicates_bool(linestring) for linestring in geom)
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


def _return_duplicated_linestring_coords(geom: Union[LineString, MultiLineString]) -> list:
    """
    Function returns list of duplicated linestring points
    """
    if isinstance(geom, MultiLineString):
        error_coords = reduce(add,
                              [_return_duplicated_linestring_coords(linestring) for linestring in geom])
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


def linestring_duplicates(geoseries: GeoSeries) -> GeoSeries:
    """
    Function returns GeoSeries object with
    MultiPoint geometries for LineStrings
    which have duplicated vertices.

    Parameters
    ----------
    geoseries : GeoSeries
        GeoSeries with (Multi)LineString geometries

    Returns
    -------
    geoseries : GeoSeries
        GeoSeries with MultiPoint geometries and
        attributes resulting from linestring checking.

    >>> geoseries = linestring_duplicates(geoseries)
    """
    geoseries = _return_affected_geoms(geoseries, func=_linestring_duplicates_bool)
    if len(geoseries) > 0:
        return geoseries.apply(_return_duplicated_linestring_coords).apply(MultiPoint)


"""CONTINUITY"""


# TODO add function that look for gaps in polygons
def continuity():
    pass


# TODO check how geometry column has being named
def overlaps(geoseries: GeoSeries,
             precision: int = 7) -> GeoDataFrame:
    """
    Function returns GeoDataFrame object with
    self-intersected geometries from provided
    set of geometries.

    Parameters
    ----------
    geoseries : GeoSeries
        GeoSeries which could contain
        self-intersections.
    precision : int, default 7
        Number of decimal places to which
        the coordinates will be rounded.

    Returns
    -------
    gdf : GeoDataFrame

    >>> gdf = overlaps(geoseries, precision=7)
    """
    geoseries = set_precision(geoseries, precision=precision)

    gdf = GeoDataFrame(geometry=geoseries)
    gdf["OBJECTID"] = range(len(gdf))

    # perform sjoin
    out_gdf = gpd.sjoin(gdf, gdf)
    # remove rows duplicates
    out_gdf = out_gdf[out_gdf.OBJECTID_left != out_gdf.OBJECTID_right]
    # create pairs of self-intersected ids
    arr = out_gdf[["OBJECTID_left", "OBJECTID_right"]].values.T
    # stack i,j indexes and sort them
    sorted_arr = np.sort(np.dstack(arr)[0])
    i, j = np.unique(sorted_arr, axis=0).T

    # join two tables on indexes pairs
    gdf = (gdf.iloc[i].reset_index(drop=True).join(gdf.iloc[j].reset_index(drop=True), rsuffix='_2'))

    # TODO doesn't scale well
    def _self_intersections_geom(row):
        new_geom = row.geometry.intersection(row.geometry_2)
        if isinstance(new_geom, (Polygon, MultiPolygon)) and not new_geom.is_empty:
            return new_geom
        else:
            return np.NaN

    geoseries = gdf.apply(_self_intersections_geom, axis=1)
    geoseries = geoseries[~geoseries.isna()]

    gdf = GeoDataFrame(geometry=geoseries)
    gdf["OBJECTID"] = range(len(gdf))
    gdf.reset_index(drop=True, inplace=True)

    return gdf
