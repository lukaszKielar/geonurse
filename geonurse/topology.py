# TODO create separate modules for specific types of geometries

from typing import Union, List, Callable

import warnings
from collections import Counter
import numpy as np
from functools import reduce
from operator import add

from shapely.geometry import box, mapping, shape
from shapely.geometry import MultiPoint
from shapely.geometry import LineString, MultiLineString
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry import GeometryCollection

import geopandas as gpd
from geopandas import GeoDataFrame, GeoSeries


"""GENERAL"""


def _round_coords(geom,
                  precision: int = 7):
    """
    Function rounds coordinates for geometries.

    >>> geoseries = geoseries.apply(lambda geom: _round_coords(geom, precision))
    """
    def _new_coords(coords,
                    precision: int):
        new_coords = []
        try:
            return round(coords, int(precision))
        except TypeError:
            for coord in coords:
                new_coords.append(_new_coords(coord, precision))
        return new_coords
    geojson = mapping(geom)
    geojson['coordinates'] = _new_coords(geojson['coordinates'], precision)
    return shape(geojson)


def set_precision(geoseries: GeoSeries,
                  precision: int = 7) -> GeoSeries:
    """
    Function returns geoseries with
    geometries that has rounded coordinates

    Parameters
    ----------
    geoseries : GeoSeries
    precision : int, default 7
        Number of decimal places to which
        the coordinates will be rounded.

    Returns
    -------
    geoseries : GeoSeries
        GeoSeries with geometries
        resulting from rounding.

    >>> geoseries = set_precision(geoseries, precision=3)
    """
    geoseries = geoseries.apply(lambda geom: _round_coords(geom, precision))
    return geoseries


# TODO check if output geometry area is the same as before katana
def _katana(geometry: Union[Polygon, MultiPolygon],
            threshold: int = 100,
            count: int = 0) -> MultiPolygon:
    """
    Copyright (c) 2016, Joshua Arnott

    All rights reserved.

    Redistribution and use in source and binary forms, with or without modification,
    are permitted provided that the following conditions are met:

        1. Redistributions of source code must retain the above copyright notice, this list
            of conditions and the following disclaimer.
        2. Redistributions in binary form must reproduce the above copyright notice,
            this list of conditions and the following disclaimer in the documentation
            and/or other materials provided with the distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
    CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
    INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
    MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
    CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
    EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
    PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
    PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

    Taken from:
    https://snorfalorpagus.net/blog/2016/03/13/splitting-large-polygons-for-faster-intersections/

    Split a Polygon into two parts across it's shortest dimension

    >>> gdf.geometry = gdf.geometry.apply(_katana, args=(threshold, count))
    """
    bounds = geometry.bounds
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]
    if max(width, height) <= threshold and count == 0:
        warnings.warn("Polygon geometry hasn't been modified! Try to decrease threshold.",
                      stacklevel=2)
        # both the polygon is smaller than the threshold and count is set to 0
        return geometry
    elif max(width, height) <= threshold or count == 5:
        # either the polygon is smaller than the threshold, or the maximum
        # number of recursions has been reached
        return [geometry]
    if height >= width:
        # split left to right
        a = box(bounds[0], bounds[1], bounds[2], bounds[1] + height / 2)
        b = box(bounds[0], bounds[1] + height / 2, bounds[2], bounds[3])
    else:
        # split top to bottom
        a = box(bounds[0], bounds[1], bounds[0] + width / 2, bounds[3])
        b = box(bounds[0] + width / 2, bounds[1], bounds[2], bounds[3])
    result = []  # type: List[MultiPolygon]
    for d in (a, b,):
        c = geometry.intersection(d)
        if not isinstance(c, GeometryCollection):
            c = [c]
        for e in c:
            if isinstance(e, (Polygon, MultiPolygon)):
                result.extend(_katana(e, threshold, count + 1))
    if count > 0:
        return result
    # convert multipart into singlepart
    final_result = []  # type: List[MultiPolygon]
    for g in result:
        if isinstance(g, MultiPolygon):
            final_result.extend(g)
        else:
            final_result.append(g)
    return MultiPolygon(final_result)


def _layer_katana(gdf: GeoDataFrame,
                  threshold: int = 100,
                  explode: bool = False) -> GeoDataFrame:
    """
    Function allows to split individual Polygon geometries
    in GeoDataFrame across it's shorter dimension.

    Parameters
    ----------
    gdf : GeoDataFrame with MultiPolygon or Polygon geometry column
    threshold : int, default 100
        Parameter is used to define maximum size of longer polygon dimension.
    explode : bool, default False
        Parameter is used to define type of the output geometries.

    Returns
    -------
    gdf : GeoDataFrame
        GeoDataFrame with new set of geometries and attributes
        resulting from the conversion

    >>> new_gdf = _layer_katana(gdf, threshold=40, explode=False)
    """
    if not all(isinstance(geom, (Polygon, MultiPolygon)) for geom in gdf.geometry):
        raise NotImplementedError("All geometries have to be line objects")
    count = 0
    gdf_copy = gdf.copy(deep=True)
    gdf_copy.geometry = gdf_copy.geometry.apply(_katana, args=(threshold, count))
    if explode:
        return gdf_copy.explode()
    else:
        return gdf_copy


def _return_affected_geoms(geoseries: GeoSeries,
                           func: Callable) -> GeoSeries:
    """
    Function returns geometry features fetched by query func

    Parameters
    ----------
    geoseries : GeoSeries
    func : function
        Function used to query geometries.

    Returns
    -------
    geoseries : GeoSeries
        GeoSeries with geometries
        resulting from the query.

    >>> geoseries = _return_affected_geoms(geoseries, func=_geom_with_interiors)
    """
    return geoseries[geoseries.apply(func)]


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
