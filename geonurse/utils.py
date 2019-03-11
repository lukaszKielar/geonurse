from typing import Union, List, Callable

import warnings
from geopandas import GeoSeries
from shapely.geometry import box, mapping, shape
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry import GeometryCollection


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

    >>> geoseries = geoseries.apply(_katana, args=(threshold, count))
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


def _layer_katana(geoseries: GeoSeries,
                  threshold: int = 100,
                  explode: bool = False) -> GeoSeries:
    """
    Function allows to split individual Polygon geometries
    in GeoDataFrame across it's shorter dimension.

    Parameters
    ----------
    geoseries : GeoSeries
        Series of (Multi)Polygon geometries
    threshold : int, default 100
        Parameter is used to define maximum
        size of longer polygon dimension.
    explode : bool, default False
        Parameter is used to define type of the output geometries.

    Returns
    -------
    geoseries : GeoSeries
        GeoSeries with new set of geometries
        resulting from the conversion

    >>> geoseries = _layer_katana(geoseries, threshold=100, explode=False)
    """
    if not all(isinstance(geom, (Polygon, MultiPolygon)) for geom in geoseries):
        raise NotImplementedError("All geometries have to be line objects")
    count = 0
    geoseries = geoseries.apply(_katana, args=(threshold, count))
    if explode:
        return geoseries.explode()
    else:
        return geoseries


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


def _zero_buffer(geoseries: GeoSeries) -> GeoSeries:
    """
    Function invokes buffer of zero distance
    on all geometries in geoseries. It helps
    to deal with topologically wrong objects.
    All loops in input geometries are flatten.

    Parameters
    ----------
    geoseries : GeoSeries
        GeoSeries with possible wrong geometries
        which may cause topological problems.

    Returns
    -------
    geoseries : GeoSeries

    >>> geoseries = _zero_buffer(geoseries)
    """
    geoseries = geoseries.apply(lambda x: x.buffer(0))
    return geoseries


def fix_geometry(geoseries: GeoSeries) -> GeoSeries:
    """
    Function invokes buffer of zero distance
    if geoseries has an invalid geometries.

    Parameters
    ----------
    geoseries : GeoSeries
        GeoSeries with possible wrong geometries
        which may cause topological problems.

    Returns
    -------
    geoseries : GeoSeries

    >>> geoseries = fix_geometry(geoseries)
    """
    if not all(geoseries.is_valid):
        return _zero_buffer(geoseries)
    else:
        return geoseries
