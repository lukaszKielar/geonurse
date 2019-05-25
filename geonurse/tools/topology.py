import numpy as np
import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame, GeoSeries
from shapely.geometry import Polygon, MultiPolygon

from geonurse.tools.linestring import has_duplicates
from geonurse.tools.polygon import has_exterior_duplicates, has_interior_duplicates
from geonurse.utils import return_affected_geoms, set_precision, _layer_katana, _round_coords


"""CONTINUITY"""


# TODO add function that look for gaps in polygons
def continuity():
    pass


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
    GeoDataFrame

    Examples
    --------
    GeoPandas:
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
    gdf.geometry = gdf.geometry.apply(lambda geom: _round_coords(geom, precision))

    return gdf


# TODO add error column to the output geodataframe
# TODO do multigeometries should be treated as invalid?
def check_polygon_topology(geoseries: GeoSeries,
                           split_layer: bool = False) -> GeoDataFrame:

    if split_layer:
        geoseries = _layer_katana(geoseries)

    duplicated_exterior_geoms = return_affected_geoms(
        geoseries, func=has_exterior_duplicates
    )

    duplicated_interior_geoms = return_affected_geoms(
        geoseries, func=has_interior_duplicates
    )

    invalid_geoms = geoseries[~geoseries.is_valid]

    # multi_geoms = ...

    crossed_geoms = geoseries[~geoseries.is_simple]

    error_gdf = GeoDataFrame(
        pd.concat(
            [duplicated_exterior_geoms, duplicated_interior_geoms, invalid_geoms, crossed_geoms]
        )
    )

    return error_gdf

    # TODO think what is the best way to return those geometries
    # if len(duplicated_exterior_geoms) > 0:
    #     duplicated_exterior_points = exterior_duplicates(duplicated_exterior_geoms)
    #
    # if len(duplicated_interior_geoms) > 0:
    #     duplicated_interior_points = interior_duplicates(duplicated_interior_geoms)


# TODO add error column to the output geodataframe
# TODO do multigeometries should be treated as invalid?
def check_polyline_topology(geoseries: GeoSeries) -> GeoDataFrame:

    duplicated_geoms = return_affected_geoms(
        geoseries, func=has_duplicates
    )

    invalid_geoms = geoseries[~geoseries.is_valid]

    # multi_geoms = ...

    crossed_geoms = geoseries[~geoseries.is_simple]

    error_gdf = gpd.GeoDataFrame(
        pd.concat(
            [duplicated_geoms, invalid_geoms, crossed_geoms]
        )
    )

    return error_gdf

    # TODO think what is the best way to return those geometries
    # if len(duplicated_geoms) > 0:
    #     duplicated_point_geoms = duplicated_coordinates(duplicated_geoms)


# TODO add error column to the output geodataframe
# TODO do multigeometries should be treated as invalid?
def check_point_topology(geoseries: GeoSeries) -> GeoDataFrame:

    # TODO add SCALABLE function which looks for duplicates
    # duplicated_geoms = return_affected_geoms(
    #     geoseries, func=_point_duplicates_bool
    # )

    duplicated_geoms = GeoSeries()

    invalid_geoms = geoseries[~geoseries.is_valid]

    # multi_geoms = ...

    error_gdf = GeoDataFrame(
        pd.concat(
            [duplicated_geoms, invalid_geoms]
        )
    )

    return error_gdf