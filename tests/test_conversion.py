from distutils.version import LooseVersion
from pandas.testing import assert_series_equal
import pandas as pd
from geopandas import GeoSeries
import pytest
from shapely.geometry import Point, MultiPoint
from shapely.geometry import Polygon, MultiPolygon

from geonurse.tools.conversion import extract_nodes
from geonurse.tools.conversion import _linestring_to_multipoint


if str(pd.__version__) < LooseVersion('0.23'):
    CONCAT_KWARGS = {}
else:
    CONCAT_KWARGS = {'sort': False}


@pytest.mark.parametrize("test_input,expected", [
    (Point(0, 0), MultiPoint()),
    (MultiPoint([Point(0, 0), Point(1, 0)]), MultiPoint()),
    (Polygon([(0., 0.), (1., 1.), (2., 0.), (0., 0.)]), MultiPoint()),
    (MultiPolygon([
        Polygon([(0., 0.), (1., 1.), (2., 0.), (0., 0.)]),
        Polygon([(3., 0.), (4., 1.), (5., 0.), (3., 0.)])]), MultiPoint())])
def test_linestring_to_multipoint_points_assertions(test_input, expected):
    assert _linestring_to_multipoint(test_input) == expected


def test_extract_nodes(test_data_conversion_gdf, expected_conversion_gdf):
    input_geoseries = test_data_conversion_gdf
    expected_qgis_geoseries = expected_conversion_gdf
    extract_nodes_multi_geoseries = extract_nodes(input_geoseries, explode=False)
    assert len(input_geoseries) == len(extract_nodes_multi_geoseries) == 462
    extract_nodes_single_geoseries = (
        GeoSeries(
            extract_nodes(input_geoseries, explode=True)
                .reset_index()
                .drop(['level_0', 'level_1'], axis=1)
                .geometry
        )
    )
    assert (len(expected_qgis_geoseries)
            == len(extract_nodes_single_geoseries)
            == 77605)
    assert_series_equal(extract_nodes_single_geoseries,
                        expected_qgis_geoseries,
                        check_index_type=False)


def test_extract_nodes_assertions(expected_conversion_gdf, expected_overlaps_gdf):
    point_geoseries = expected_conversion_gdf
    _, polygon_geoseries = expected_overlaps_gdf
    # point geoseries
    with pytest.raises(NotImplementedError):
        extract_nodes(point_geoseries, explode=False)
    with pytest.raises(NotImplementedError):
        extract_nodes(point_geoseries, explode=True)
    # polygon geoseries
    with pytest.raises(NotImplementedError):
        extract_nodes(polygon_geoseries, explode=False)
    with pytest.raises(NotImplementedError):
        extract_nodes(polygon_geoseries, explode=True)
