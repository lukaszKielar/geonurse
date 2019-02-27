from distutils.version import LooseVersion

import os
import pytest
from pandas.testing import assert_series_equal

import pandas as pd
from shapely.geometry import MultiPoint

from geopandas import read_file
from geopandas import GeoSeries
from geonurse.conversion import _linestring_to_multipoint
from geonurse.conversion import extract_nodes

DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'conversion')

SHP_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'conversion', 'shp')

if str(pd.__version__) < LooseVersion('0.23'):
    CONCAT_KWARGS = {}
else:
    CONCAT_KWARGS = {'sort': False}


@pytest.fixture
def input_linestring_gdf():
    gdf_path = os.path.join(SHP_DATA, 'naturalearth_admin_boundary_lines.shp')
    gdf = read_file(gdf_path)
    return gdf.geometry


@pytest.fixture
def qgis_extracted_points_gdf():
    gdf_path = os.path.join(SHP_DATA, 'naturalearth_admin_boundary_qgis_points.shp')
    gdf = read_file(gdf_path)
    return gdf.geometry


def test_extract_nodes(input_linestring_gdf, qgis_extracted_points_gdf):
    input_geoseries = input_linestring_gdf
    expected_qgis_geoseries = qgis_extracted_points_gdf
    extract_nodes_multi_geoseries = extract_nodes(input_geoseries, explode=False)
    assert len(input_geoseries) == len(extract_nodes_multi_geoseries) == 462
    extract_nodes_single_geoseries = (
        GeoSeries(extract_nodes(input_geoseries, explode=True)
                                .reset_index()
                                .drop(['level_0', 'level_1'], axis=1)
                                .geometry)
    )
    assert len(expected_qgis_geoseries) == len(extract_nodes_single_geoseries) == 77605
    assert_series_equal(extract_nodes_single_geoseries,
                        expected_qgis_geoseries,
                        check_index_type=False)
