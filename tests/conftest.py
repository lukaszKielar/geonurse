import os
import pytest
from geopandas import read_file

TOPOLOGY_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'topology')
TOPOLOGY_SHP_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'topology', 'shp')
CONVERSION_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'conversion')
CONVERSION_SHP_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'conversion', 'shp')
UTLIS_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'utils')
# UTILS_SHP_DATA = os.path.join(
#     os.path.abspath(os.path.dirname(__file__)), 'data', 'utils', 'shp')


@pytest.fixture
def test_data_conversion():
    gdf_path = os.path.join(CONVERSION_SHP_DATA,
                            'naturalearth_admin_boundary_lines.shp')
    gdf = read_file(gdf_path)
    return gdf.geometry


@pytest.fixture
def expected_conversion():
    gdf_path = os.path.join(CONVERSION_SHP_DATA,
                            'naturalearth_admin_boundary_qgis_points.shp')
    gdf = read_file(gdf_path)
    return gdf.geometry


# TODO add more complex geometries
@pytest.fixture
def test_data_layer_precision():
    gdf_path = os.path.join(UTLIS_DATA,
                            'test_data_layer_precision.geojson')
    gdf = read_file(gdf_path)
    return gdf.geometry


@pytest.fixture
def expected_layer_precision_3decimals():
    gdf_path = os.path.join(UTLIS_DATA,
                            'expected_layer_precision_3decimals.geojson')
    gdf = read_file(gdf_path)
    return gdf.geometry


@pytest.fixture
def test_data_polygon_exterior_duplicates():
    gdf_path = os.path.join(TOPOLOGY_DATA,
                            'test_data_polygon_exterior_duplicates.geojson')
    gdf = read_file(gdf_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def test_data_polygon_interior_duplicates():
    gdf_path = os.path.join(TOPOLOGY_DATA,
                            'test_data_polygon_interior_duplicates.geojson')
    gdf = read_file(gdf_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def test_data_linestring_duplicates():
    gdf_path = os.path.join(TOPOLOGY_DATA,
                            'test_data_linestring_duplicates.geojson')
    gdf = read_file(gdf_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def expected_linestring_duplicates():
    gdf_path = os.path.join(TOPOLOGY_DATA,
                            'expected_linestring_duplicates.geojson')
    gdf = read_file(gdf_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def test_data_overlaps():
    gdf_path = os.path.join(TOPOLOGY_SHP_DATA,
                            'test_data_overlaps.shp')
    gdf = read_file(gdf_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def expected_overlaps():
    gdf_path = os.path.join(TOPOLOGY_SHP_DATA,
                            'expected_overlaps.shp')
    gdf = read_file(gdf_path)
    return [gdf, gdf.geometry]
