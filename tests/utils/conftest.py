import os
import pytest
import geopandas as gpd


DATA_FOLDER = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data')


@pytest.fixture
def test_data_layer_precision():
    path = os.path.join(DATA_FOLDER, 'test_data_layer_precision.geojson')
    return gpd.read_file(path).geometry


@pytest.fixture
def expected_layer_precision():
    path = os.path.join(DATA_FOLDER, 'expected_layer_precision_3decimals.geojson')
    return gpd.read_file(path).geometry


@pytest.fixture
def naturalearth_admin_countries_50m():
    path = os.path.join(DATA_FOLDER, 'naturalearth_admin_countries_50m.geojson')
    return gpd.read_file(path).geometry


@pytest.fixture
def test_data_return_affected_geometries():
    path = os.path.join(DATA_FOLDER, 'test_data_return_affected_geometries.geojson')
    return gpd.read_file(path).geometry
