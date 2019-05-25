import os
import pytest
import geopandas as gpd


DATA_FOLDER = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data')


@pytest.fixture
def test_data_linestring_duplicates():
    path = os.path.join(DATA_FOLDER, 'test_data_linestring_duplicates.geojson')
    return gpd.read_file(path).geometry


@pytest.fixture
def expected_linestring_with_duplicates():
    path = os.path.join(DATA_FOLDER, 'expected_linestring_with_duplicates.geojson')
    return gpd.read_file(path).geometry


@pytest.fixture
def expected_linestring_duplicates():
    path = os.path.join(DATA_FOLDER, 'expected_linestring_duplicates.geojson')
    return gpd.read_file(path).geometry


@pytest.fixture
def test_data_extract_vertices():
    path = os.path.join(DATA_FOLDER, 'test_data_extract_vertices.geojson')
    return gpd.read_file(path).geometry


@pytest.fixture
def expected_extract_vertices():
    path = os.path.join(DATA_FOLDER, 'expected_extract_vertices.geojson')
    return gpd.read_file(path).geometry

