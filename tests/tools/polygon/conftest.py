import os
import pytest
import geopandas as gpd


EXTERIOR_DATA_FOLDER = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'exterior')
INTERIOR_DATA_FOLDER = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'interior')


"""--------------------"""
""" EXTERIOR TEST DATA """
"""--------------------"""


@pytest.fixture
def test_data_polygon_exterior_duplicates():
    path = os.path.join(
        EXTERIOR_DATA_FOLDER,
        'test_data_polygon_exterior_duplicates.geojson')
    return gpd.read_file(path).geometry


@pytest.fixture
def expected_polygon_with_exterior_duplicates():
    path = os.path.join(
        EXTERIOR_DATA_FOLDER,
        'expected_polygon_with_exterior_duplicates.geojson')
    return gpd.read_file(path).geometry


@pytest.fixture
def expected_polygon_exterior_duplicates():
    path = os.path.join(
        EXTERIOR_DATA_FOLDER,
        'expected_polygon_exterior_duplicates.geojson')
    return gpd.read_file(path).geometry


"""--------------------"""
""" INTERIOR TEST DATA """
"""--------------------"""


@pytest.fixture
def test_data_polygon_interior_duplicates():
    path = os.path.join(
        INTERIOR_DATA_FOLDER,
        'test_data_polygon_interior_duplicates.geojson')
    return gpd.read_file(path).geometry


@pytest.fixture
def expected_polygon_with_interior():
    path = os.path.join(
        INTERIOR_DATA_FOLDER,
        'expected_polygon_with_interior.geojson')
    return gpd.read_file(path).geometry


@pytest.fixture
def expected_polygon_with_interior_duplicates():
    path = os.path.join(
        INTERIOR_DATA_FOLDER,
        'expected_polygon_with_interior_duplicates.geojson')
    return gpd.read_file(path).geometry


@pytest.fixture
def expected_polygon_interior_duplicates():
    path = os.path.join(
        INTERIOR_DATA_FOLDER,
        'expected_polygon_interior_duplicates.geojson')
    return gpd.read_file(path).geometry
