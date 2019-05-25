import os
import pytest
import geopandas as gpd


DATA_FOLDER = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data')


@pytest.fixture
def test_data_overlaps():
    path = os.path.join(DATA_FOLDER, 'test_data_overlaps.geojson')
    return gpd.read_file(path).geometry


@pytest.fixture
def expected_overlaps():
    path = os.path.join(DATA_FOLDER, 'expected_overlaps.geojson')
    return gpd.read_file(path).geometry
