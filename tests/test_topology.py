from distutils.version import LooseVersion

import os
import pytest
from pandas.testing import assert_series_equal

import pandas as pd
from shapely.geometry import MultiPoint

from geopandas import read_file
from shp_tools.topology import set_precision
from shp_tools.topology import _return_affected_geoms
from shp_tools.topology import _exterior_duplicates_bool
from shp_tools.topology import _return_duplicated_exterior_coords
from shp_tools.topology import exterior_duplicates
from shp_tools.topology import _geom_with_interiors
from shp_tools.topology import _interior_duplicates_bool
from shp_tools.topology import _return_duplicated_interior_coords
from shp_tools.topology import interior_duplicates
from shp_tools.topology import _linestring_duplicates_bool
from shp_tools.topology import _return_duplicated_linestring_coords
from shp_tools.topology import linestring_duplicates

DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'topology')
SHP_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'topology', 'shp')

if str(pd.__version__) < LooseVersion('0.23'):
    CONCAT_KWARGS = {}
else:
    CONCAT_KWARGS = {'sort': False}


"""TEST AND EXPECTED DATA"""


# TODO add more complex geometries
@pytest.fixture
def layer_precision_input():
    gdf_path = os.path.join(DATA, 'input_layer_precision.geojson')
    gdf = read_file(gdf_path)
    return gdf.geometry


@pytest.fixture
def expected_coord_output():
    gdf_path = os.path.join(DATA, 'output_layer_precision_3decimals.geojson')
    gdf = read_file(gdf_path)
    return gdf.geometry


@pytest.fixture
def polygon_exterior_df():
    gdf_path = os.path.join(DATA, 'polygon_exterior_test_data.geojson')
    gdf = read_file(gdf_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def polygon_interior_df():
    gdf_path = os.path.join(DATA, 'polygon_interior_test_data.geojson')
    gdf = read_file(gdf_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def linestring_test_df():
    gdf_path = os.path.join(DATA, 'linestring_duplicates_test_data.geojson')
    gdf = read_file(gdf_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def linestring_expected_df():
    gdf_path = os.path.join(DATA, 'linestring_duplicates_expected_data.geojson')
    gdf = read_file(gdf_path)
    return [gdf, gdf.geometry]


"""GENERAL"""


def test_layer_precision(layer_precision_input, expected_coord_output):
    input_geoseries = layer_precision_input
    output_geoseries = set_precision(input_geoseries, precision=3)
    assert_series_equal(output_geoseries, expected_coord_output, check_index_type=False)


def test_return_affected_geoms(polygon_interior_df, polygon_exterior_df):
    _, int_geoseries = polygon_interior_df
    raw_int_geoms = int_geoseries[int_geoseries.apply(_geom_with_interiors)]
    func_int_geoms = _return_affected_geoms(int_geoseries, func=_geom_with_interiors)
    assert_series_equal(raw_int_geoms, func_int_geoms, check_index_type=False)

    _, ext_geoseries = polygon_exterior_df
    raw_ext_geoms = ext_geoseries[ext_geoseries.apply(_exterior_duplicates_bool)]
    func_ext_geoms = _return_affected_geoms(ext_geoseries, func=_exterior_duplicates_bool)
    assert_series_equal(raw_ext_geoms, func_ext_geoms, check_index_type=False)


"""POLYGON'S EXTERIOR DUPLICATES"""


def test_exterior_duplicates_bool(polygon_exterior_df):
    _, geoseries = polygon_exterior_df
    assert len(geoseries) == len(geoseries.apply(_exterior_duplicates_bool))
    for i in range(0, len(geoseries)):
        if i in [0, 3, 6, 9, 12, 19]:
            assert _exterior_duplicates_bool(geoseries[i]) is False
        else:
            assert _exterior_duplicates_bool(geoseries[i]) is True


# TODO finish remaining assertions
def test_return_duplicated_exterior_coords(polygon_exterior_df):
    _, geoseries = polygon_exterior_df
    for i in range(0, len(geoseries)):
        if i in [0, 3, 6, 9, 12, 19]:
            assert _return_duplicated_exterior_coords(geoseries[i]) == []
    assert _return_duplicated_exterior_coords(geoseries[1]) == [(3.0, 0.0)]
    assert _return_duplicated_exterior_coords(geoseries[2]).sort() == [(3.0, 0.0), (3.0, 3.0)].sort()
    # assert _return_duplicated_exterior_coords(geoseries[3]) == [(7.0, 1.0)]
    # assert _return_duplicated_exterior_coords(geoseries[4]) == []
    # assert _return_duplicated_exterior_coords(geoseries[5]).sort() == [(2.0, 1.0), (7.0, 1.0), (7.0, 2.0)].sort()
    # assert _return_duplicated_exterior_coords(geoseries[6]) == []
    # assert _return_duplicated_exterior_coords(geoseries[7]).sort() == [(2.0, 1.0), (4.0, 1.0)].sort()


def test_exterior_duplicates(polygon_exterior_df):
    _, geoseries = polygon_exterior_df
    assert len(exterior_duplicates(geoseries)) == 20
    assert exterior_duplicates(geoseries)[1].equals(MultiPoint([(3.0, 0.0)])) is True
    assert exterior_duplicates(geoseries)[21].equals(MultiPoint([(8.0, 3.0), (8.0, 0.0)])) is True


"""POLYGON'S INTERIOR DUPLICATES"""


def test_geom_with_interiors(polygon_interior_df):
    _, geoseries = polygon_interior_df
    exploded_geoseries = geoseries.explode()
    assert len(geoseries) == len(geoseries.apply(_geom_with_interiors))
    assert len(geoseries[geoseries.apply(_geom_with_interiors)]) == 6
    assert len(exploded_geoseries[exploded_geoseries.apply(_geom_with_interiors)]) == 7


def test_interior_duplicates_bool(polygon_interior_df):
    _, geoseries = polygon_interior_df
    for i, geom in geoseries.iteritems():
        if i % 2 == 0:
            assert _interior_duplicates_bool(geom) is False
        else:
            assert _interior_duplicates_bool(geom) is True


def test_return_duplicated_interior_coords(polygon_interior_df):
    _, geoseries = polygon_interior_df
    new_geoseries = _return_affected_geoms(geoseries, func=_interior_duplicates_bool)
    assert len(new_geoseries) == 4
    for i, geom in geoseries.iteritems():
        if i % 2 == 0:
            assert _return_duplicated_interior_coords(geom) == []
    assert _return_duplicated_interior_coords(geoseries[1]) == [(2.0, 1.0)]
    assert _return_duplicated_interior_coords(geoseries[3]) == [(7.0, 1.0)]
    assert _return_duplicated_interior_coords(geoseries[5]).sort() == [(2.0, 1.0), (7.0, 1.0), (7.0, 2.0)].sort()
    assert _return_duplicated_interior_coords(geoseries[7]).sort() == [(2.0, 1.0), (4.0, 1.0), (4.0, 2.0)].sort()


def test_interior_duplicates(polygon_interior_df):
    _, geoseries = polygon_interior_df
    assert interior_duplicates(geoseries)[1].equals(MultiPoint([(2.0, 1.0)])) is True
    assert interior_duplicates(geoseries)[3].equals(MultiPoint([(7.0, 1.0)])) is True
    assert interior_duplicates(geoseries)[5].equals(MultiPoint([(2.0, 1.0), (7.0, 1.0), (7.0, 2.0)])) is True
    assert interior_duplicates(geoseries)[7].equals(MultiPoint([(2.0, 1.0), (4.0, 1.0), (4.0, 2.0)])) is True


"""LINESTRING DUPLICATES"""


def test_linestring_duplicates_bool(linestring_test_df):
    _, geoseries = linestring_test_df
    new_geoseries = _return_affected_geoms(geoseries, func=_linestring_duplicates_bool)
    assert len(new_geoseries) == 5
    for i, geom in geoseries.iteritems():
        if i in [0, 1, 4]:
            assert _linestring_duplicates_bool(geom) is False
        else:
            assert _linestring_duplicates_bool(geom) is True


def test_return_duplicated_linestring_coords(linestring_test_df, linestring_expected_df):
    _, test_geoseries = linestring_test_df
    _, expected_geoseries = linestring_expected_df
    assert len(test_geoseries) == len(expected_geoseries)
    for i, _ in test_geoseries.iteritems():
        if i in [0, 1, 4]:
            continue
        output_geom = _return_duplicated_linestring_coords(test_geoseries.geometry[i])
        expected_geom = expected_geoseries.geometry[i]
        assert MultiPoint(output_geom).equals(expected_geom)


def test_linestring_duplicates(linestring_test_df, linestring_expected_df):
    _, test_geoseries = linestring_test_df
    _, expected_geoseries = linestring_expected_df
    output_geoseries = linestring_duplicates(test_geoseries)
    assert len(output_geoseries) == 5
    # indexes stay the same so let's iterate over expected geoseries
    for i, _ in expected_geoseries.iteritems():
        if i in [0, 1, 4]:
            continue
        output_geom = output_geoseries.geometry[i]
        expected_geom = expected_geoseries.geometry[i]
        assert output_geom.equals(expected_geom)
