from distutils.version import LooseVersion
import os
import pandas as pd
from shapely.geometry import MultiPoint

from geonurse.utils import set_precision
from geonurse.topology import _return_affected_geoms
from geonurse.topology import _exterior_duplicates_bool
from geonurse.topology import _return_duplicated_exterior_coords
from geonurse.topology import exterior_duplicates
from geonurse.topology import _geom_with_interiors
from geonurse.topology import _interior_duplicates_bool
from geonurse.topology import _return_duplicated_interior_coords
from geonurse.topology import interior_duplicates
from geonurse.topology import _linestring_duplicates_bool
from geonurse.topology import _return_duplicated_linestring_coords
from geonurse.topology import linestring_duplicates
from geonurse.topology import overlaps

DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'topology')
SHP_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'topology', 'shp')

if str(pd.__version__) < LooseVersion('0.23'):
    CONCAT_KWARGS = {}
else:
    CONCAT_KWARGS = {'sort': False}


"""POLYGON'S EXTERIOR DUPLICATES"""


def test_exterior_duplicates_bool(test_data_polygon_exterior_duplicates):
    _, geoseries = test_data_polygon_exterior_duplicates
    assert len(geoseries) == len(geoseries.apply(_exterior_duplicates_bool))
    for i in range(0, len(geoseries)):
        if i in [0, 3, 6, 9, 12, 19]:
            assert _exterior_duplicates_bool(geoseries[i]) is False
        else:
            assert _exterior_duplicates_bool(geoseries[i]) is True


# TODO finish remaining assertions
def test_return_duplicated_exterior_coords(test_data_polygon_exterior_duplicates):
    _, geoseries = test_data_polygon_exterior_duplicates
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


def test_exterior_duplicates(test_data_polygon_exterior_duplicates):
    _, geoseries = test_data_polygon_exterior_duplicates
    assert len(exterior_duplicates(geoseries)) == 20
    assert exterior_duplicates(geoseries)[1].equals(MultiPoint([(3.0, 0.0)])) is True
    assert exterior_duplicates(geoseries)[21].equals(MultiPoint([(8.0, 3.0), (8.0, 0.0)])) is True


"""POLYGON'S INTERIOR DUPLICATES"""


def test_geom_with_interiors(test_data_polygon_interior_duplicates):
    _, geoseries = test_data_polygon_interior_duplicates
    exploded_geoseries = geoseries.explode()
    assert len(geoseries) == len(geoseries.apply(_geom_with_interiors))
    assert len(geoseries[geoseries.apply(_geom_with_interiors)]) == 6
    assert len(exploded_geoseries[exploded_geoseries.apply(_geom_with_interiors)]) == 7


def test_interior_duplicates_bool(test_data_polygon_interior_duplicates):
    _, geoseries = test_data_polygon_interior_duplicates
    for i, geom in geoseries.iteritems():
        if i % 2 == 0:
            assert _interior_duplicates_bool(geom) is False
        else:
            assert _interior_duplicates_bool(geom) is True


def test_return_duplicated_interior_coords(test_data_polygon_interior_duplicates):
    _, geoseries = test_data_polygon_interior_duplicates
    new_geoseries = _return_affected_geoms(geoseries,
                                           func=_interior_duplicates_bool)
    assert len(new_geoseries) == 4
    for i, geom in geoseries.iteritems():
        if i % 2 == 0:
            assert _return_duplicated_interior_coords(geom) == []
    assert _return_duplicated_interior_coords(geoseries[1]) == [(2.0, 1.0)]
    assert _return_duplicated_interior_coords(geoseries[3]) == [(7.0, 1.0)]
    assert _return_duplicated_interior_coords(geoseries[5]).sort() == [(2.0, 1.0), (7.0, 1.0), (7.0, 2.0)].sort()
    assert _return_duplicated_interior_coords(geoseries[7]).sort() == [(2.0, 1.0), (4.0, 1.0), (4.0, 2.0)].sort()


def test_interior_duplicates(test_data_polygon_interior_duplicates):
    _, geoseries = test_data_polygon_interior_duplicates
    assert interior_duplicates(geoseries)[1].equals(MultiPoint([(2.0, 1.0)])) is True
    assert interior_duplicates(geoseries)[3].equals(MultiPoint([(7.0, 1.0)])) is True
    assert interior_duplicates(geoseries)[5].equals(MultiPoint([(2.0, 1.0), (7.0, 1.0), (7.0, 2.0)])) is True
    assert interior_duplicates(geoseries)[7].equals(MultiPoint([(2.0, 1.0), (4.0, 1.0), (4.0, 2.0)])) is True


"""LINESTRING DUPLICATES"""


def test_linestring_duplicates_bool(test_data_linestring_duplicates):
    _, geoseries = test_data_linestring_duplicates
    new_geoseries = _return_affected_geoms(geoseries,
                                           func=_linestring_duplicates_bool)
    assert len(new_geoseries) == 5
    for i, geom in geoseries.iteritems():
        if i in [0, 1, 4]:
            assert _linestring_duplicates_bool(geom) is False
        else:
            assert _linestring_duplicates_bool(geom) is True


def test_return_duplicated_linestring_coords(test_data_linestring_duplicates,
                                             expected_linestring_duplicates):
    _, test_geoseries = test_data_linestring_duplicates
    _, expected_geoseries = expected_linestring_duplicates
    assert len(test_geoseries) == len(expected_geoseries)
    for i, _ in test_geoseries.iteritems():
        if i in [0, 1, 4]:
            continue
        output_geom = _return_duplicated_linestring_coords(test_geoseries.geometry[i])
        expected_geom = expected_geoseries.geometry[i]
        assert MultiPoint(output_geom).equals(expected_geom)


def test_linestring_duplicates(test_data_linestring_duplicates,
                               expected_linestring_duplicates):
    _, test_geoseries = test_data_linestring_duplicates
    _, expected_geoseries = expected_linestring_duplicates
    output_geoseries = linestring_duplicates(test_geoseries)
    assert len(output_geoseries) == 5
    # indexes stay the same so let's iterate over expected geoseries
    for i, _ in expected_geoseries.iteritems():
        if i in [0, 1, 4]:
            continue
        output_geom = output_geoseries.geometry[i]
        expected_geom = expected_geoseries.geometry[i]
        assert output_geom.equals(expected_geom)


""" OVERLAPS """


def test_overlaps(test_data_overlaps,
                  expected_overlaps):
    test_gdf, _ = test_data_overlaps
    expected_gdf, _ = expected_overlaps
    # check expected output with different rounding
    for i in range(3, 7):
        temp_test_geom = set_precision(overlaps(test_gdf.geometry).geometry,
                                       precision=i).unary_union
        temp_expected_geom = set_precision(expected_gdf.geometry,
                                           precision=i).unary_union
        temp_test_geom.equals(temp_expected_geom)
