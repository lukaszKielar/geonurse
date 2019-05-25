from pandas.testing import assert_series_equal

from geonurse.utils import set_precision
from geonurse.tools.topology import overlaps


def test_overlaps(test_data_overlaps, expected_overlaps):

    input_geoseries = test_data_overlaps

    output_geoseries = overlaps(input_geoseries, precision=7).geometry

    assert_series_equal(output_geoseries, expected_overlaps, check_index_type=False)

    # check expected output with different rounding
    for i in range(3, 7):
        input_geom = set_precision(overlaps(input_geoseries).geometry, precision=i).unary_union
        expected_geom = set_precision(expected_overlaps, precision=i).unary_union

        assert input_geom.equals(expected_geom)
