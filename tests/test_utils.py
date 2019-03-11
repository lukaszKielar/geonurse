from distutils.version import LooseVersion
import pandas as pd
from pandas.testing import assert_series_equal

from geonurse.utils import set_precision
from geonurse.topology import _return_affected_geoms
from geonurse.topology import _exterior_duplicates_bool
from geonurse.topology import _geom_with_interiors


if str(pd.__version__) < LooseVersion('0.23'):
    CONCAT_KWARGS = {}
else:
    CONCAT_KWARGS = {'sort': False}


def test_layer_precision(test_data_layer_precision,
                         expected_layer_precision_3decimals):
    output_geoseries = set_precision(test_data_layer_precision, precision=3)
    assert_series_equal(output_geoseries,
                        expected_layer_precision_3decimals,
                        check_index_type=False)


def test_return_affected_geoms(test_data_polygon_interior_duplicates,
                               test_data_polygon_exterior_duplicates):
    _, int_geoseries = test_data_polygon_interior_duplicates
    raw_int_geoms = int_geoseries[int_geoseries.apply(_geom_with_interiors)]
    func_int_geoms = _return_affected_geoms(int_geoseries, func=_geom_with_interiors)
    assert_series_equal(raw_int_geoms, func_int_geoms, check_index_type=False)

    _, ext_geoseries = test_data_polygon_exterior_duplicates
    raw_ext_geoms = ext_geoseries[ext_geoseries.apply(_exterior_duplicates_bool)]
    func_ext_geoms = _return_affected_geoms(ext_geoseries, func=_exterior_duplicates_bool)
    assert_series_equal(raw_ext_geoms, func_ext_geoms, check_index_type=False)
