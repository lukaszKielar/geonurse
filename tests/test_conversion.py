from distutils.version import LooseVersion
from pandas.testing import assert_series_equal
import pandas as pd
from geopandas import GeoSeries

from geonurse.conversion import extract_nodes
from geonurse.conversion import _linestring_to_multipoint


if str(pd.__version__) < LooseVersion('0.23'):
    CONCAT_KWARGS = {}
else:
    CONCAT_KWARGS = {'sort': False}


def test_extract_nodes(test_data_conversion, expected_conversion):
    input_geoseries = test_data_conversion
    expected_qgis_geoseries = expected_conversion
    extract_nodes_multi_geoseries = extract_nodes(input_geoseries, explode=False)
    assert len(input_geoseries) == len(extract_nodes_multi_geoseries) == 462
    extract_nodes_single_geoseries = (
        GeoSeries(
            extract_nodes(input_geoseries, explode=True)
                .reset_index()
                .drop(['level_0', 'level_1'], axis=1)
                .geometry
        )
    )
    assert (len(expected_qgis_geoseries)
            == len(extract_nodes_single_geoseries)
            == 77605)
    assert_series_equal(extract_nodes_single_geoseries,
                        expected_qgis_geoseries,
                        check_index_type=False)
