from distutils.version import LooseVersion
import pandas as pd
from geopandas import GeoSeries
from shapely.geometry import Polygon, MultiPolygon
from pandas.testing import assert_series_equal
import pytest

from geonurse.tools.topology import _return_affected_geoms
from geonurse.tools.topology import _exterior_duplicates_bool
from geonurse.tools.topology import _geom_with_interiors
from geonurse.utils import set_precision
from geonurse.utils import _katana, _layer_katana
from geonurse.utils import _zero_buffer, fix_geometry


if str(pd.__version__) < LooseVersion('0.23'):
    CONCAT_KWARGS = {}
else:
    CONCAT_KWARGS = {'sort': False}


def test_set_precision(test_data_layer_precision_gdf,
                       expected_layer_precision_3decimals_gdf):
    output_geoseries = set_precision(test_data_layer_precision_gdf, precision=3)
    assert_series_equal(output_geoseries,
                        expected_layer_precision_3decimals_gdf,
                        check_index_type=False)


def test_katana(test_data_na_countries_gdf):
    _, test_geoseries = test_data_na_countries_gdf
    output_geoseries = test_geoseries.apply(_katana)
    assert abs(test_geoseries.area.sum()-output_geoseries.area.sum()) < 0.00001
    assert len(test_geoseries) == len(output_geoseries)
    assert isinstance(output_geoseries, GeoSeries)
    assert all([isinstance(geom, (Polygon, MultiPolygon)) for geom in output_geoseries])


def test_katana_warning(test_data_na_countries_gdf):
    test_gdf, _ = test_data_na_countries_gdf
    test_gdf = test_gdf[test_gdf.geometry.area < 100]
    test_geoseries = test_gdf.geometry
    with pytest.warns(UserWarning):
        output_geoseries = test_geoseries.apply(_katana)


def test_layer_katana(test_data_na_countries_gdf):
    _, test_geoseries = test_data_na_countries_gdf
    output_geoseries = _layer_katana(test_geoseries)
    assert abs(test_geoseries.area.sum()-output_geoseries.area.sum()) < 0.00001
    assert len(test_geoseries) == len(output_geoseries)
    assert isinstance(output_geoseries, GeoSeries)
    output_geoseries = _layer_katana(test_geoseries, explode=True)
    assert abs(test_geoseries.area.sum() - output_geoseries.area.sum()) < 0.00001
    assert len(test_geoseries) <= len(output_geoseries)
    assert isinstance(output_geoseries, GeoSeries)


def test_layer_katana_exception(test_data_linestring_duplicates_gdf):
    _, test_geoseries = test_data_linestring_duplicates_gdf
    with pytest.raises(NotImplementedError) as exec_info:
        output_geoseries = _layer_katana(test_geoseries)
    assert str(exec_info.value) == 'All geometries have to be line objects'


def test_return_affected_geoms(test_data_polygon_interior_duplicates_gdf,
                               test_data_polygon_exterior_duplicates_gdf):
    _, int_geoseries = test_data_polygon_interior_duplicates_gdf
    raw_int_geoms = int_geoseries[int_geoseries.apply(_geom_with_interiors)]
    func_int_geoms = _return_affected_geoms(int_geoseries, func=_geom_with_interiors)
    assert_series_equal(raw_int_geoms, func_int_geoms, check_index_type=False)

    _, ext_geoseries = test_data_polygon_exterior_duplicates_gdf
    raw_ext_geoms = ext_geoseries[ext_geoseries.apply(_exterior_duplicates_bool)]
    func_ext_geoms = _return_affected_geoms(ext_geoseries, func=_exterior_duplicates_bool)
    assert_series_equal(raw_ext_geoms, func_ext_geoms, check_index_type=False)


def test_zero_buffer(test_data_na_countries_gdf):
    _, test_geoseries = test_data_na_countries_gdf
    output_geoseries = _zero_buffer(test_geoseries)
    assert test_geoseries.area.sum() == output_geoseries.area.sum()
    assert len(test_geoseries) == len(output_geoseries)


def test_fix_geometry(test_data_na_countries_gdf):
    _, test_geoseries = test_data_na_countries_gdf
    output_geoseries = fix_geometry(test_geoseries)
    assert len(test_geoseries) == len(output_geoseries)
    assert all(output_geoseries.is_valid)
