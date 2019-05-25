import pytest
import geopandas
from shapely.geometry import Polygon, MultiPolygon

from geonurse.tools.polygon import has_interior

from geonurse.utils import set_precision
from geonurse.utils import _katana
from geonurse.utils import _layer_katana
from geonurse.utils import return_affected_geoms
from geonurse.utils import _zero_buffer
from geonurse.utils import fix_geometry


def test_set_precision(test_data_layer_precision, expected_layer_precision):

    input_geoseries = test_data_layer_precision
    output_geoseries = set_precision(input_geoseries, precision=3)

    assert len(input_geoseries) == len(output_geoseries)
    assert output_geoseries.unary_union == expected_layer_precision.unary_union


def test_katana(naturalearth_admin_countries_50m):

    input_geoseries = naturalearth_admin_countries_50m
    output_geoseries = input_geoseries.apply(_katana)

    assert abs(input_geoseries.area.sum()-output_geoseries.area.sum()) < 0.00001
    assert len(input_geoseries) == len(output_geoseries)
    assert isinstance(output_geoseries, geopandas.GeoSeries)
    assert all([isinstance(geom, (Polygon, MultiPolygon)) for geom in output_geoseries])


def test_katana_warning(naturalearth_admin_countries_50m):

    input_geoseries = naturalearth_admin_countries_50m[naturalearth_admin_countries_50m.area < 100]

    with pytest.warns(UserWarning):
        input_geoseries.apply(_katana)


def test_layer_katana(naturalearth_admin_countries_50m):

    input_geoseries = naturalearth_admin_countries_50m
    output_geoseries = _layer_katana(input_geoseries)

    assert abs(input_geoseries.area.sum()-output_geoseries.area.sum()) < 0.00001
    assert len(input_geoseries) == len(output_geoseries)
    assert isinstance(output_geoseries, geopandas.GeoSeries)

    output_geoseries = _layer_katana(input_geoseries, explode=True)

    assert abs(input_geoseries.area.sum() - output_geoseries.area.sum()) < 0.00001
    assert len(input_geoseries) <= len(output_geoseries)
    assert isinstance(output_geoseries, geopandas.GeoSeries)


def test_layer_katana_exception(test_data_layer_precision):

    input_geoseries = test_data_layer_precision

    with pytest.raises(NotImplementedError) as exec_info:
        _layer_katana(input_geoseries)

        assert str(exec_info.value) == 'All geometries have to be polygon or multipolygon objects'


def test_return_affected_geoms(test_data_return_affected_geometries):

    input_geoseries = test_data_return_affected_geometries
    raw_output_geoseries = input_geoseries[input_geoseries.apply(has_interior)]
    output_geoseries = return_affected_geoms(input_geoseries, func=has_interior)

    assert len(raw_output_geoseries) == len(output_geoseries)
    assert raw_output_geoseries.unary_union == output_geoseries.unary_union


def test_zero_buffer(naturalearth_admin_countries_50m):

    input_geoseries = naturalearth_admin_countries_50m
    output_geoseries = _zero_buffer(input_geoseries)

    assert len(input_geoseries) == len(output_geoseries)
    assert input_geoseries.area.sum() == output_geoseries.area.sum()


def test_fix_geometry(naturalearth_admin_countries_50m):

    input_geoseries = naturalearth_admin_countries_50m
    output_geoseries = fix_geometry(input_geoseries)

    assert len(input_geoseries) == len(output_geoseries)
    assert all(output_geoseries.is_valid)
