from shapely.geometry import MultiPoint

from geonurse.tools.polygon import has_interior
from geonurse.tools.polygon import has_interior_duplicates
from geonurse.tools.polygon import _duplicated_interior_coordinates_list


def test_geom_with_interiors(
        test_data_polygon_interior_duplicates,
        expected_polygon_with_interior):

    input_geoseries = test_data_polygon_interior_duplicates

    has_interior_geoseries = input_geoseries.apply(has_interior)

    assert len(input_geoseries) == len(has_interior_geoseries)

    output_geometries = input_geoseries[has_interior_geoseries]

    assert len(output_geometries) == len(expected_polygon_with_interior)
    assert output_geometries.unary_union == expected_polygon_with_interior.unary_union


def test_geom_with_interiors_duplicates(
        expected_polygon_with_interior,
        expected_polygon_with_interior_duplicates):

    input_geoseries = expected_polygon_with_interior

    has_interior_duplicates_geoseries = input_geoseries.apply(has_interior_duplicates)

    assert len(input_geoseries) == len(has_interior_duplicates_geoseries)

    output_geometries = input_geoseries[has_interior_duplicates_geoseries]

    assert len(output_geometries) == len(expected_polygon_with_interior_duplicates)
    assert output_geometries.unary_union == expected_polygon_with_interior_duplicates.unary_union


def test_duplicated_interior_coordinates_list(
        expected_polygon_with_interior_duplicates,
        expected_polygon_interior_duplicates):

    input_geoseries = expected_polygon_with_interior_duplicates

    output_geoseries = (
        input_geoseries
            .apply(_duplicated_interior_coordinates_list)
            .apply(MultiPoint)
    )

    assert len(input_geoseries) == len(output_geoseries)
    assert output_geoseries.unary_union == expected_polygon_interior_duplicates.unary_union
