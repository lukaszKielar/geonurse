from shapely.geometry import MultiPoint

from geonurse.tools.polygon import has_exterior_duplicates
from geonurse.tools.polygon import _duplicated_exterior_coordinates_list


def test_has_exterior_duplicates(
        test_data_polygon_exterior_duplicates,
        expected_polygon_with_exterior_duplicates):

    input_geoseries = test_data_polygon_exterior_duplicates

    has_exterior_duplicates_geoseries = input_geoseries.apply(has_exterior_duplicates)

    assert len(input_geoseries) == len(has_exterior_duplicates_geoseries)

    output_geometries = input_geoseries[has_exterior_duplicates_geoseries]

    assert len(output_geometries) == len(expected_polygon_with_exterior_duplicates)
    assert output_geometries.unary_union == expected_polygon_with_exterior_duplicates.unary_union


def test_duplicated_exterior_coordinates_list(
        expected_polygon_with_exterior_duplicates,
        expected_polygon_exterior_duplicates):

    input_geoseries = expected_polygon_with_exterior_duplicates

    output_geoseries = (
        input_geoseries
            .apply(_duplicated_exterior_coordinates_list)
            .apply(MultiPoint)
    )

    assert len(input_geoseries) == len(output_geoseries)
    assert output_geoseries.unary_union == expected_polygon_exterior_duplicates.unary_union
