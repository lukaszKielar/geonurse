import geopandas
from shapely.geometry import MultiPoint

from geonurse.tools.linestring import has_duplicates, _duplicated_coordinates_list, extract_vertices


def test_has_duplicates(test_data_linestring_duplicates, expected_linestring_with_duplicates):

    input_geoseries = test_data_linestring_duplicates

    has_duplicates_geoseries = input_geoseries.apply(has_duplicates)

    assert len(input_geoseries) == len(has_duplicates_geoseries)

    output_geometries = input_geoseries[has_duplicates_geoseries]

    assert len(output_geometries) == len(expected_linestring_with_duplicates)
    assert output_geometries.unary_union == expected_linestring_with_duplicates.unary_union


def test_duplicated_coordinates_list(expected_linestring_with_duplicates, expected_linestring_duplicates):

    input_geoseries = expected_linestring_with_duplicates

    output_geoseries = (
        input_geoseries
            .apply(_duplicated_coordinates_list)
            .apply(MultiPoint)
    )

    assert len(input_geoseries) == len(output_geoseries)
    assert output_geoseries.unary_union == expected_linestring_duplicates.unary_union


def test_extract_vertices(test_data_extract_vertices, expected_extract_vertices):

    input_geoseries = test_data_extract_vertices

    output_geoseries = input_geoseries.apply(extract_vertices)

    assert len(input_geoseries) == len(output_geoseries)

    # multipart to singlepart conversion
    output_geoseries = geopandas.GeoSeries(
        output_geoseries
            .explode()
            .reset_index()
            .drop(['level_0', 'level_1'], axis=1)
            .geometry
    )

    assert len(output_geoseries) == len(expected_extract_vertices)
    assert output_geoseries.unary_union == expected_extract_vertices.unary_union
