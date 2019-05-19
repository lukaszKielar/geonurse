# TODO change tests for the oldest features
# TODO split tests into separate folders
import os
import pytest
import geopandas as gpd
import pyspark
import fiona

TOPOLOGY_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'topology')
TOPOLOGY_SHP_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'topology', 'shp')
CONVERSION_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'conversion')
CONVERSION_SHP_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'conversion', 'shp')
UTILS_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'utils')
UTILS_SHP_DATA = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'data', 'utils', 'shp')


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        pyspark.sql.SparkSession
            .builder
            .master('local[*]')
            .appName('geonurse unit tests')
            .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def spark_context():
    spark = (
        pyspark.sql.SparkSession
            .builder
            .master('local[*]')
            .appName('geonurse unit tests')
            .getOrCreate()
    )
    yield spark.sparkContext
    spark.stop()


@pytest.fixture
def test_data_conversion_path():
    shp_path = os.path.join(
        CONVERSION_SHP_DATA,
        'naturalearth_admin_boundary_lines.shp'
    )
    return shp_path


@pytest.fixture
def test_data_conversion_gdf(test_data_conversion_path):
    gdf = gpd.read_file(test_data_conversion_path)
    return gdf.geometry


@pytest.fixture
def test_data_conversion_fiona(test_data_conversion_path):
    shp = fiona.open(test_data_conversion_path)
    yield shp
    shp.close()


@pytest.fixture
def expected_conversion_path():
    shp_path = os.path.join(
        CONVERSION_SHP_DATA,
        'naturalearth_admin_boundary_qgis_points.shp'
    )
    return shp_path


@pytest.fixture
def expected_conversion_gdf(expected_conversion_path):
    gdf = gpd.read_file(expected_conversion_path)
    return gdf.geometry


@pytest.fixture
def test_data_layer_precision_path():
    shp_path = os.path.join(
        UTILS_DATA,
        'test_data_layer_precision.geojson'
    )
    return shp_path


# TODO add more complex geometries
@pytest.fixture
def test_data_layer_precision_gdf(test_data_layer_precision_path):
    gdf = gpd.read_file(test_data_layer_precision_path)
    return gdf.geometry


@pytest.fixture
def expected_layer_precision_3decimals_path():
    shp_path = os.path.join(
        UTILS_DATA,
        'expected_layer_precision_3decimals.geojson'
    )
    return shp_path


@pytest.fixture
def expected_layer_precision_3decimals_gdf(expected_layer_precision_3decimals_path):
    gdf = gpd.read_file(expected_layer_precision_3decimals_path)
    return gdf.geometry


@pytest.fixture
def test_data_na_countries_path():
    shp_path = os.path.join(
        UTILS_SHP_DATA,
        'naturalearth_admin_countries_50m.shp'
    )
    return shp_path


@pytest.fixture
def test_data_na_countries_gdf(test_data_na_countries_path):
    gdf = gpd.read_file(test_data_na_countries_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def test_data_polygon_exterior_duplicates_path():
    shp_path = os.path.join(
        TOPOLOGY_DATA,
        'test_data_polygon_exterior_duplicates.geojson'
    )
    return shp_path


@pytest.fixture
def test_data_polygon_exterior_duplicates_gdf(test_data_polygon_exterior_duplicates_path):
    gdf = gpd.read_file(test_data_polygon_exterior_duplicates_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def test_data_polygon_interior_duplicates_path():
    shp_path = os.path.join(
        TOPOLOGY_DATA,
        'test_data_polygon_interior_duplicates.geojson'
    )
    return shp_path


@pytest.fixture
def test_data_polygon_interior_duplicates_gdf(test_data_polygon_interior_duplicates_path):
    gdf = gpd.read_file(test_data_polygon_interior_duplicates_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def test_data_linestring_duplicates_path():
    shp_path = os.path.join(
        TOPOLOGY_DATA,
        'test_data_linestring_duplicates.geojson'
    )
    return shp_path


@pytest.fixture
def test_data_linestring_duplicates_gdf(test_data_linestring_duplicates_path):
    gdf = gpd.read_file(test_data_linestring_duplicates_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def expected_linestring_duplicates_path():
    shp_path = os.path.join(
        TOPOLOGY_DATA,
        'expected_linestring_duplicates.geojson'
    )
    return shp_path


@pytest.fixture
def expected_linestring_duplicates_gdf(expected_linestring_duplicates_path):
    gdf = gpd.read_file(expected_linestring_duplicates_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def test_data_overlaps_path():
    shp_path = os.path.join(
        TOPOLOGY_SHP_DATA,
        'test_data_overlaps.shp'
    )
    return shp_path


@pytest.fixture
def test_data_overlaps_gdf(test_data_overlaps_path):
    gdf = gpd.read_file(test_data_overlaps_path)
    return [gdf, gdf.geometry]


@pytest.fixture
def expected_overlaps_path():
    shp_path = os.path.join(
        TOPOLOGY_SHP_DATA,
        'expected_overlaps.shp'
    )
    return shp_path


@pytest.fixture
def expected_overlaps_gdf(expected_overlaps_path):
    gdf = gpd.read_file(expected_overlaps_path)
    return [gdf, gdf.geometry]
