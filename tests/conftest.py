import os
import sys
import fiona
import pytest
import pyspark


DATA_FOLDER = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data')


@pytest.fixture(scope="session")
def spark_session():
    os.environ['PYSPARK_PYTHON'] = sys.executable

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
    os.environ['PYSPARK_PYTHON'] = sys.executable

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
def test_data_geodataframe_path():
    return os.path.join(DATA_FOLDER, 'test_data_geodataframe.geojson')


@pytest.fixture
def test_data_geordd(test_data_geodataframe_path):
    shp = fiona.open(test_data_geodataframe_path)
    yield shp
    shp.close()
