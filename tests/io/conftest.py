import os
import sys
import pytest
import pyspark


DATA_FOLDER = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data')


@pytest.fixture(scope="session")
def spark_session():
    os.environ["PYSPARK_PYTHON"] = sys.executable

    spark = (
        pyspark.sql.SparkSession
            .builder
            .master('local[*]')
            .appName('geonurse unit tests')
            .getOrCreate()
    )
    yield spark
    spark.stop()



@pytest.fixture
def test_data_path():
    test_data_path_list = [
        os.path.join(DATA_FOLDER, test_data) for test_data in os.listdir(DATA_FOLDER)
        if not test_data.endswith('.txt')
    ]
    return test_data_path_list
